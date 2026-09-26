//! Hourly metering buckets keyed by (`dimension`, `key`, `hour`).
//!
//! Buckets are the query path; raw completion records are retained only for
//! debugging and pruned after `[metering] raw_retention_days`. Each counter
//! is a little-endian `u64` updated with a `FoundationDB` atomic `Add`, so the
//! completion record and its bucket updates commit in one transaction.

use std::collections::BTreeMap;

use foundationdb::Transaction;
use foundationdb::options::MutationType;
use jiff::{Timestamp, ToSpan, civil::Weekday};
use serde::{Deserialize, Serialize};
use swarmy_core::{TokenUsage, UsageTotals};

use crate::{Result, Store, StoreError, read, scan};

/// Queryable rollup dimensions. `Entry` is `provider/label`. The
/// `AgentEntry` and `SessionEntry` combinations are written for the
/// `agent show` and `session show` breakdowns; they are internal because the
/// public `by=` filter stays limited to the six single dimensions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MeteringDimension {
    Session,
    Agent,
    Provider,
    Entry,
    EntryKind,
    Model,
    AgentEntry,
    SessionEntry,
}

impl MeteringDimension {
    /// Canonical key prefix used in bucket keys.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Agent => "agent",
            Self::Provider => "provider",
            Self::Entry => "entry",
            Self::EntryKind => "entry_kind",
            Self::Model => "model",
            Self::AgentEntry => "agent_entry",
            Self::SessionEntry => "session_entry",
        }
    }

    /// Parse a dimension name from CLI or API input. The combined dimensions
    /// stay internal: `agent show` and `session show` read them, but `by=`
    /// accepts only the six single dimensions (with `kind` as the CLI-facing
    /// alias for `entry_kind`).
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "session" => Some(Self::Session),
            "agent" => Some(Self::Agent),
            "provider" => Some(Self::Provider),
            "entry" => Some(Self::Entry),
            "entry_kind" | "kind" => Some(Self::EntryKind),
            "model" => Some(Self::Model),
            _ => None,
        }
    }
}

/// Grouping for range queries. Weeks start on Monday at UTC midnight, so a
/// week that crosses a month still forms one group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageGroupBy {
    Day,
    Week,
    Month,
    Year,
}

/// One grouped sum over hourly buckets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageGroup {
    pub start: Timestamp,
    pub end: Timestamp,
    pub totals: UsageTotals,
    pub completions: u64,
}

/// One key's summed buckets across all hours, used for per-entry
/// breakdowns in the show commands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DimensionTotal {
    pub key: String,
    pub totals: UsageTotals,
    pub completions: u64,
}

/// Bucket fields stored as separate atomic counters.
const FIELDS: [&str; 8] = [
    "input",
    "cached",
    "cache_write",
    "output",
    "reasoning",
    "total",
    "cost",
    "completions",
];

/// Truncate a unix second to its hour.
#[must_use]
pub fn hour_floor(second: i64) -> i64 {
    second - second.rem_euclid(3_600)
}

/// Join provider and label without tuple escaping surprises.
#[must_use]
pub fn entry_key(provider: &str, label: &str) -> String {
    format!("{provider}/{label}")
}

/// Join an owner id with its entry for the combined breakdown dimensions.
/// Owner ids are ULIDs without slashes, so the entry is everything after
/// the first slash.
#[must_use]
pub fn agent_entry_key(agent: &str, entry: &str) -> String {
    format!("{agent}/{entry}")
}

/// Join a session id with its entry for the combined breakdown dimensions.
#[must_use]
pub fn session_entry_key(session: &str, entry: &str) -> String {
    format!("{session}/{entry}")
}

/// Split a combined key back into its owner prefix and entry suffix.
#[must_use]
pub fn split_owner_key(key: &str) -> Option<(&str, &str)> {
    key.split_once('/')
}

fn add(trx: &Transaction, key: &[u8], delta: u64) {
    if delta == 0 {
        return;
    }
    trx.atomic_op(key, &delta.to_le_bytes(), MutationType::Add);
}

fn counter(bytes: &[u8]) -> u64 {
    if bytes.is_empty() {
        return 0;
    }
    let mut padded = [0_u8; 8];
    let width = bytes.len().min(8);
    padded[..width].copy_from_slice(&bytes[..width]);
    u64::from_le_bytes(padded)
}

impl Store {
    pub(crate) fn metering_bucket_key(
        &self,
        dimension: &str,
        key: &str,
        hour: i64,
        field: &str,
    ) -> Vec<u8> {
        self.root
            .pack(&("metering_hour", dimension, key, hour, field))
    }

    pub(crate) fn metering_add(
        &self,
        trx: &Transaction,
        dimension: &str,
        key: &str,
        hour: i64,
        usage: &TokenUsage,
        cost_micros: u64,
    ) {
        let deltas = [
            ("input", usage.input_tokens),
            ("cached", usage.cached_input_tokens),
            ("cache_write", usage.cache_write_input_tokens),
            ("output", usage.output_tokens),
            ("reasoning", usage.reasoning_output_tokens),
            ("total", usage.total_tokens),
            ("cost", cost_micros),
            ("completions", 1),
        ];
        for (field, delta) in deltas {
            add(
                trx,
                &self.metering_bucket_key(dimension, key, hour, field),
                delta,
            );
        }
    }
}

/// Midnight UTC starting the timestamp's calendar day.
fn day_start(moment: Timestamp) -> Timestamp {
    let date = moment.to_zoned(jiff::tz::TimeZone::UTC).date();
    match date.to_zoned(jiff::tz::TimeZone::UTC) {
        Ok(zoned) => zoned.timestamp(),
        Err(_) => moment,
    }
}

/// Monday midnight UTC starting the timestamp's ISO week.
fn week_start(moment: Timestamp) -> Timestamp {
    let zoned = moment.to_zoned(jiff::tz::TimeZone::UTC);
    let date = zoned.date();
    let days = match date.weekday() {
        Weekday::Monday => 0,
        Weekday::Tuesday => 1,
        Weekday::Wednesday => 2,
        Weekday::Thursday => 3,
        Weekday::Friday => 4,
        Weekday::Saturday => 5,
        Weekday::Sunday => 6,
    };
    let monday = date.saturating_sub(days.days());
    match monday.to_zoned(jiff::tz::TimeZone::UTC) {
        Ok(zoned) => zoned.timestamp(),
        Err(_) => moment,
    }
}

/// Midnight UTC starting the timestamp's calendar month.
fn month_start(moment: Timestamp) -> Timestamp {
    let zoned = moment.to_zoned(jiff::tz::TimeZone::UTC);
    let date = zoned.date().first_of_month();
    match date.to_zoned(jiff::tz::TimeZone::UTC) {
        Ok(zoned) => zoned.timestamp(),
        Err(_) => moment,
    }
}

/// Midnight UTC starting the timestamp's calendar year.
fn year_start(moment: Timestamp) -> Timestamp {
    let zoned = moment.to_zoned(jiff::tz::TimeZone::UTC);
    let date = zoned.date().first_of_year();
    match date.to_zoned(jiff::tz::TimeZone::UTC) {
        Ok(zoned) => zoned.timestamp(),
        Err(_) => moment,
    }
}

/// Start of the group containing `moment`.
#[must_use]
pub fn group_start(moment: Timestamp, group_by: UsageGroupBy) -> Timestamp {
    match group_by {
        UsageGroupBy::Day => day_start(moment),
        UsageGroupBy::Week => week_start(moment),
        UsageGroupBy::Month => month_start(moment),
        UsageGroupBy::Year => year_start(moment),
    }
}

/// Exclusive end of the group starting at `start`.
#[must_use]
pub fn group_end(start: Timestamp, group_by: UsageGroupBy) -> Timestamp {
    match group_by {
        UsageGroupBy::Day => start.checked_add(24.hours()).unwrap_or(start),
        UsageGroupBy::Week => start.checked_add(168.hours()).unwrap_or(start),
        UsageGroupBy::Month => next_month_start(start).unwrap_or(start),
        UsageGroupBy::Year => next_year_start(start).unwrap_or(start),
    }
}

/// Midnight UTC starting the next calendar month after `start`'s month.
fn next_month_start(start: Timestamp) -> Option<Timestamp> {
    let zoned = start.to_zoned(jiff::tz::TimeZone::UTC);
    let first = zoned.date().first_of_month();
    let next = first.checked_add(1.months()).ok()?;
    next.to_zoned(jiff::tz::TimeZone::UTC)
        .ok()
        .map(|zoned| zoned.timestamp())
}

/// Midnight UTC starting the next calendar year after `start`'s year.
fn next_year_start(start: Timestamp) -> Option<Timestamp> {
    let zoned = start.to_zoned(jiff::tz::TimeZone::UTC);
    let first = zoned.date().first_of_year();
    let next = first.checked_add(1.years()).ok()?;
    next.to_zoned(jiff::tz::TimeZone::UTC)
        .ok()
        .map(|zoned| zoned.timestamp())
}

fn accumulate(
    groups: &mut BTreeMap<i64, (Timestamp, Timestamp, UsageTotals, u64)>,
    hour: i64,
    group_by: UsageGroupBy,
    totals: &UsageTotals,
    completions: u64,
) {
    let moment = Timestamp::from_second(hour).unwrap_or(Timestamp::UNIX_EPOCH);
    let start = group_start(moment, group_by);
    let end = group_end(start, group_by);
    let slot = groups
        .entry(start.as_second())
        .or_insert((start, end, UsageTotals::default(), 0));
    add_totals(&mut slot.2, totals);
    slot.3 = slot.3.saturating_add(completions);
}

/// Add one bucket's totals into a running sum without losing whole
/// diagnostics to overflow.
fn add_totals(into: &mut UsageTotals, totals: &UsageTotals) {
    into.usage.input_tokens = into
        .usage
        .input_tokens
        .saturating_add(totals.usage.input_tokens);
    into.usage.cached_input_tokens = into
        .usage
        .cached_input_tokens
        .saturating_add(totals.usage.cached_input_tokens);
    into.usage.cache_write_input_tokens = into
        .usage
        .cache_write_input_tokens
        .saturating_add(totals.usage.cache_write_input_tokens);
    into.usage.output_tokens = into
        .usage
        .output_tokens
        .saturating_add(totals.usage.output_tokens);
    into.usage.reasoning_output_tokens = into
        .usage
        .reasoning_output_tokens
        .saturating_add(totals.usage.reasoning_output_tokens);
    into.usage.total_tokens = into
        .usage
        .total_tokens
        .saturating_add(totals.usage.total_tokens);
    into.cost_micros = into.cost_micros.saturating_add(totals.cost_micros);
}

/// Merge one hour's buckets into an hourly map shared by the single-key
/// and aggregate query paths.
fn merge_hour(
    hours: &mut BTreeMap<i64, (UsageTotals, u64)>,
    hour: i64,
    totals: &UsageTotals,
    completions: u64,
) {
    let slot = hours
        .entry(hour)
        .or_insert((UsageTotals::default(), 0));
    add_totals(&mut slot.0, totals);
    slot.1 = slot.1.saturating_add(completions);
}

/// Fold an hourly map into calendar groups in time order.
fn group_hours(
    hours: &BTreeMap<i64, (UsageTotals, u64)>,
    group_by: UsageGroupBy,
) -> Vec<UsageGroup> {
    let mut grouped: BTreeMap<i64, (Timestamp, Timestamp, UsageTotals, u64)> = BTreeMap::new();
    for (hour, (totals, completions)) in hours {
        accumulate(&mut grouped, *hour, group_by, totals, *completions);
    }
    grouped
        .into_values()
        .map(|(start, end, totals, completions)| UsageGroup {
            start,
            end,
            totals,
            completions,
        })
        .collect()
}

impl Store {
    async fn bucket_hours(
        &self,
        dimension: MeteringDimension,
        key: &str,
        from_hour: i64,
        to_hour: i64,
    ) -> Result<BTreeMap<i64, (UsageTotals, u64)>> {
        if to_hour < from_hour {
            return Ok(BTreeMap::new());
        }
        let end_hour = to_hour.checked_add(3_600).unwrap_or(to_hour);
        let begin = self
            .root
            .pack(&("metering_hour", dimension.as_str(), key, from_hour));
        let end = self
            .root
            .pack(&("metering_hour", dimension.as_str(), key, end_hour));
        let mut hours: BTreeMap<i64, BTreeMap<String, u64>> = BTreeMap::new();
        let mut cursor = begin.clone();
        loop {
            let rows = self
                .transaction(|trx| {
                    let range = (cursor.clone(), end.clone());
                    async move { scan(&trx, range, crate::MAX_SCAN_LIMIT).await }
                })
                .await?;
            if rows.is_empty() {
                break;
            }
            // Range bounds already restrict hours; unpack failures still error.
            let prefix = self
                .root
                .subspace(&("metering_hour", dimension.as_str(), key));
            for (raw_key, value) in &rows {
                let (hour, field): (i64, String) =
                    prefix.unpack(raw_key).map_err(|_| StoreError::Corrupt)?;
                if FIELDS.contains(&field.as_str()) {
                    hours
                        .entry(hour)
                        .or_default()
                        .insert(field.clone(), counter(value));
                }
                cursor.clone_from(raw_key);
                cursor.push(0);
            }
            if rows.len() < crate::MAX_SCAN_LIMIT {
                break;
            }
        }
        let mut result = BTreeMap::new();
        for (hour, fields) in hours {
            let totals = UsageTotals {
                usage: TokenUsage {
                    input_tokens: fields.get("input").copied().unwrap_or(0),
                    cached_input_tokens: fields.get("cached").copied().unwrap_or(0),
                    cache_write_input_tokens: fields.get("cache_write").copied().unwrap_or(0),
                    output_tokens: fields.get("output").copied().unwrap_or(0),
                    reasoning_output_tokens: fields.get("reasoning").copied().unwrap_or(0),
                    total_tokens: fields.get("total").copied().unwrap_or(0),
                },
                cost_micros: fields.get("cost").copied().unwrap_or(0),
            };
            result.insert(
                hour,
                (totals, fields.get("completions").copied().unwrap_or(0)),
            );
        }
        Ok(result)
    }

    /// Sum hourly buckets over `[from, to)` into calendar groups.
    /// # Errors
    /// Returns decoding or storage errors.
    pub async fn usage(
        &self,
        dimension: MeteringDimension,
        key: &str,
        from: Timestamp,
        to: Timestamp,
        group_by: UsageGroupBy,
    ) -> Result<Vec<UsageGroup>> {
        if to <= from {
            return Ok(Vec::new());
        }
        let from_hour = hour_floor(from.as_second());
        let to_hour = hour_floor(to.as_second().saturating_sub(1));
        let hours = self
            .bucket_hours(dimension, key, from_hour, to_hour)
            .await?;
        Ok(group_hours(&hours, group_by))
    }

    /// Sum hourly buckets over `[from, to)` across every key in `dimension`
    /// into calendar groups. `swarmy cost` without a key filter reads the
    /// fleet-wide series through this instead of enumerating keys.
    /// # Errors
    /// Returns decoding or storage errors.
    pub async fn usage_aggregate(
        &self,
        dimension: MeteringDimension,
        from: Timestamp,
        to: Timestamp,
        group_by: UsageGroupBy,
    ) -> Result<Vec<UsageGroup>> {
        if to <= from {
            return Ok(Vec::new());
        }
        let from_hour = hour_floor(from.as_second());
        let to_hour = hour_floor(to.as_second().saturating_sub(1));
        if to_hour < from_hour {
            return Ok(Vec::new());
        }
        let mut hours: BTreeMap<i64, (UsageTotals, u64)> = BTreeMap::new();
        for (_, hour, totals, completions) in self
            .scan_dimension(dimension, Some((from_hour, to_hour)))
            .await?
        {
            merge_hour(&mut hours, hour, &totals, completions);
        }
        Ok(group_hours(&hours, group_by))
    }

    /// Sum every key starting with `prefix` in `dimension` into per-key
    /// totals, ignoring time. The show commands list one owner's entries
    /// through the combined dimensions with an `{owner}/` prefix.
    /// # Errors
    /// Returns decoding or storage errors.
    pub async fn dimension_totals(
        &self,
        dimension: MeteringDimension,
        prefix: &str,
    ) -> Result<Vec<DimensionTotal>> {
        let mut totals: BTreeMap<String, (UsageTotals, u64)> = BTreeMap::new();
        for (key, _, bucket, completions) in self
            .scan_dimension_keys(dimension, Some(prefix))
            .await?
        {
            let slot = totals.entry(key).or_insert((UsageTotals::default(), 0));
            add_totals(&mut slot.0, &bucket);
            slot.1 = slot.1.saturating_add(completions);
        }
        Ok(totals
            .into_iter()
            .map(|(key, (totals, completions))| DimensionTotal {
                key,
                totals,
                completions,
            })
            .collect())
    }

    /// Scan every bucket row in `dimension`, optionally restricted to an
    /// inclusive hour range. Rows arrive as `(key, hour, totals,
    /// completions)` in key and hour order.
    async fn scan_dimension(
        &self,
        dimension: MeteringDimension,
        hours: Option<(i64, i64)>,
    ) -> Result<Vec<(String, i64, UsageTotals, u64)>> {
        let mut kept = Vec::new();
        for (key, hour, totals, completions) in
            self.scan_dimension_keys(dimension, None).await?
        {
            if hours.is_some_and(|(from, to)| hour < from || hour > to) {
                continue;
            }
            kept.push((key, hour, totals, completions));
        }
        Ok(kept)
    }

    /// Scan every bucket row in `dimension`, optionally keeping only keys
    /// with the given prefix. Each row decodes to its key, hour, totals,
    /// and completion count.
    async fn scan_dimension_keys(
        &self,
        dimension: MeteringDimension,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, i64, UsageTotals, u64)>> {
        let subspace = self
            .root
            .subspace(&("metering_hour", dimension.as_str()));
        let (begin, end) = subspace.range();
        let mut cursor = begin.clone();
        let mut rows = Vec::new();
        loop {
            let batch = self
                .transaction(|trx| {
                    let range = (cursor.clone(), end.clone());
                    async move { scan(&trx, range, crate::MAX_SCAN_LIMIT).await }
                })
                .await?;
            if batch.is_empty() {
                break;
            }
            for (raw_key, value) in &batch {
                let (key, hour, field): (String, i64, String) =
                    subspace.unpack(raw_key).map_err(|_| StoreError::Corrupt)?;
                if FIELDS.contains(&field.as_str())
                    && prefix.is_none_or(|want| key.starts_with(want))
                {
                    rows.push((key, hour, field, counter(value)));
                }
                cursor.clone_from(raw_key);
                cursor.push(0);
            }
            if batch.len() < crate::MAX_SCAN_LIMIT {
                break;
            }
        }
        // Fold the eight field counters of each key and hour into totals.
        let mut folded: BTreeMap<(String, i64), BTreeMap<String, u64>> = BTreeMap::new();
        for (key, hour, field, value) in rows {
            folded
                .entry((key, hour))
                .or_default()
                .insert(field, value);
        }
        Ok(folded
            .into_iter()
            .map(|((key, hour), fields)| {
                let totals = UsageTotals {
                    usage: TokenUsage {
                        input_tokens: fields.get("input").copied().unwrap_or(0),
                        cached_input_tokens: fields.get("cached").copied().unwrap_or(0),
                        cache_write_input_tokens: fields
                            .get("cache_write")
                            .copied()
                            .unwrap_or(0),
                        output_tokens: fields.get("output").copied().unwrap_or(0),
                        reasoning_output_tokens: fields
                            .get("reasoning")
                            .copied()
                            .unwrap_or(0),
                        total_tokens: fields.get("total").copied().unwrap_or(0),
                    },
                    cost_micros: fields.get("cost").copied().unwrap_or(0),
                };
                (
                    key,
                    hour,
                    totals,
                    fields.get("completions").copied().unwrap_or(0),
                )
            })
            .collect())
    }

    /// Delete raw completion records at or before `before`, keeping rollups.
    /// New records write a `(recorded_at hour, request id)` index entry in
    /// the same completion transaction, so pruning scans that index from the
    /// oldest hour up to the cutoff in bounded batches. Deleting the index
    /// entry with its record advances the scan, so repeated ticks drain more
    /// than one batch. Records without `recorded_at` predate the index and
    /// are prunable once the retention window has passed since the upgrade
    /// marker recorded on first prune.
    /// # Errors
    /// Returns decoding or storage errors.
    pub async fn prune_metering_raw(&self, before: Timestamp, limit: usize) -> Result<usize> {
        crate::check_limit(limit)?;
        if limit == 0 {
            return Ok(0);
        }
        let upgrade_key = self.root.pack(&("metering_upgrade_at",));
        let upgrade_at: Option<Timestamp> = self
            .transaction(|trx| {
                let upgrade_key = &upgrade_key;
                async move { read(&trx, upgrade_key).await }
            })
            .await?;
        let upgrade_at = if let Some(at) = upgrade_at {
            at
        } else {
            let now = Timestamp::now();
            let bytes = crate::encode(&now)?;
            self.transaction(|trx| {
                let upgrade_key = &upgrade_key;
                let bytes = &bytes;
                async move {
                    trx.set(upgrade_key, bytes);
                    Ok(())
                }
            })
            .await?;
            now
        };
        let cutoff_hour = crate::metering::hour_floor(before.as_second());
        let mut pruned = 0;
        pruned += self
            .prune_index_hours_before(cutoff_hour, limit - pruned)
            .await?;
        if pruned < limit {
            pruned += self
                .prune_index_hour_exact(cutoff_hour, before, limit - pruned)
                .await?;
        }
        if pruned < limit {
            pruned += self
                .prune_legacy_raw(before, upgrade_at, limit - pruned)
                .await?;
        }
        Ok(pruned)
    }

    async fn prune_index_hours_before(&self, cutoff_hour: i64, limit: usize) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let prefix = self.root.subspace(&("usage_record_by_time",));
        let (range_start, _) = prefix.range();
        let range_end = self.root.pack(&("usage_record_by_time", cutoff_hour));
        let rows = self
            .transaction(|trx| {
                let range = (range_start.clone(), range_end.clone());
                async move { scan(&trx, range, limit).await }
            })
            .await?;
        if rows.is_empty() {
            return Ok(0);
        }
        let mut stale = Vec::with_capacity(rows.len() * 2);
        for (index_key, _) in &rows {
            let (_, request): (i64, Vec<u8>) =
                prefix.unpack(index_key).map_err(|_| StoreError::Corrupt)?;
            let record_key = self.root.pack(&("usage_record", request.as_slice()));
            stale.push(index_key.clone());
            stale.push(record_key);
        }
        let pruned = rows.len();
        self.transaction(|trx| {
            let stale = &stale;
            async move {
                for key in stale {
                    trx.clear(key);
                }
                Ok(())
            }
        })
        .await?;
        Ok(pruned)
    }

    async fn prune_index_hour_exact(
        &self,
        cutoff_hour: i64,
        before: Timestamp,
        limit: usize,
    ) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let prefix = self.root.subspace(&("usage_record_by_time",));
        let range_start = self.root.pack(&("usage_record_by_time", cutoff_hour));
        let next_hour = cutoff_hour.checked_add(3_600).unwrap_or(cutoff_hour);
        let range_end = self.root.pack(&("usage_record_by_time", next_hour));
        let rows = self
            .transaction(|trx| {
                let range = (range_start.clone(), range_end.clone());
                async move { scan(&trx, range, limit).await }
            })
            .await?;
        if rows.is_empty() {
            return Ok(0);
        }
        let mut pairs = Vec::with_capacity(rows.len());
        for (index_key, _) in &rows {
            let (_, request): (i64, Vec<u8>) =
                prefix.unpack(index_key).map_err(|_| StoreError::Corrupt)?;
            let record_key = self.root.pack(&("usage_record", request.as_slice()));
            pairs.push((index_key.clone(), record_key));
        }
        let keys: Vec<Vec<u8>> = pairs.iter().map(|(_, key)| key.clone()).collect();
        let records: Vec<Option<crate::usage::UsageRecord>> = self
            .transaction(|trx| {
                let keys = &keys;
                async move {
                    let mut out = Vec::with_capacity(keys.len());
                    for key in keys {
                        out.push(read(&trx, key).await?);
                    }
                    Ok(out)
                }
            })
            .await?;
        let mut stale = Vec::new();
        for ((index_key, record_key), record) in pairs.into_iter().zip(records) {
            let old = record
                .as_ref()
                .is_some_and(|record| record.recorded_at.is_some_and(|at| at <= before));
            if old {
                stale.push(index_key);
                stale.push(record_key);
            }
        }
        let pruned = stale.len() / 2;
        if stale.is_empty() {
            return Ok(0);
        }
        self.transaction(|trx| {
            let stale = &stale;
            async move {
                for key in stale {
                    trx.clear(key);
                }
                Ok(())
            }
        })
        .await?;
        Ok(pruned)
    }

    async fn prune_legacy_raw(
        &self,
        before: Timestamp,
        upgrade_at: Timestamp,
        limit: usize,
    ) -> Result<usize> {
        if limit == 0 || upgrade_at > before {
            return Ok(0);
        }
        let cursor_key = self.root.pack(&("metering_prune_cursor",));
        let done_key = self.root.pack(&("metering_legacy_pruned",));
        let (cursor, done): (Option<Vec<u8>>, Option<bool>) = self
            .transaction(|trx| {
                let cursor_key = &cursor_key;
                let done_key = &done_key;
                async move {
                    let cursor = read(&trx, cursor_key).await?;
                    let done = read(&trx, done_key).await?;
                    Ok((cursor, done))
                }
            })
            .await?;
        if done.unwrap_or(false) {
            return Ok(0);
        }
        let prefix = self.root.subspace(&("usage_record",));
        let (range_start, range_end) = prefix.range();
        let mut begin = range_start.clone();
        if let Some(last) = cursor {
            begin = last;
            begin.push(0);
        }
        let rows = self
            .transaction(|trx| {
                let range = (begin.clone(), range_end.clone());
                async move { scan(&trx, range, limit).await }
            })
            .await?;
        if rows.is_empty() {
            // The scan reached the end of the subspace: every pre-index
            // record has been examined, and new writes carry the index, so
            // mark legacy cleanup complete instead of restarting from the
            // beginning and decoding every raw record on each tick.
            self.transaction(|trx| {
                let cursor_key = &cursor_key;
                let done_key = &done_key;
                async move {
                    trx.clear(cursor_key);
                    crate::write(&trx, done_key, &true)?;
                    Ok(())
                }
            })
            .await?;
            return Ok(0);
        }
        let mut stale = Vec::new();
        for (key, value) in &rows {
            let record: crate::usage::UsageRecord = crate::decode(value)?;
            if record.recorded_at.is_none() {
                stale.push(key.clone());
            }
        }
        let last_key = rows.last().map(|(key, _)| key.clone());
        let pruned = stale.len();
        self.transaction(|trx| {
            let stale = &stale;
            let last_key = &last_key;
            let cursor_key = &cursor_key;
            async move {
                for key in stale {
                    trx.clear(key);
                }
                if let Some(last) = last_key {
                    crate::write(&trx, cursor_key, last)?;
                }
                Ok(())
            }
        })
        .await?;
        Ok(pruned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(year: i16, month: i8, day: i8, hour: i8) -> Timestamp {
        jiff::civil::date(year, month, day)
            .at(hour, 0, 0, 0)
            .to_zoned(jiff::tz::TimeZone::UTC)
            .unwrap()
            .timestamp()
    }

    #[test]
    fn day_week_month_and_year_groups_have_expected_boundaries() {
        let moment = stamp(2026, 3, 15, 12);
        assert_eq!(
            group_start(moment, UsageGroupBy::Day),
            stamp(2026, 3, 15, 0)
        );
        assert_eq!(
            group_end(stamp(2026, 3, 15, 0), UsageGroupBy::Day),
            stamp(2026, 3, 16, 0)
        );
        assert_eq!(
            group_start(moment, UsageGroupBy::Month),
            stamp(2026, 3, 1, 0)
        );
        assert_eq!(
            group_end(stamp(2026, 3, 1, 0), UsageGroupBy::Month),
            stamp(2026, 4, 1, 0)
        );
        assert_eq!(
            group_start(moment, UsageGroupBy::Year),
            stamp(2026, 1, 1, 0)
        );
        assert_eq!(
            group_end(stamp(2026, 1, 1, 0), UsageGroupBy::Year),
            stamp(2027, 1, 1, 0)
        );
    }

    #[test]
    fn week_groups_start_monday_and_cross_months_as_one_group() {
        // 2026-03-01 is a Sunday; its week starts Monday 2026-02-23.
        let sunday = stamp(2026, 3, 1, 10);
        assert_eq!(
            group_start(sunday, UsageGroupBy::Week),
            stamp(2026, 2, 23, 0)
        );
        assert_eq!(
            group_end(stamp(2026, 2, 23, 0), UsageGroupBy::Week),
            stamp(2026, 3, 2, 0)
        );
        let monday = stamp(2026, 3, 2, 0);
        assert_eq!(group_start(monday, UsageGroupBy::Week), monday);
    }

    #[test]
    fn three_month_range_groups_cover_every_calendar_month() {
        let start = stamp(2026, 1, 15, 0);
        let end = stamp(2026, 4, 15, 0);
        let mut cursor = group_start(start, UsageGroupBy::Month);
        let mut months = Vec::new();
        while cursor < end {
            let next = group_end(cursor, UsageGroupBy::Month);
            months.push(cursor);
            cursor = next;
        }
        assert_eq!(
            months,
            vec![
                stamp(2026, 1, 1, 0),
                stamp(2026, 2, 1, 0),
                stamp(2026, 3, 1, 0),
                stamp(2026, 4, 1, 0),
            ]
        );
    }
}
