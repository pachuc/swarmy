//! Hourly metering buckets with hour-scoped range reads.
//!
//! Buckets are the query path; raw completion records are retained only for
//! debugging and pruned after `[metering] raw_retention_days`. Each counter
//! is a little-endian `u64` updated with a `FoundationDB` atomic `Add`, so the
//! completion record and its bucket updates commit in one transaction.
//!
//! Single dimensions pack (`dimension`, `hour`, `key`, `field`) and the
//! combined breakdown dimensions pack (`dimension`, `owner`, `hour`,
//! `entry`, `field`). Every read is one contiguous range over the queried
//! hours: a single key reads its hour slice, an aggregate reads the whole
//! hour slice, and one owner's entries read the owner's hour slice. Hour
//! always precedes the varying key parts so time bounds stay inside the
//! range instead of filtering a full-dimension scan in memory. Buckets
//! written before this layout (keyed `dimension`, `key`, `hour`, `field`)
//! are superseded: lifetime totals under the `usage` keys are unaffected,
//! but the cost series restarts.

use std::collections::BTreeMap;

use foundationdb::Transaction;
use foundationdb::options::MutationType;
use foundationdb::tuple::Subspace;
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

/// Entry name for completions recorded without an entry. The provider stays
/// known, so breakdowns name it and only the entry is missing.
#[must_use]
pub fn unknown_entry_key(provider: &str) -> String {
    entry_key(provider, "-")
}

/// Whether a combined dimension nests its owner as a tuple element.
fn is_combined(dimension: MeteringDimension) -> bool {
    matches!(
        dimension,
        MeteringDimension::AgentEntry | MeteringDimension::SessionEntry
    )
}

/// Split an `owner/entry` query key for the combined dimensions. Owner ids
/// are ULIDs without slashes, so the entry is everything after the first
/// slash, including its own `provider/label` separator.
fn split_combined_key(key: &str) -> Result<(String, String)> {
    key.split_once('/')
        .map(|(owner, entry)| (owner.to_owned(), entry.to_owned()))
        .ok_or(StoreError::Corrupt)
}

fn add(trx: &Transaction, key: &[u8], delta: u64) {
    if delta == 0 {
        return;
    }
    trx.atomic_op(key, &delta.to_le_bytes(), MutationType::Add);
}

/// One completion's counter updates, shared by the single and combined
/// bucket writers.
fn bucket_deltas(usage: &TokenUsage, cost_micros: u64) -> [(&'static str, u64); 8] {
    [
        ("input", usage.input_tokens),
        ("cached", usage.cached_input_tokens),
        ("cache_write", usage.cache_write_input_tokens),
        ("output", usage.output_tokens),
        ("reasoning", usage.reasoning_output_tokens),
        ("total", usage.total_tokens),
        ("cost", cost_micros),
        ("completions", 1),
    ]
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
    pub(crate) fn metering_bucket_key_single(
        &self,
        dimension: &str,
        hour: i64,
        key: &str,
        field: &str,
    ) -> Vec<u8> {
        self.root.pack(&("metering_hour", dimension, hour, key, field))
    }

    pub(crate) fn metering_bucket_key_combined(
        &self,
        dimension: &str,
        owner: &str,
        hour: i64,
        entry: &str,
        field: &str,
    ) -> Vec<u8> {
        self.root
            .pack(&("metering_hour", dimension, owner, hour, entry, field))
    }

    pub(crate) fn metering_add_single(
        &self,
        trx: &Transaction,
        dimension: &str,
        key: &str,
        hour: i64,
        usage: &TokenUsage,
        cost_micros: u64,
    ) {
        for (field, delta) in bucket_deltas(usage, cost_micros) {
            add(
                trx,
                &self.metering_bucket_key_single(dimension, hour, key, field),
                delta,
            );
        }
    }

    pub(crate) fn metering_add_combined(
        &self,
        trx: &Transaction,
        dimension: &str,
        owner: &str,
        entry: &str,
        hour: i64,
        usage: &TokenUsage,
        cost_micros: u64,
    ) {
        for (field, delta) in bucket_deltas(usage, cost_micros) {
            add(
                trx,
                &self.metering_bucket_key_combined(dimension, owner, hour, entry, field),
                delta,
            );
        }
    }

    /// One contiguous hour slice of a single dimension: every key in
    /// `[from_hour, to_hour]` inclusive.
    fn single_hour_range(
        &self,
        dimension: MeteringDimension,
        from_hour: i64,
        to_hour: i64,
    ) -> (Vec<u8>, Vec<u8>) {
        single_hour_range(&self.root, dimension.as_str(), from_hour, to_hour)
    }

    /// One owner's slice of a combined dimension, optionally narrowed to an
    /// inclusive hour window. The owner is a tuple element, so the range
    /// holds exactly that owner's rows and nothing else's.
    fn owner_hour_range(
        &self,
        dimension: MeteringDimension,
        owner: &str,
        hours: Option<(i64, i64)>,
    ) -> (Vec<u8>, Vec<u8>) {
        owner_hour_range(&self.root, dimension.as_str(), owner, hours)
    }
}

/// One contiguous hour slice of a single dimension: every key in
/// `[from_hour, to_hour]` inclusive. A free function so unit tests prove
/// the range holds exactly the queried rows without a database.
fn single_hour_range(
    root: &Subspace,
    dimension: &str,
    from_hour: i64,
    to_hour: i64,
) -> (Vec<u8>, Vec<u8>) {
    let begin = root.pack(&("metering_hour", dimension, from_hour));
    let end_hour = to_hour.checked_add(3_600).unwrap_or(to_hour);
    let end = root.pack(&("metering_hour", dimension, end_hour));
    (begin, end)
}

/// One owner's slice of a combined dimension, optionally narrowed to an
/// inclusive hour window. The owner is a tuple element, so the range holds
/// exactly that owner's rows and nothing else's.
fn owner_hour_range(
    root: &Subspace,
    dimension: &str,
    owner: &str,
    hours: Option<(i64, i64)>,
) -> (Vec<u8>, Vec<u8>) {
    let Some((from_hour, to_hour)) = hours else {
        return root.subspace(&("metering_hour", dimension, owner)).range();
    };
    let begin = root.pack(&("metering_hour", dimension, owner, from_hour));
    let end_hour = to_hour.checked_add(3_600).unwrap_or(to_hour);
    let end = root.pack(&("metering_hour", dimension, owner, end_hour));
    (begin, end)
}

impl Store {
    /// Page one contiguous bucket range into raw rows. Every caller passes a
    /// range built by `single_hour_range` or `owner_hour_range`, so reads
    /// stay proportional to the queried hours, never the fleet's history.
    async fn scan_bucket_range(
        &self,
        begin: Vec<u8>,
        end: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
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
            let full = batch.len() >= crate::MAX_SCAN_LIMIT;
            for (raw_key, _) in &batch {
                cursor.clone_from(raw_key);
                cursor.push(0);
            }
            rows.extend(batch);
            if !full {
                break;
            }
        }
        Ok(rows)
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
    slot.2.add(&totals.usage, totals.cost_micros);
    slot.3 = slot.3.saturating_add(completions);
}

/// Add one counter row to a per-group field fold, summing across keys that
/// share the group. Each coordinate holds one atomic counter, so every row
/// adds exactly once.
fn fold_row<K: Ord>(
    folded: &mut BTreeMap<K, BTreeMap<String, u64>>,
    key: K,
    field: String,
    value: u64,
) {
    let slot = folded.entry(key).or_default().entry(field).or_default();
    *slot = slot.saturating_add(value);
}

/// Build summed totals from one key-hour's field counters.
fn totals_of(fields: &BTreeMap<String, u64>) -> (UsageTotals, u64) {
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
    (
        totals,
        fields.get("completions").copied().unwrap_or(0),
    )
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
        let (owner, entry) = if is_combined(dimension) {
            split_combined_key(key)?
        } else {
            (String::new(), String::new())
        };
        let (begin, end) = if is_combined(dimension) {
            self.owner_hour_range(dimension, &owner, Some((from_hour, to_hour)))
        } else {
            self.single_hour_range(dimension, from_hour, to_hour)
        };
        let rows = self.scan_bucket_range(begin, end).await?;
        let mut hours: BTreeMap<i64, BTreeMap<String, u64>> = BTreeMap::new();
        if is_combined(dimension) {
            let prefix = self.root.subspace(&(
                "metering_hour",
                dimension.as_str(),
                owner.as_str(),
            ));
            for (raw_key, value) in &rows {
                let (hour, row_entry, field): (i64, String, String) =
                    prefix.unpack(raw_key).map_err(|_| StoreError::Corrupt)?;
                if row_entry == entry && FIELDS.contains(&field.as_str()) {
                    hours
                        .entry(hour)
                        .or_default()
                        .insert(field.clone(), counter(value));
                }
            }
        } else {
            let prefix = self.root.subspace(&("metering_hour", dimension.as_str()));
            for (raw_key, value) in &rows {
                let (hour, row_key, field): (i64, String, String) =
                    prefix.unpack(raw_key).map_err(|_| StoreError::Corrupt)?;
                if row_key == key && FIELDS.contains(&field.as_str()) {
                    hours
                        .entry(hour)
                        .or_default()
                        .insert(field.clone(), counter(value));
                }
            }
        }
        let mut result = BTreeMap::new();
        for (hour, fields) in hours {
            let (totals, completions) = totals_of(&fields);
            result.insert(hour, (totals, completions));
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
    /// fleet-wide series through this instead of enumerating keys. The read
    /// is one hour slice, so a status poll costs the window, not the
    /// fleet's whole history.
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
        let (begin, end) = self.single_hour_range(dimension, from_hour, to_hour);
        let rows = self.scan_bucket_range(begin, end).await?;
        let prefix = self.root.subspace(&("metering_hour", dimension.as_str()));
        // Each (hour, key, field) coordinate holds one atomic counter, so
        // every row in the slice folds exactly once.
        let mut folded: BTreeMap<i64, BTreeMap<String, u64>> = BTreeMap::new();
        for (raw_key, value) in &rows {
            let (hour, _key, field): (i64, String, String) =
                prefix.unpack(raw_key).map_err(|_| StoreError::Corrupt)?;
            if FIELDS.contains(&field.as_str()) {
                fold_row(&mut folded, hour, field, counter(value));
            }
        }
        let mut hours: BTreeMap<i64, (UsageTotals, u64)> = BTreeMap::new();
        for (hour, fields) in folded {
            let (totals, completions) = totals_of(&fields);
            hours.insert(hour, (totals, completions));
        }
        Ok(group_hours(&hours, group_by))
    }

    /// Sum one owner's entries in a combined dimension into per-entry
    /// totals. The show commands list one owner's entries this way; with an
    /// hour window the read is that owner's slice of those hours, otherwise
    /// the owner's whole slice. Either way no other owner's rows are read.
    /// # Errors
    /// Returns decoding or storage errors.
    pub async fn dimension_totals(
        &self,
        dimension: MeteringDimension,
        owner: &str,
        hours: Option<(Timestamp, Timestamp)>,
    ) -> Result<Vec<DimensionTotal>> {
        let window = hours
            .filter(|(from, to)| to > from)
            .map(|(from, to)| {
                (
                    hour_floor(from.as_second()),
                    hour_floor(to.as_second().saturating_sub(1)),
                )
            })
            .filter(|(from_hour, to_hour)| to_hour >= from_hour);
        if hours.is_some() && window.is_none() {
            return Ok(Vec::new());
        }
        let (begin, end) = self.owner_hour_range(dimension, owner, window);
        let rows = self.scan_bucket_range(begin, end).await?;
        let prefix = self
            .root
            .subspace(&("metering_hour", dimension.as_str(), owner));
        let mut folded: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        for (raw_key, value) in &rows {
            let (_hour, entry, field): (i64, String, String) =
                prefix.unpack(raw_key).map_err(|_| StoreError::Corrupt)?;
            if FIELDS.contains(&field.as_str()) {
                fold_row(&mut folded, entry, field, counter(value));
            }
        }
        Ok(folded
            .into_iter()
            .map(|(key, fields)| {
                let (totals, completions) = totals_of(&fields);
                DimensionTotal {
                    key,
                    totals,
                    completions,
                }
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
    use foundationdb::tuple::Subspace;

    /// The single-dimension hour slice holds exactly its hours across every
    /// key, and nothing else: rows for other hours or dimensions sort
    /// outside `[begin, end)`, so the aggregate never reads them however
    /// many the fleet has written.
    #[test]
    fn single_hour_slice_excludes_rows_outside_its_hours() {
        let root = Subspace::all();
        let row = |dimension: &str, hour: i64, key: &str| {
            root.pack(&("metering_hour", dimension, hour, key, "input"))
        };
        // Row hours are always multiples of 3_600, so the end bound one hour
        // past the last queried hour excludes exactly the hours after it.
        let (begin, end) = single_hour_range(&root, "agent", 3_600, 7_200);
        for hour in [3_600, 7_200] {
            for key in ["a", "z"] {
                let packed = row("agent", hour, key);
                assert!(begin <= packed && packed < end, "{hour}/{key}");
            }
        }
        for hour in [0, 3_600 - 3_600, 7_200 + 3_600, 100_000_000] {
            let packed = row("agent", hour, "a");
            assert!(packed < begin || packed >= end, "{hour}");
        }
        for dimension in ["session", "agent_entry"] {
            let packed = row(dimension, 2_000, "a");
            assert!(packed < begin || packed >= end, "{dimension}");
        }
        // Growing history outside the window never enters the slice.
        let (again, _) = single_hour_range(&root, "agent", 3_600, 7_200);
        assert_eq!(begin, again);
    }

    /// One owner's slice of a combined dimension holds exactly that owner's
    /// rows in the queried hours: other owners and other hours sort outside
    /// the range, so an entry breakdown costs one owner, not the fleet.
    #[test]
    fn owner_hour_slice_excludes_other_owners_and_hours() {
        let root = Subspace::all();
        let row = |owner: &str, hour: i64, entry: &str| {
            root.pack(&("metering_hour", "agent_entry", owner, hour, entry, "cost"))
        };
        let (begin, end) =
            owner_hour_range(&root, "agent_entry", "owner-a", Some((3_600, 7_200)));
        for hour in [3_600, 7_200] {
            for entry in ["openai/main", "openai/-"] {
                let packed = row("owner-a", hour, entry);
                assert!(begin <= packed && packed < end, "{hour}/{entry}");
            }
        }
        for owner in ["owner-b", "owner-a2", "owner"] {
            let packed = row(owner, 3_600, "openai/main");
            assert!(packed < begin || packed >= end, "{owner}");
        }
        for hour in [0, 7_200 + 3_600, 100_000_000] {
            let packed = row("owner-a", hour, "openai/main");
            assert!(packed < begin || packed >= end, "{hour}");
        }
        // Without a window the slice is still exactly one owner's rows.
        let (all_begin, all_end) = owner_hour_range(&root, "agent_entry", "owner-a", None);
        for hour in [0, 1_000, 10_000_000] {
            let packed = row("owner-a", hour, "openai/main");
            assert!(all_begin <= packed && packed < all_end, "{hour}");
        }
        let packed = row("owner-b", 1_000, "openai/main");
        assert!(packed < all_begin || packed >= all_end);
    }

    #[test]
    fn combined_keys_split_owner_from_entry() {
        assert_eq!(
            split_combined_key("owner/openai/main"),
            Ok(("owner".into(), "openai/main".into()))
        );
        assert_eq!(
            split_combined_key("owner/openai/-"),
            Ok(("owner".into(), "openai/-".into()))
        );
        assert!(split_combined_key("owner").is_err());
        assert_eq!(unknown_entry_key("openai"), "openai/-");
    }

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
