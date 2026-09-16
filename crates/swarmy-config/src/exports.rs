use std::collections::BTreeMap;

use crate::Error;

/// Parse the stack's export file as data. No shell or command substitutions run.
/// # Errors
/// Rejects anything other than a single `export SWARMY_*=value` per line.
pub fn parse_exports(input: &str) -> Result<BTreeMap<String, String>, Error> {
    let mut values = BTreeMap::new();
    for (index, line) in input.lines().enumerate() {
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let assignment = line
            .strip_prefix("export ")
            .ok_or(Error::Export(index + 1))?;
        let (key, raw) = assignment.split_once('=').ok_or(Error::Export(index + 1))?;
        if !key.starts_with("SWARMY_")
            || !key
                .bytes()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
        {
            return Err(Error::Export(index + 1));
        }
        let value = if let Some(quoted) = raw
            .strip_prefix("$'")
            .and_then(|value| value.strip_suffix('\''))
        {
            ansi_string(quoted).ok_or(Error::Export(index + 1))?
        } else {
            let mut words = shell_words::split(raw).map_err(|_| Error::Export(index + 1))?;
            if words.len() != 1 {
                return Err(Error::Export(index + 1));
            }
            words.remove(0)
        };
        values.insert(key.to_owned(), value);
    }
    Ok(values)
}

// Bash printf %q uses ANSI-C quoting for paths containing control characters.
fn ansi_string(input: &str) -> Option<String> {
    let mut bytes = input.bytes().peekable();
    let mut output = Vec::new();
    while let Some(byte) = bytes.next() {
        if byte == b'\'' {
            return None;
        }
        if byte != b'\\' {
            output.push(byte);
            continue;
        }
        output.push(match bytes.next()? {
            b'a' => 7,
            b'b' => 8,
            b'e' | b'E' => 27,
            b'f' => 12,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'v' => 11,
            b'\\' => b'\\',
            b'\'' => b'\'',
            first @ b'0'..=b'7' => {
                let mut value = u16::from(first - b'0');
                for _ in 0..2 {
                    if let Some(digit @ b'0'..=b'7') = bytes.peek().copied() {
                        bytes.next();
                        value = value * 8 + u16::from(digit - b'0');
                    } else {
                        break;
                    }
                }
                u8::try_from(value).ok()?
            }
            _ => return None,
        });
    }
    String::from_utf8(output).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_stack_setting() {
        let input = "export SWARMY_FDB_CLUSTER_FILE=/tmp/fdb.cluster\nexport SWARMY_NATS_URL=nats://127.0.0.1:4222\nexport SWARMY_S3_ENDPOINT=http://127.0.0.1:8333\nexport SWARMY_S3_ACCESS_KEY=swarmy-dev\nexport SWARMY_S3_SECRET_KEY=swarmy-dev-secret\nexport SWARMY_S3_BUCKET=swarmy\nexport SWARMY_S3_PREFIX=''\nexport SWARMY_S3_REGION=us-east-1\n";
        let values = parse_exports(input).unwrap();
        assert_eq!(values.len(), 8);
        assert_eq!(values["SWARMY_S3_PREFIX"], "");
        assert_eq!(values["SWARMY_S3_ENDPOINT"], "http://127.0.0.1:8333");
    }

    #[test]
    fn parses_bash_control_character_and_octal_quoting() {
        let values = parse_exports(
            r"export SWARMY_FDB_CLUSTER_FILE=$'/tmp/a\tb\nc\047d\303\251/fdb.cluster'",
        )
        .unwrap();
        assert_eq!(
            values["SWARMY_FDB_CLUSTER_FILE"],
            "/tmp/a\tb\nc'dé/fdb.cluster"
        );
        assert!(parse_exports(r"export SWARMY_MODEL=$'unterminated").is_err());
    }
}
