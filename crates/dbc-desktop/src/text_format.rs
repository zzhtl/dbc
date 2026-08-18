//! Text encodings used by the exporters.
//!
//! Small, exactly-specified formats implemented here rather than pulled in as
//! dependencies: the quoting and alphabet rules are short enough to state and
//! test in full, and keeping them local means one place defines how a value
//! reaches a file.

use std::{
    borrow::Cow,
    io::{self, Write},
};

/// Quote a field per RFC 4180, borrowing when no quoting is needed.
#[must_use]
pub fn csv_field(value: &str) -> Cow<'_, str> {
    if !value.contains([',', '"', '\n', '\r']) {
        return Cow::Borrowed(value);
    }
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for character in value.chars() {
        if character == '"' {
            quoted.push('"');
        }
        quoted.push(character);
    }
    quoted.push('"');
    Cow::Owned(quoted)
}

/// Write one RFC 4180 record.
///
/// # Errors
///
/// Propagates the writer's I/O error.
pub fn write_csv_record<W: Write>(
    writer: &mut W,
    fields: impl IntoIterator<Item = impl AsRef<str>>,
) -> io::Result<()> {
    let mut first = true;
    for field in fields {
        if !first {
            writer.write_all(b",")?;
        }
        first = false;
        writer.write_all(csv_field(field.as_ref()).as_bytes())?;
    }
    // LF rather than the RFC's CRLF: it is what this exporter has always
    // written, every reader accepts it, and changing it would silently alter
    // files people already diff.
    writer.write_all(b"\n")
}

/// Standard base64 with padding (RFC 4648 §4).
#[must_use]
pub fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let mut buffer = [0_u8; 3];
        buffer[..chunk.len()].copy_from_slice(chunk);
        let packed = (u32::from(buffer[0]) << 16) | (u32::from(buffer[1]) << 8) | u32::from(buffer[2]);
        let indices = [
            (packed >> 18) & 0x3f,
            (packed >> 12) & 0x3f,
            (packed >> 6) & 0x3f,
            packed & 0x3f,
        ];
        // A 1-byte tail encodes 2 characters, a 2-byte tail encodes 3.
        let kept = chunk.len() + 1;
        for index in indices.iter().take(kept) {
            out.push(char::from(ALPHABET[*index as usize]));
        }
        for _ in kept..4 {
            out.push('=');
        }
    }
    out
}

/// Escape a value for a GitHub-flavoured Markdown table cell.
#[must_use]
pub fn markdown_cell(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('\n', "<br>")
        .replace('\r', "")
}

#[cfg(test)]
mod tests {
    use super::{encode_base64, markdown_cell, write_csv_record};

    fn csv(fields: &[&str]) -> String {
        let mut out = Vec::new();
        write_csv_record(&mut out, fields).expect("writing to a Vec cannot fail");
        String::from_utf8(out).expect("CSV output is UTF-8")
    }

    #[test]
    fn plain_fields_are_written_unquoted() {
        assert_eq!(csv(&["id", "name"]), "id,name\n");
    }

    #[test]
    fn delimiters_quotes_and_newlines_are_quoted() {
        assert_eq!(csv(&["a,b"]), "\"a,b\"\n");
        assert_eq!(csv(&["say \"hi\""]), "\"say \"\"hi\"\"\"\n");
        assert_eq!(csv(&["line1\nline2"]), "\"line1\nline2\"\n");
    }

    #[test]
    fn empty_records_and_fields_round_trip() {
        assert_eq!(csv(&[]), "\n");
        assert_eq!(csv(&["", ""]), ",\n");
    }

    #[test]
    fn base64_matches_the_rfc4648_test_vectors() {
        for (input, expected) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(encode_base64(input.as_bytes()), expected, "input {input:?}");
        }
    }

    #[test]
    fn base64_covers_the_whole_byte_range() {
        assert_eq!(encode_base64(&[0x00, 0xff]), "AP8=");
        assert_eq!(encode_base64(&[0xfb, 0xff, 0xfe]), "+//+");
    }

    #[test]
    fn markdown_cells_cannot_break_out_of_the_table() {
        assert_eq!(markdown_cell("a|b"), "a\\|b");
        assert_eq!(markdown_cell("line1\nline2"), "line1<br>line2");
        assert_eq!(markdown_cell("back\\slash"), "back\\\\slash");
    }
}
