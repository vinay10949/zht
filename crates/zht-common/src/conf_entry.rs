//! Configuration entry (name-value pair).
//!
//! The Rust equivalent of the C++ `ConfEntry` class. Each configuration
//! file consists of multiple entries, where each line has a name and value
//! separated by whitespace.

use std::fmt;
use std::str::FromStr;

/// A single configuration entry with a name and optional value.
///
/// Lines in `zht.conf` and `neighbor.conf` are parsed into `ConfEntry` instances.
/// Comment lines (starting with `#`) and empty lines are skipped during parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfEntry {
    /// The parameter name (e.g., `"PROTOCOL"`, `"PORT"`)
    pub name: String,
    /// The parameter value (e.g., `"TCP"`, `"50000"`)
    /// May be empty if the config line has only a name.
    pub value: String,
}

impl ConfEntry {
    /// Create a new configuration entry.
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    /// Parse a configuration entry from a single line of text.
    ///
    /// The first whitespace-delimited token is the name, the second is the value.
    /// Leading/trailing whitespace is trimmed. Returns `None` for empty lines
    /// or comment lines (starting with `#`).
    pub fn from_line(line: &str) -> Option<Self> {
        let trimmed = line.trim();

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return None;
        }

        // Split on whitespace — first token is name, second (optional) is value
        let mut parts = trimmed.splitn(2, |c: char| c.is_whitespace());
        let name = parts.next()?.trim().to_string();
        let value = parts.next().unwrap_or("").trim().to_string();

        if name.is_empty() {
            return None;
        }

        Some(Self { name, value })
    }

    /// Get the name as a string reference.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the value as a string reference.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Try to parse the value as a specific type.
    pub fn parse_value<T: FromStr>(&self) -> Option<T> {
        self.value.parse().ok()
    }

    /// Get the value, defaulting to the provided value if empty or unparseable.
    pub fn value_or<T: FromStr>(&self, default: T) -> T {
        self.parse_value().unwrap_or(default)
    }

    /// Serialize to "name value" string.
    pub fn to_string(&self) -> String {
        if self.value.is_empty() {
            self.name.clone()
        } else {
            format!("{} {}", self.name, self.value)
        }
    }
}

impl fmt::Display for ConfEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.value.is_empty() {
            write!(f, "{}", self.name)
        } else {
            write!(f, "{} {}", self.name, self.value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_line_simple() {
        let entry = ConfEntry::from_line("PROTOCOL TCP").unwrap();
        assert_eq!(entry.name, "PROTOCOL");
        assert_eq!(entry.value, "TCP");
    }

    #[test]
    fn test_from_line_with_comment() {
        assert!(ConfEntry::from_line("# this is a comment").is_none());
    }

    #[test]
    fn test_from_line_empty() {
        assert!(ConfEntry::from_line("").is_none());
        assert!(ConfEntry::from_line("   ").is_none());
    }

    #[test]
    fn test_from_line_name_only() {
        let entry = ConfEntry::from_line("SINGLETON").unwrap();
        assert_eq!(entry.name, "SINGLETON");
        assert_eq!(entry.value, "");
    }

    #[test]
    fn test_from_line_with_tabs() {
        let entry = ConfEntry::from_line("PORT\t\t50000").unwrap();
        assert_eq!(entry.name, "PORT");
        assert_eq!(entry.value, "50000");
    }

    #[test]
    fn test_parse_value() {
        let entry = ConfEntry::from_line("PORT 50000").unwrap();
        assert_eq!(entry.parse_value::<u16>(), Some(50000));
        assert_eq!(entry.parse_value::<i32>(), Some(50000));
    }

    #[test]
    fn test_value_or() {
        let entry = ConfEntry::from_line("PORT 50000").unwrap();
        assert_eq!(entry.value_or(0u16), 50000);

        let empty = ConfEntry::from_line("COMMENTED_OUT").unwrap();
        assert_eq!(empty.value_or(9999u16), 9999);
    }

    #[test]
    fn test_display() {
        let entry = ConfEntry::new("PROTOCOL", "TCP");
        assert_eq!(format!("{}", entry), "PROTOCOL TCP");

        let entry2 = ConfEntry::new("SINGLE", "");
        assert_eq!(format!("{}", entry2), "SINGLE");
    }

    #[test]
    fn test_to_string() {
        let entry = ConfEntry::new("MSG_MAXSIZE", "1000000");
        assert_eq!(entry.to_string(), "MSG_MAXSIZE 1000000");
    }
}
