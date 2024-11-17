use super::util::Scanner;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Date {
    year: u16,
    month: u8,
    day: u8,
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

impl FromStr for Date {
    type Err = DateError;

    fn from_str(s: &str) -> Result<Date, DateError> {
        let mut scanner = Scanner::new(s, DateError);
        let year = scanner.scan_year()?;
        scanner.scan_char('-')?;
        let month = scanner.scan_u8(1, 12)?;
        scanner.scan_char('-')?;
        let day = scanner.scan_u8(1, 31)?;
        scanner.eof()?;
        Ok(Date { year, month, day })
    }
}

#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid timestamp format; expected YYYY-MM-DD")]
pub(crate) struct DateError;

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("2024-01-01", 2024, 1, 1)]
    #[case("2024-11-14", 2024, 11, 14)]
    #[case("2024-12-31", 2024, 12, 31)]
    fn parse(#[case] s: &str, #[case] year: u16, #[case] month: u8, #[case] day: u8) {
        assert_eq!(s.parse(), Ok(Date { year, month, day }));
    }

    #[rstest]
    #[case("2024-00-01")]
    #[case("2024-13-01")]
    #[case("2024-10-00")]
    #[case("2024-10-32")]
    #[case("2024-1-2")]
    #[case("224-12-01")]
    #[case("2024-12-0")]
    #[case("2024-10-15T12-02Z")]
    #[case("2024-12-01-01-00Z")]
    fn parse_err(#[case] s: &str) {
        assert_eq!(s.parse::<Date>(), Err(DateError));
    }

    #[rstest]
    #[case(Date {year: 2024, month: 1, day: 1}, "2024-01-01")]
    #[case(Date {year: 2024, month: 12, day: 31}, "2024-12-31")]
    fn display(#[case] it: Date, #[case] s: &str) {
        assert_eq!(it.to_string(), s);
    }
}
