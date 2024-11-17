use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DateHM {
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
}

impl fmt::Display for DateHM {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02}T{:02}-{:02}Z",
            self.year, self.month, self.day, self.hour, self.minute
        )
    }
}

impl FromStr for DateHM {
    type Err = DateHMError;

    fn from_str(s: &str) -> Result<DateHM, DateHMError> {
        fn accept(t: &mut &str, c: char) -> Result<(), DateHMError> {
            let Some(t2) = t.strip_prefix(c) else {
                return Err(DateHMError);
            };
            *t = t2;
            Ok(())
        }

        fn parse_u8(t: &mut &str, min: u8, max: u8) -> Result<u8, DateHMError> {
            let Some((ss, t2)) = t.split_at_checked(2) else {
                return Err(DateHMError);
            };
            if !ss.chars().all(|c| c.is_ascii_digit()) {
                return Err(DateHMError);
            }
            let Ok(value) = ss.parse::<u8>() else {
                return Err(DateHMError);
            };
            if !((min..=max).contains(&value)) {
                return Err(DateHMError);
            };
            *t = t2;
            Ok(value)
        }

        let Some((year_str, mut s)) = s.split_at_checked(4) else {
            return Err(DateHMError);
        };
        if !year_str.chars().all(|c| c.is_ascii_digit()) {
            return Err(DateHMError);
        }
        let Ok(year) = year_str.parse::<u16>() else {
            return Err(DateHMError);
        };
        accept(&mut s, '-')?;
        let month = parse_u8(&mut s, 1, 12)?;
        accept(&mut s, '-')?;
        let day = parse_u8(&mut s, 1, 31)?;
        accept(&mut s, 'T')?;
        let hour = parse_u8(&mut s, 0, 23)?;
        accept(&mut s, '-')?;
        let minute = parse_u8(&mut s, 0, 59)?;
        accept(&mut s, 'Z')?;
        if !s.is_empty() {
            return Err(DateHMError);
        }
        Ok(DateHM {
            year,
            month,
            day,
            hour,
            minute,
        })
    }
}

#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid timestamp format; expected YYYY-MM-DDTHH-MMZ")]
pub(crate) struct DateHMError;

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("2024-01-01T00-00Z", 2024, 1, 1, 0, 0)]
    #[case("2024-11-14T14-58Z", 2024, 11, 14, 14, 58)]
    #[case("2024-12-31T23-59Z", 2024, 12, 31, 23, 59)]
    fn parse(
        #[case] s: &str,
        #[case] year: u16,
        #[case] month: u8,
        #[case] day: u8,
        #[case] hour: u8,
        #[case] minute: u8,
    ) {
        assert_eq!(
            s.parse(),
            Ok(DateHM {
                year,
                month,
                day,
                hour,
                minute
            })
        );
    }

    #[rstest]
    #[case("2024-00-01T01-00Z")]
    #[case("2024-13-01T01-00Z")]
    #[case("2024-10-00T01-02Z")]
    #[case("2024-10-32T01-02Z")]
    #[case("2024-10-15")]
    #[case("2024-10-15T24-02Z")]
    #[case("2024-10-15T01-60Z")]
    #[case("2024-1-2T3-4Z")]
    #[case("224-12-01T01-00Z")]
    #[case("2024-12-01T01-00")]
    #[case("2024-12-01-01-00Z")]
    fn parse_err(#[case] s: &str) {
        assert_eq!(s.parse::<DateHM>(), Err(DateHMError));
    }

    #[rstest]
    #[case(DateHM {year: 2024, month: 1, day: 1, hour: 0, minute: 0}, "2024-01-01T00-00Z")]
    #[case(DateHM {year: 2024, month: 12, day: 31, hour: 23, minute: 59}, "2024-12-31T23-59Z")]
    fn display(#[case] it: DateHM, #[case] s: &str) {
        assert_eq!(it.to_string(), s);
    }
}
