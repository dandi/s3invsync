use super::date::Date;
use super::datehm::DateHM;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum DateMaybeHM {
    Date(Date),
    DateHM(DateHM),
}

impl fmt::Display for DateMaybeHM {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DateMaybeHM::Date(d) => write!(f, "{d}"),
            DateMaybeHM::DateHM(d) => write!(f, "{d}"),
        }
    }
}

impl FromStr for DateMaybeHM {
    type Err = DateMaybeHMError;

    fn from_str(s: &str) -> Result<DateMaybeHM, DateMaybeHMError> {
        if s.contains('T') {
            match s.parse::<DateHM>() {
                Ok(d) => Ok(DateMaybeHM::DateHM(d)),
                Err(_) => Err(DateMaybeHMError),
            }
        } else {
            match s.parse::<Date>() {
                Ok(d) => Ok(DateMaybeHM::Date(d)),
                Err(_) => Err(DateMaybeHMError),
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid timestamp format; expected YYYY-MM-DD[THH-MMZ]")]
pub(crate) struct DateMaybeHMError;
