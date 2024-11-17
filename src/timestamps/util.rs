use std::num::ParseIntError;
use thiserror::Error;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct Scanner<'a, E> {
    s: &'a str,
    err: E,
}

impl<'a, E: Copy> Scanner<'a, E> {
    pub(super) fn new(s: &'a str, err: E) -> Self {
        Scanner { s, err }
    }

    pub(super) fn scan_year(&mut self) -> Result<u16, E> {
        let Some((year_str, t)) = self.s.split_at_checked(4) else {
            return Err(self.err);
        };
        if !year_str.chars().all(|c| c.is_ascii_digit()) {
            return Err(self.err);
        }
        let Ok(year) = year_str.parse::<u16>() else {
            return Err(self.err);
        };
        self.s = t;
        Ok(year)
    }

    pub(super) fn scan_u8(&mut self, min: u8, max: u8) -> Result<u8, E> {
        let Some((ss, t2)) = self.s.split_at_checked(2) else {
            return Err(self.err);
        };
        if !ss.chars().all(|c| c.is_ascii_digit()) {
            return Err(self.err);
        }
        let Ok(value) = ss.parse::<u8>() else {
            return Err(self.err);
        };
        if !((min..=max).contains(&value)) {
            return Err(self.err);
        };
        self.s = t2;
        Ok(value)
    }

    pub(super) fn scan_char(&mut self, c: char) -> Result<(), E> {
        let Some(t2) = self.s.strip_prefix(c) else {
            return Err(self.err);
        };
        self.s = t2;
        Ok(())
    }

    pub(super) fn eof(&self) -> Result<(), E> {
        if !self.s.is_empty() {
            Err(self.err)
        } else {
            Ok(())
        }
    }
}