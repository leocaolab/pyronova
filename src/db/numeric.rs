//! Postgres NUMERIC binary wire format ⇄ plain decimal text (`-12.50`, `NaN`, `Infinity`).
//!
//! Hand-rolled because the decimal crates sqlx supports each lose values Python's `Decimal`
//! holds: `rust_decimal` stops at 28 digits, `bigdecimal` has no NaN or ±Infinity.
//!
//! Wire layout (`numeric.c`, `numeric_send`): four big-endian u16 header words
//! `ndigits, weight (i16), sign, dscale`, then `ndigits` base-10000 digits. The value is
//! `Σ digit[i] · 10000^(weight − i)`; `dscale` is the number of decimal places shown.

const DIGITS_PER_GROUP: usize = 4;
const HEADER_WORDS: usize = 4;
/// Every group is one base-10000 digit.
const NBASE: u16 = 10_000;
/// The largest display scale Postgres accepts (`NUMERIC_DSCALE_MAX`, `numeric.c`);
/// `numeric_recv` rejects a larger one.
const MAX_DSCALE: u16 = 0x3FFF;

const SIGN_POSITIVE: u16 = 0x0000;
const SIGN_NEGATIVE: u16 = 0x4000;
const SIGN_NAN: u16 = 0xC000;
const SIGN_POS_INFINITY: u16 = 0xD000;
const SIGN_NEG_INFINITY: u16 = 0xF000;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum NumericError {
    #[error("numeric value of {0} bytes is shorter than its header says")]
    Truncated(usize),
    #[error("numeric value has an unknown sign word {0:#06x}")]
    Sign(u16),
    #[error("{0:?} is not a decimal number")]
    Syntax(String),
    #[error("{0:?} has more digits than a Postgres numeric holds")]
    TooLarge(String),
    #[error("{0:?} has more than {MAX_DSCALE} decimal places, the most a Postgres numeric shows")]
    Scale(String),
    #[error("numeric value has a digit group {0}, not a base-10000 digit")]
    Digit(u16),
}

/// Reads a binary NUMERIC as the decimal text Python's `Decimal` parses exactly.
pub(crate) fn decode(buf: &[u8]) -> Result<String, NumericError> {
    let word = |i: usize| {
        buf.get(2 * i..2 * i + 2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]))
            .ok_or(NumericError::Truncated(buf.len()))
    };
    let ndigits = usize::from(word(0)?);
    let weight = i32::from(word(1)? as i16);
    let sign = word(2)?;
    let dscale = usize::from(word(3)?);
    let digits = (0..ndigits)
        .map(|i| word(HEADER_WORDS + i))
        .collect::<Result<Vec<u16>, _>>()?;
    if let Some(&digit) = digits.iter().find(|&&d| d >= NBASE) {
        return Err(NumericError::Digit(digit));
    }

    let prefix = match sign {
        SIGN_POSITIVE => "",
        SIGN_NEGATIVE => "-",
        SIGN_NAN => return Ok("NaN".to_owned()),
        SIGN_POS_INFINITY => return Ok("Infinity".to_owned()),
        SIGN_NEG_INFINITY => return Ok("-Infinity".to_owned()),
        other => return Err(NumericError::Sign(other)),
    };
    let group = |g: i32| {
        usize::try_from(g)
            .ok()
            .and_then(|g| digits.get(g).copied())
            .unwrap_or(0)
    };

    let integer = if weight < 0 {
        "0".to_owned()
    } else {
        let rest: String = (1..=weight).map(|g| format!("{:04}", group(g))).collect();
        format!("{}{rest}", group(0))
    };
    let fraction_groups = dscale.div_ceil(DIGITS_PER_GROUP) as i32;
    let mut fraction: String = (weight + 1..weight + 1 + fraction_groups)
        .map(|g| format!("{:04}", group(g)))
        .collect();
    fraction.truncate(dscale);

    Ok(if fraction.is_empty() {
        format!("{prefix}{integer}")
    } else {
        format!("{prefix}{integer}.{fraction}")
    })
}

/// Writes plain decimal text (`[-]digits[.digits]`, `NaN`, `[-]Infinity` or Rust's
/// `[-]inf`) as a binary NUMERIC.
pub(crate) fn encode(text: &str, out: &mut Vec<u8>) -> Result<(), NumericError> {
    let special = match text {
        "NaN" => Some(SIGN_NAN),
        "Infinity" | "inf" => Some(SIGN_POS_INFINITY),
        "-Infinity" | "-inf" => Some(SIGN_NEG_INFINITY),
        _ => None,
    };
    if let Some(sign) = special {
        write_words(out, &[0, 0, sign, 0]);
        return Ok(());
    }

    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let is_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if integer.is_empty() && fraction.is_empty() || !is_digits(integer) || !is_digits(fraction) {
        return Err(NumericError::Syntax(text.to_owned()));
    }

    let integer = integer.trim_start_matches('0');
    let lead_pad = (DIGITS_PER_GROUP - integer.len() % DIGITS_PER_GROUP) % DIGITS_PER_GROUP;
    let tail_pad = (DIGITS_PER_GROUP - fraction.len() % DIGITS_PER_GROUP) % DIGITS_PER_GROUP;
    let padded: Vec<u8> = std::iter::repeat_n(b'0', lead_pad)
        .chain(integer.bytes())
        .chain(fraction.bytes())
        .chain(std::iter::repeat_n(b'0', tail_pad))
        .collect();
    let groups: Vec<u16> = padded
        .chunks(DIGITS_PER_GROUP)
        .map(|c| c.iter().fold(0u16, |acc, d| acc * 10 + u16::from(d - b'0')))
        .collect();
    let integer_groups = (lead_pad + integer.len()) / DIGITS_PER_GROUP;

    // Postgres stores no leading or trailing zero groups; zero has no groups at all.
    let leading_zeros = groups.iter().take_while(|&&g| g == 0).count();
    let significant = groups[leading_zeros..]
        .iter()
        .rposition(|&g| g != 0)
        .map_or(&[][..], |last| {
            &groups[leading_zeros..=leading_zeros + last]
        });
    let too_large = || NumericError::TooLarge(text.to_owned());
    let weight = if significant.is_empty() {
        0
    } else {
        i16::try_from(integer_groups as i64 - 1 - leading_zeros as i64).map_err(|_| too_large())?
    };
    let sign = if negative && !significant.is_empty() {
        SIGN_NEGATIVE
    } else {
        SIGN_POSITIVE
    };
    let ndigits = u16::try_from(significant.len()).map_err(|_| too_large())?;
    let dscale = u16::try_from(fraction.len())
        .ok()
        .filter(|&scale| scale <= MAX_DSCALE)
        .ok_or_else(|| NumericError::Scale(text.to_owned()))?;

    write_words(out, &[ndigits, weight as u16, sign, dscale]);
    write_words(out, significant);
    Ok(())
}

fn write_words(out: &mut Vec<u8>, words: &[u16]) {
    out.extend(words.iter().flat_map(|w| w.to_be_bytes()));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(text: &str) -> String {
        let mut wire = Vec::new();
        encode(text, &mut wire).unwrap();
        decode(&wire).unwrap()
    }

    #[test]
    fn decodes_what_postgres_sends() {
        // SELECT numeric_send(12.50::numeric(6,2)): 2 groups, weight 0, scale 2 → [12, 5000].
        let wire = [0, 2, 0, 0, 0, 0, 0, 2, 0, 12, 0x13, 0x88];
        assert_eq!(decode(&wire).unwrap(), "12.50");
        // SELECT numeric_send(0.000001): weight -2, scale 6 → [100].
        let wire = [0, 1, 0xff, 0xfe, 0, 0, 0, 6, 0, 100];
        assert_eq!(decode(&wire).unwrap(), "0.000001");
    }

    #[test]
    fn round_trips_keep_digits_and_scale() {
        for text in [
            "0",
            "0.00",
            "7",
            "10000",
            "-12.50",
            "12345678901234567890.000123400",
            "-98765432109876543210.5",
            "0.00000001",
            "1000000000000000000000000000000000000000",
        ] {
            assert_eq!(round_trip(text), text);
        }
        assert_eq!(round_trip("-0.0"), "0.0");
        assert_eq!(round_trip("007.5"), "7.5");
    }

    #[test]
    fn specials_round_trip() {
        assert_eq!(round_trip("NaN"), "NaN");
        assert_eq!(round_trip("inf"), "Infinity");
        assert_eq!(round_trip("-Infinity"), "-Infinity");
    }

    #[test]
    fn rejects_what_is_not_a_plain_decimal() {
        for text in ["", ".", "1e5", "1.2.3", "--1", "sNaN", "１"] {
            let mut wire = Vec::new();
            assert_eq!(
                encode(text, &mut wire),
                Err(NumericError::Syntax(text.to_owned())),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_digit_group_of_10000_or_more_is_an_error() {
        // ndigits 1, weight 0, positive, dscale 0, digit 10000
        let wire = [0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x10];
        assert_eq!(decode(&wire), Err(NumericError::Digit(10_000)));
        let wire = [0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x0f];
        assert_eq!(decode(&wire).unwrap(), "9999");
    }

    #[test]
    fn a_scale_past_0x3fff_is_refused() {
        let at_max = format!("0.{}", "1".repeat(usize::from(MAX_DSCALE)));
        let mut wire = Vec::new();
        assert_eq!(encode(&at_max, &mut wire), Ok(()));
        let past = format!("0.{}", "1".repeat(usize::from(MAX_DSCALE) + 1));
        assert_eq!(
            encode(&past, &mut Vec::new()),
            Err(NumericError::Scale(past.clone()))
        );
    }

    #[test]
    fn truncated_wire_is_an_error() {
        assert_eq!(decode(&[0, 1, 0, 0]), Err(NumericError::Truncated(4)));
        assert_eq!(
            decode(&[0, 0, 0, 0, 0x12, 0x34, 0, 0]),
            Err(NumericError::Sign(0x1234))
        );
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Plain decimal text as `decode` writes it: no leading zeros, no `-0`.
    fn canonical_decimal() -> impl Strategy<Value = String> {
        (
            any::<bool>(),
            "0|[1-9][0-9]{0,40}",
            proptest::option::of("[0-9]{1,40}"),
        )
            .prop_map(|(negative, integer, fraction)| {
                let unsigned = match fraction {
                    Some(fraction) => format!("{integer}.{fraction}"),
                    None => integer,
                };
                let is_zero = unsigned.bytes().all(|b| b == b'0' || b == b'.');
                if negative && !is_zero {
                    format!("-{unsigned}")
                } else {
                    unsigned
                }
            })
    }

    /// `text` without leading integer zeros or the sign of a zero: the same number as
    /// `decode` writes it for a wire in Postgres's own normal form.
    fn normal_form(text: &str) -> String {
        let (negative, unsigned) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text),
        };
        let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        let integer = match integer.trim_start_matches('0') {
            "" => "0",
            trimmed => trimmed,
        };
        let is_zero = integer == "0" && fraction.bytes().all(|b| b == b'0');
        let sign = if negative && !is_zero { "-" } else { "" };
        if fraction.is_empty() {
            format!("{sign}{integer}")
        } else {
            format!("{sign}{integer}.{fraction}")
        }
    }

    proptest! {
        #[test]
        fn canonical_text_round_trips(text in canonical_decimal()) {
            let mut wire = Vec::new();
            encode(&text, &mut wire).unwrap();
            prop_assert_eq!(decode(&wire).unwrap(), text);
        }

        #[test]
        fn the_wire_carries_no_leading_or_trailing_zero_group(text in canonical_decimal()) {
            let mut wire = Vec::new();
            encode(&text, &mut wire).unwrap();
            let groups: Vec<u16> = wire[2 * HEADER_WORDS..]
                .chunks(2)
                .map(|b| u16::from_be_bytes([b[0], b[1]]))
                .collect();
            prop_assert_eq!(usize::from(u16::from_be_bytes([wire[0], wire[1]])), groups.len());
            prop_assert!(groups.iter().all(|&g| g < NBASE));
            prop_assert!(groups.first().is_none_or(|&g| g != 0));
            prop_assert!(groups.last().is_none_or(|&g| g != 0));
        }

        #[test]
        fn any_text_encodes_or_is_refused_without_panicking(text in "\\PC{0,50}") {
            let mut wire = Vec::new();
            if encode(&text, &mut wire).is_ok() {
                prop_assert!(decode(&wire).is_ok());
            }
        }

        #[test]
        fn any_wire_decodes_or_is_refused_and_a_decoded_value_is_stable(
            weight in -10i16..10,
            sign in prop_oneof![
                Just(SIGN_POSITIVE),
                Just(SIGN_NEGATIVE),
                Just(SIGN_NAN),
                Just(SIGN_POS_INFINITY),
                Just(SIGN_NEG_INFINITY),
                any::<u16>(),
            ],
            dscale in prop_oneof![0u16..40, any::<u16>()],
            digits in proptest::collection::vec(prop_oneof![9 => 0u16..NBASE, 1 => any::<u16>()], 0..8),
            cut in any::<prop::sample::Index>(),
        ) {
            let mut wire = Vec::new();
            write_words(&mut wire, &[digits.len() as u16, weight as u16, sign, dscale]);
            write_words(&mut wire, &digits);
            // Sometimes truncated: the header promises more than the buffer holds.
            if cut.index(4) == 0 {
                wire.truncate(cut.index(wire.len() + 1));
            }
            let Ok(text) = decode(&wire) else {
                return Ok(());
            };
            let mut again = Vec::new();
            // A wire scale past what Postgres shows is readable but not writable.
            if encode(&text, &mut again).is_ok() {
                let expected = match text.as_str() {
                    "NaN" | "Infinity" | "-Infinity" => text.clone(),
                    number => normal_form(number),
                };
                prop_assert_eq!(decode(&again).unwrap(), expected);
            }
        }
    }
}
