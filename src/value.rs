use mysql_async::Value as MySqlValue;
use serde_json::Value as JsonValue;

#[must_use]
pub fn mysql_value_to_json(value: &MySqlValue) -> JsonValue {
    match value {
        MySqlValue::NULL => JsonValue::Null,
        MySqlValue::Bytes(bytes) => JsonValue::String(String::from_utf8_lossy(bytes).into_owned()),
        MySqlValue::Int(value) => JsonValue::from(*value),
        MySqlValue::UInt(value) => JsonValue::from(*value),
        MySqlValue::Float(value) => JsonValue::from(f64::from(*value)),
        MySqlValue::Double(value) => JsonValue::from(*value),
        MySqlValue::Date(year, month, day, hour, minute, second, micros) => JsonValue::String(
            format_datetime(*year, *month, *day, *hour, *minute, *second, *micros),
        ),
        MySqlValue::Time(negative, days, hours, minutes, seconds, micros) => JsonValue::String(
            format_time(*negative, *days, *hours, *minutes, *seconds, *micros),
        ),
    }
}

#[must_use]
pub fn mysql_value_to_document_id(value: &MySqlValue) -> String {
    match value {
        MySqlValue::NULL => String::new(),
        MySqlValue::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        MySqlValue::Int(value) => value.to_string(),
        MySqlValue::UInt(value) => value.to_string(),
        MySqlValue::Float(value) => value.to_string(),
        MySqlValue::Double(value) => value.to_string(),
        MySqlValue::Date(year, month, day, hour, minute, second, micros) => {
            format_datetime(*year, *month, *day, *hour, *minute, *second, *micros)
        }
        MySqlValue::Time(negative, days, hours, minutes, seconds, micros) => {
            format_time(*negative, *days, *hours, *minutes, *seconds, *micros)
        }
    }
}

fn format_datetime(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
) -> String {
    if micros == 0 {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}")
    }
}

fn format_time(
    negative: bool,
    days: u32,
    hours: u8,
    minutes: u8,
    seconds: u8,
    micros: u32,
) -> String {
    let sign = if negative { "-" } else { "" };
    let total_hours = days.saturating_mul(24).saturating_add(u32::from(hours));
    if micros == 0 {
        format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_datetime_for_json() {
        let value = MySqlValue::Date(2026, 6, 28, 17, 8, 9, 42);
        assert_eq!(
            mysql_value_to_json(&value),
            JsonValue::String("2026-06-28T17:08:09.000042".to_owned())
        );
    }

    #[test]
    fn converts_scalar_values_to_json() {
        assert_eq!(mysql_value_to_json(&MySqlValue::NULL), JsonValue::Null);
        assert_eq!(
            mysql_value_to_json(&MySqlValue::Int(-12)),
            JsonValue::from(-12)
        );
        assert_eq!(
            mysql_value_to_json(&MySqlValue::UInt(12)),
            JsonValue::from(12_u64)
        );
        assert_eq!(
            mysql_value_to_json(&MySqlValue::Bytes(b"SKU-42".to_vec())),
            JsonValue::String("SKU-42".to_owned())
        );
    }

    #[test]
    fn formats_time_with_days_and_microseconds() {
        let value = MySqlValue::Time(true, 2, 3, 4, 5, 6);

        assert_eq!(
            mysql_value_to_json(&value),
            JsonValue::String("-51:04:05.000006".to_owned())
        );
        assert_eq!(mysql_value_to_document_id(&value), "-51:04:05.000006");
    }

    #[test]
    fn converts_values_to_document_ids() {
        assert_eq!(mysql_value_to_document_id(&MySqlValue::NULL), "");
        assert_eq!(
            mysql_value_to_document_id(&MySqlValue::Bytes(b"SKU-42".to_vec())),
            "SKU-42"
        );
        assert_eq!(mysql_value_to_document_id(&MySqlValue::Int(-12)), "-12");
        assert_eq!(mysql_value_to_document_id(&MySqlValue::UInt(12)), "12");
    }
}
