// coord-core/workflow/cron.rs
// CRON 表达式解析与下一次触发时间计算（标准 §Scheduling `schedule.cron`）
//
// 支持 5 字段（分 时 日 月 周）与 6 字段（秒 分 时 日 月 周）：
//   ┌───────────── 秒 (0-59, 可选，6 字段才有)
//   │ ┌─────────── 分 (0-59)
//   │ │ ┌───────── 时 (0-23)
//   │ │ │ ┌─────── 日 (1-31)
//   │ │ │ │ ┌───── 月 (1-12)
//   │ │ │ │ │ ┌─── 周 (0-7, 0/7 = 周日)
//   │ │ │ │ │ │
//   * * * * * *
//
// 字段支持：`*`、`*/n`（步进）、`a-b`（范围）、`a,b,c`（列表）、
// `a-b/n`（范围步进）、月份/星期名称（jan-dec、sun-sat，大小写不敏感）。
// 纯函数、无 I/O、无外部依赖（与 expression.rs / jsonschema.rs 同风格）。

/// CRON 字段值集合（已展开的允许值位图，0-59 位）
#[derive(Debug, Clone, PartialEq)]
pub struct CronField {
    /// 该字段允许的取值（升序、去重）
    values: Vec<u32>,
}

impl CronField {
    fn contains(&self, v: u32) -> bool {
        self.values.binary_search(&v).is_ok()
    }
}

/// 解析后的 CRON 表达式
#[derive(Debug, Clone, PartialEq)]
pub struct CronSchedule {
    seconds: CronField,
    minutes: CronField,
    hours: CronField,
    days: CronField,
    months: CronField,
    weekdays: CronField,
}

/// CRON 解析错误
#[derive(Debug, Clone, PartialEq)]
pub struct CronError(pub String);

impl std::fmt::Display for CronError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid cron expression: {}", self.0)
    }
}

/// 解析 CRON 表达式（5 或 6 字段）
pub fn parse_cron(expr: &str) -> Result<CronSchedule, CronError> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 && fields.len() != 6 {
        return Err(CronError(format!(
            "expected 5 or 6 fields, got {}",
            fields.len()
        )));
    }
    let has_seconds = fields.len() == 6;
    let mut idx = 0;
    let seconds = if has_seconds {
        let f = parse_field(fields[idx], 0, 59)?;
        idx += 1;
        f
    } else {
        // 5 字段：秒默认为 0
        CronField {
            values: vec![0],
        }
    };
    let minutes = parse_field(fields[idx], 0, 59)?;
    let hours = parse_field(fields[idx + 1], 0, 23)?;
    let days = parse_field(fields[idx + 2], 1, 31)?;
    let months = parse_field_names(fields[idx + 3], 1, 12, &MONTH_NAMES)?;
    let weekdays = parse_field_names(fields[idx + 4], 0, 7, &WEEKDAY_NAMES)?;

    Ok(CronSchedule {
        seconds,
        minutes,
        hours,
        days,
        months,
        weekdays,
    })
}

const MONTH_NAMES: &[(&str, u32)] = &[
    ("jan", 1), ("feb", 2), ("mar", 3), ("apr", 4), ("may", 5), ("jun", 6),
    ("jul", 7), ("aug", 8), ("sep", 9), ("oct", 10), ("nov", 11), ("dec", 12),
];

const WEEKDAY_NAMES: &[(&str, u32)] = &[
    ("sun", 0), ("mon", 1), ("tue", 2), ("wed", 3), ("thu", 4), ("fri", 5), ("sat", 6),
];

fn month_num(name: &str) -> Option<u32> {
    let lower = name.to_ascii_lowercase();
    let short = &lower[..lower.len().min(3)];
    MONTH_NAMES
        .iter()
        .find(|(n, _)| *n == short)
        .map(|(_, v)| *v)
}

fn weekday_num(name: &str) -> Option<u32> {
    let lower = name.to_ascii_lowercase();
    let short = &lower[..lower.len().min(3)];
    WEEKDAY_NAMES
        .iter()
        .find(|(n, _)| *n == short)
        .map(|(_, v)| *v)
}

/// 解析字段（含月份/星期名称解析）
fn parse_field_names(
    field: &str,
    min: u32,
    max: u32,
    names: &[(&str, u32)],
) -> Result<CronField, CronError> {
    let lookup = |part: &str| -> String {
        if part.parse::<u32>().is_ok() || part == "*" {
            return part.to_string();
        }
        let num = if names == MONTH_NAMES {
            month_num(part)
        } else {
            weekday_num(part)
        };
        match num {
            Some(v) => v.to_string(),
            None => part.to_string(),
        }
    };

    // 逐逗号段处理；范围/步进先映射名称端点（如 mon-fri → 1-5）
    let mut mapped_parts = Vec::new();
    for part in field.split(',') {
        let part = part.trim();
        if part.contains('-') && !part.starts_with("*/") && !part.starts_with('*') {
            // a-b 范围：映射两端（排除 a-b/n 中的斜杠部分先拆分）
            let (range, step) = match part.split_once('/') {
                Some((r, s)) => (r, Some(format!("/{s}"))),
                None => (part, None),
            };
            if let Some(dash) = range.find('-') {
                let l = lookup(&range[..dash]);
                let r = lookup(&range[dash + 1..]);
                let mapped = format!("{l}-{r}{}", step.unwrap_or_default());
                mapped_parts.push(mapped);
                continue;
            }
        }
        mapped_parts.push(lookup(part));
    }

    let joined = mapped_parts.join(",");
    parse_field(&joined, min, max)
}

/// 解析单个字段（* / */n / a-b / a-b/n / a,b,c）
fn parse_field(field: &str, min: u32, max: u32) -> Result<CronField, CronError> {
    let mut values: Vec<u32> = Vec::new();
    for part in field.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // a-b/n 或 */n 或 a-b 或 a
        let (range, step) = match part.split('/').collect::<Vec<_>>().as_slice() {
            [r] => (*r, 1u32),
            [r, s] => {
                let step = s
                    .parse::<u32>()
                    .map_err(|_| CronError(format!("invalid step in '{part}'")))?;
                if step == 0 {
                    return Err(CronError(format!("step cannot be zero in '{part}'")));
                }
                (*r, step)
            }
            _ => return Err(CronError(format!("invalid field '{part}'"))),
        };

        let (start, end) = if range == "*" {
            (min, max)
        } else if let Some(dash) = range.find('-') {
            let s = range[..dash]
                .parse::<u32>()
                .map_err(|_| CronError(format!("invalid range start in '{part}'")))?;
            let e = range[dash + 1..]
                .parse::<u32>()
                .map_err(|_| CronError(format!("invalid range end in '{part}'")))?;
            (s, e)
        } else {
            let v = range
                .parse::<u32>()
                .map_err(|_| CronError(format!("invalid value '{part}'")))?;
            (v, v)
        };

        if start < min || end > max || start > end {
            return Err(CronError(format!(
                "value out of range in '{part}' (expected {min}-{max})"
            )));
        }

        let mut v = start;
        while v <= end {
            values.push(v);
            v += step;
        }
    }

    if values.is_empty() {
        return Err(CronError(format!("field '{field}' matches nothing")));
    }
    values.sort_unstable();
    values.dedup();
    Ok(CronField { values })
}

/// 计算 `after` 之后的最近一次触发时间（Unix 秒）
///
/// 从 after+1 秒开始扫描，最多扫描 5 年（避免死循环）。
pub fn next_fire(schedule: &CronSchedule, after_secs: i64) -> Option<i64> {
    let mut ts = after_secs + 1;
    let limit = after_secs + 5 * 366 * 24 * 3600;
    while ts <= limit {
        let (_y, mo, d, h, mi, s, wd) = to_calendar(ts);
        if schedule.months.contains(mo)
            && schedule.days.contains(d)
            && schedule.hours.contains(h)
            && schedule.minutes.contains(mi)
            && schedule.seconds.contains(s)
            && (schedule.weekdays.contains(wd) || schedule.weekdays.contains(7) && wd == 0)
        {
            return Some(ts);
        }
        ts += 1;
    }
    None
}

/// 将 Unix 秒转换为日历字段（公历，1970+）
fn to_calendar(secs: i64) -> (i64, u32, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let h = (rem / 3600) as u32;
    let mi = ((rem % 3600) / 60) as u32;
    let s = (rem % 60) as u32;
    // 1970-01-01 = 星期四 (dow 4)
    let wd = ((days + 4).rem_euclid(7)) as u32;

    // 公历日期换算（Hinnant 算法）
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let _y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (_y, m, d, h, mi, s, wd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_fields() {
        let c = parse_cron("*/5 * * * *").unwrap();
        assert!(c.minutes.contains(0));
        assert!(c.minutes.contains(5));
        assert!(c.minutes.contains(55));
        assert!(!c.minutes.contains(3));
    }

    #[test]
    fn test_parse_six_fields() {
        let c = parse_cron("0 0 12 * * *").unwrap();
        assert!(c.seconds.contains(0));
        assert!(c.minutes.contains(0));
        assert!(c.hours.contains(12));
    }

    #[test]
    fn test_parse_range_and_list() {
        let c = parse_cron("10-20/5 1,15 * * *").unwrap();
        assert!(c.minutes.contains(10));
        assert!(c.minutes.contains(15));
        assert!(c.minutes.contains(20));
        assert!(!c.minutes.contains(21));
        assert!(c.hours.contains(1));
        assert!(c.hours.contains(15));
        assert!(!c.hours.contains(2));
    }

    #[test]
    fn test_parse_names() {
        let c = parse_cron("0 9 * jan,mar mon-fri").unwrap();
        assert!(c.months.contains(1));
        assert!(c.months.contains(3));
        assert!(!c.months.contains(2));
        assert!(c.weekdays.contains(1));
        assert!(c.weekdays.contains(5));
        assert!(!c.weekdays.contains(0));
    }

    #[test]
    fn test_parse_invalid() {
        assert!(parse_cron("").is_err());
        assert!(parse_cron("* * * *").is_err()); // 4 fields
        assert!(parse_cron("60 * * * *").is_err()); // minute out of range
        assert!(parse_cron("* * * * * * *").is_err()); // 7 fields
        assert!(parse_cron("*/0 * * * *").is_err()); // zero step
    }

    #[test]
    fn test_next_fire_every_minute() {
        let c = parse_cron("* * * * *").unwrap();
        // after = 2024-01-01 00:00:00 UTC；5 字段秒=0，下次触发为 00:01:00
        let after = 1704067200;
        let next = next_fire(&c, after).unwrap();
        assert_eq!(next, after + 60);
    }

    #[test]
    fn test_next_fire_specific_time() {
        // 每天 09:30
        let c = parse_cron("30 9 * * *").unwrap();
        let after = 1704067200; // 2024-01-01 00:00:00
        let next = next_fire(&c, after).unwrap();
        // 2024-01-01 09:30:00 UTC = 1704101400
        assert_eq!(next, 1704101400);
    }

    #[test]
    fn test_next_fire_hourly() {
        // 每小时第 0 分
        let c = parse_cron("0 * * * *").unwrap();
        let after = 1704067200; // 00:00:00
        let next = next_fire(&c, after).unwrap();
        assert_eq!(next, 1704070800); // 01:00:00
    }

    #[test]
    fn test_weekday_match() {
        // 每工作日 09:00
        let c = parse_cron("0 9 * * 1-5").unwrap();
        // 2024-01-06 是周六，2024-01-08 是周一
        let sat = 1704499200; // 2024-01-06 00:00:00
        let next = next_fire(&c, sat).unwrap();
        let (_, _, _, h, mi, _, wd) = to_calendar(next);
        assert_eq!(h, 9);
        assert_eq!(mi, 0);
        assert!(wd >= 1 && wd <= 5);
    }
}
