//! First-Expiring-First-Out consumption planning (FR-3.2). Pure: given EARN rows that still have
//! points left — already sorted oldest-expiring first by the caller — and an amount to consume,
//! produce the per-row deductions. No I/O, fully unit-testable.

use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct FefoRow {
    pub id: Uuid,
    pub available: i64,
}

/// Plan how to draw `needed` points from `rows` (consumed in order).
/// `Ok(plan)` lists `(ledger_row_id, take)`; `Err(shortfall)` means the rows hold `shortfall`
/// fewer points than needed — caller must reject the redemption.
pub fn consume(rows: &[FefoRow], needed: i64) -> Result<Vec<(Uuid, i64)>, i64> {
    if needed <= 0 {
        return Ok(Vec::new());
    }
    let mut remaining = needed;
    let mut plan = Vec::new();
    for r in rows {
        if remaining == 0 {
            break;
        }
        let take = r.available.min(remaining);
        if take > 0 {
            plan.push((r.id, take));
            remaining -= take;
        }
    }
    if remaining > 0 {
        Err(remaining)
    } else {
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(n: u128, available: i64) -> FefoRow {
        FefoRow {
            id: Uuid::from_u128(n),
            available,
        }
    }

    #[test]
    fn drains_oldest_first() {
        let rows = [row(1, 30), row(2, 50), row(3, 100)];
        let plan = consume(&rows, 60).unwrap();
        assert_eq!(plan, vec![(rows[0].id, 30), (rows[1].id, 30)]);
    }

    #[test]
    fn exact_and_partial() {
        let rows = [row(1, 40), row(2, 60)];
        assert_eq!(consume(&rows, 100).unwrap().len(), 2);
        assert_eq!(consume(&rows, 40).unwrap(), vec![(rows[0].id, 40)]);
    }

    #[test]
    fn shortfall_is_reported() {
        let rows = [row(1, 10), row(2, 5)];
        assert_eq!(consume(&rows, 100), Err(85));
    }
}
