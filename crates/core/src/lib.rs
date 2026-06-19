//! Pure domain logic — no I/O. Ledger math, FEFO consumption, lock state machine, rule eval.
//! Kept dependency-free so it can be exhaustively unit-tested without a DB.

pub mod fefo;
pub mod rules;
pub mod webhook;

/// The only five ways points may move (FR-3.1). Stored as the ledger `txn_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnType {
    Earn,
    Redeem,
    Expire,
    Adjustment,
    Unlock,
}

impl TxnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TxnType::Earn => "EARN",
            TxnType::Redeem => "REDEEM",
            TxnType::Expire => "EXPIRE",
            TxnType::Adjustment => "ADJUSTMENT",
            TxnType::Unlock => "UNLOCK",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "EARN" => Some(TxnType::Earn),
            "REDEEM" => Some(TxnType::Redeem),
            "EXPIRE" => Some(TxnType::Expire),
            "ADJUSTMENT" => Some(TxnType::Adjustment),
            "UNLOCK" => Some(TxnType::Unlock),
            _ => None,
        }
    }
}

/// A tier milestone. `id` + the lifetime threshold that unlocks it.
#[derive(Debug, Clone, Copy)]
pub struct Tier {
    pub id: uuid::Uuid,
    pub threshold: i64,
}

/// Pick the highest tier whose threshold the customer has reached (FR-1.3).
/// `tiers` need not be sorted. Returns None when no tier qualifies.
pub fn select_tier(lifetime_earned: i64, tiers: &[Tier]) -> Option<uuid::Uuid> {
    tiers
        .iter()
        .filter(|t| t.threshold <= lifetime_earned)
        .max_by_key(|t| t.threshold)
        .map(|t| t.id)
}

/// Validate an amount for the txn types the P1 write path accepts.
/// EARN must be strictly positive; ADJUSTMENT must be non-zero (signed correction).
pub fn validate_txn(txn_type: TxnType, amount: i64) -> Result<(), &'static str> {
    match txn_type {
        TxnType::Earn if amount <= 0 => Err("EARN amount must be positive"),
        TxnType::Adjustment if amount == 0 => Err("ADJUSTMENT amount must be non-zero"),
        TxnType::Earn | TxnType::Adjustment => Ok(()),
        _ => Err("unsupported transaction type on this endpoint"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_type_strings_are_stable() {
        assert_eq!(TxnType::Earn.as_str(), "EARN");
        assert_eq!(TxnType::Unlock.as_str(), "UNLOCK");
        assert_eq!(TxnType::parse("ADJUSTMENT"), Some(TxnType::Adjustment));
        assert_eq!(TxnType::parse("nope"), None);
    }

    #[test]
    fn select_tier_picks_highest_reached() {
        let g = Tier {
            id: uuid::Uuid::from_u128(1),
            threshold: 0,
        };
        let s = Tier {
            id: uuid::Uuid::from_u128(2),
            threshold: 100,
        };
        let p = Tier {
            id: uuid::Uuid::from_u128(3),
            threshold: 500,
        };
        let tiers = [s, g, p]; // intentionally unsorted
        assert_eq!(select_tier(50, &tiers), Some(g.id));
        assert_eq!(select_tier(100, &tiers), Some(s.id));
        assert_eq!(select_tier(999, &tiers), Some(p.id));
        assert_eq!(select_tier(-1, &[]), None);
    }

    #[test]
    fn validate_txn_rules() {
        assert!(validate_txn(TxnType::Earn, 10).is_ok());
        assert!(validate_txn(TxnType::Earn, 0).is_err());
        assert!(validate_txn(TxnType::Adjustment, -5).is_ok());
        assert!(validate_txn(TxnType::Adjustment, 0).is_err());
        assert!(validate_txn(TxnType::Redeem, 5).is_err());
    }
}
