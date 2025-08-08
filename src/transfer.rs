use anyhow::Result;
use bigdecimal::{BigDecimal, Zero};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use sui_sdk::rpc_types::BalanceChange;
use sui_types::TypeTag;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransferEvent {
    pub amount: BigDecimal,
    pub token: TypeTag,
    pub sender: String,
    pub receiver: String,
    pub timestamp_ms: i64,
}

pub fn decode_transfer(balance_changes: Vec<BalanceChange>) -> Result<TransferEvent> {
    // TODO: balance_change maybe equals to 1, which is self_transfer
    assert!(balance_changes.len() >= 1 && balance_changes.len() <= 3);

    if balance_changes.len() == 1 {
        let balance_change = balance_changes.first().unwrap();
        return Ok(TransferEvent {
            amount: BigDecimal::from(balance_change.amount).abs(),
            token: balance_change.coin_type.clone(),
            sender: balance_change
                .owner
                .get_owner_address()
                .unwrap()
                .to_string(),
            receiver: balance_change
                .owner
                .get_owner_address()
                .unwrap()
                .to_string(),
            timestamp_ms: 0,
        });
    }

    if balance_changes.len() == 2 {
        // transfer sui
        let mut send_changes: Vec<_> = balance_changes.iter().filter(|b| b.amount < 0).collect();
        let mut receive_change: Vec<_> = balance_changes.iter().filter(|b| b.amount > 0).collect();
        let send_changes = send_changes.pop().unwrap();
        let receive_change = receive_change.pop().unwrap();
        assert_eq!(send_changes.coin_type, receive_change.coin_type);

        Ok(TransferEvent {
            amount: BigDecimal::from(receive_change.amount),
            token: send_changes.coin_type.clone(),
            sender: send_changes.owner.get_owner_address().unwrap().to_string(),
            receiver: receive_change
                .owner
                .get_owner_address()
                .unwrap()
                .to_string(),
            timestamp_ms: 0,
        })
    } else {
        let mut coin_count = HashMap::new();
        for c in &balance_changes {
            coin_count
                .entry(c.coin_type.clone())
                .and_modify(|v| *v += 1)
                .or_insert(1);
        }
        assert!(coin_count.len() == 2);

        let mut send_token = TypeTag::Bool;
        for coin in &coin_count {
            if *coin.1 == 2 {
                send_token = coin.0.clone();
            }
        }

        let mut amount = BigDecimal::zero();
        let mut sender = String::new();
        let mut receiver = String::new();
        let mut token = TypeTag::U128;

        for c in &balance_changes {
            if c.coin_type == send_token {
                amount = BigDecimal::from(c.amount.abs());
                if c.amount.is_negative() {
                    token = c.coin_type.clone();
                    sender = c.owner.get_owner_address().unwrap().to_string();
                } else {
                    token = c.coin_type.clone();
                    receiver = c.owner.get_owner_address().unwrap().to_string();
                }
            }
        }
        Ok(TransferEvent {
            amount,
            token,
            sender,
            receiver,
            timestamp_ms: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bigdecimal::BigDecimal;
    use sui_sdk::rpc_types::BalanceChange;
    use sui_types::{TypeTag, base_types::SuiAddress, object::Owner};

    use super::{TransferEvent, decode_transfer};

    #[test]
    #[rustfmt::skip]
    fn test_decode_self_transfer() {
        let balance_changes = vec![BalanceChange {
            owner: Owner::AddressOwner(SuiAddress::from_str("0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff",).unwrap()),
            coin_type: TypeTag::from_str("0x2::sui::SUI").unwrap(),
            amount: "-2095504".parse::<i128>().unwrap(),
        }];

        assert_eq!(
            decode_transfer(balance_changes).unwrap(),
            TransferEvent {
                amount: BigDecimal::from(2095504),
                token: TypeTag::from_str("0x2::sui::SUI").unwrap(),
                sender: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff".to_string(),
                receiver: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff".to_string(),
                timestamp_ms: 0,
            }
        );
    }

    #[test]
    #[rustfmt::skip]
    fn test_decode_transfer_sui() {
        let balance_changes = vec![
            BalanceChange {
                owner: Owner::AddressOwner(SuiAddress::from_str("0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff").unwrap()),
                coin_type: TypeTag::from_str("0x2::sui::SUI").unwrap(),
                amount: "-12004001747880".parse::<i128>().unwrap(),
            },
            BalanceChange {
                owner: Owner::AddressOwner(SuiAddress::from_str("0xf261e0419966da973b7964a293fc4fe592727df803b4339ee5460f98e9537946").unwrap()),
                coin_type: TypeTag::from_str("0x2::sui::SUI").unwrap(),
                amount: "12004000000000".parse::<i128>().unwrap(),
            },
        ];

        assert_eq!(
            decode_transfer(balance_changes).unwrap(),
            TransferEvent {
                sender: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff".to_string(),
                receiver: "0xf261e0419966da973b7964a293fc4fe592727df803b4339ee5460f98e9537946".to_string(),
                amount: BigDecimal::from(12004000000000i128),
                token: TypeTag::from_str("0x2::sui::SUI").unwrap(),
                timestamp_ms: 0,
            }
        )
    }

    #[test]
    #[rustfmt::skip]
    fn test_decode_transfer_coins() {
        let balance_changes = vec![
            BalanceChange{
                owner: Owner::AddressOwner(SuiAddress::from_str( "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff").unwrap()),
                coin_type: TypeTag::from_str( "0x2::sui::SUI").unwrap(),
                amount: "-773104".parse::<i128>().unwrap(),
            },
            BalanceChange{
                owner: Owner::AddressOwner(SuiAddress::from_str( "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff").unwrap()),
                coin_type:TypeTag::from_str( "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC").unwrap(),
                amount: "-65403000000".parse::<i128>().unwrap(),
            },
            BalanceChange {
                owner: Owner::AddressOwner(SuiAddress::from_str( "0xef6bb8190f8caaa2e67ac0d91389777b0a0c6a7d0feddfcbfc72f40343fb522b").unwrap()),
                coin_type: TypeTag::from_str( "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC").unwrap(),
                amount: "65403000000".parse::<i128>().unwrap(),
            },
        ];

        assert_eq!(
            decode_transfer(balance_changes).unwrap(),
            TransferEvent {
                sender: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff".to_string(),
                receiver: "0xef6bb8190f8caaa2e67ac0d91389777b0a0c6a7d0feddfcbfc72f40343fb522b".to_string(),
                amount: BigDecimal::from(65403000000i128),
                token: TypeTag::from_str("0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC").unwrap(),
                timestamp_ms: 0,
            }
        )
    }
}
