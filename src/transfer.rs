use anyhow::{Context, Result, anyhow, bail};
use bigdecimal::{BigDecimal, Zero};
use itertools::Itertools;
use mini_macro::here;
use move_core_types::language_storage::StructTag;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
use sui_sdk::rpc_types::BalanceChange;
use sui_types::{TypeTag, balance_change, base_types::SuiAddress};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransferEvent {
    pub amount: BigDecimal,
    pub token: TypeTag,
    pub sender: String,
    pub receiver: String,
    pub timestamp_ms: i64,
}

fn transfer_amount(
    balance_changes: &Vec<BalanceChange>,
    receiver: SuiAddress,
    token: TypeTag,
) -> Option<i128> {
    // balance_changes.iter().filter(|c| c.)
    for c in balance_changes {
        if c.owner.get_owner_address().ok()? == receiver && c.amount > 0 && c.coin_type == token {
            return Some(c.amount);
        }
    }
    None
}

fn transfer_token(balance_changes: &Vec<BalanceChange>) -> Result<TypeTag> {
    let sui_addr = TypeTag::from_str("0x2::sui::SUI").context(here!())?;
    let neg_change: Vec<_> = balance_changes
        .iter()
        .filter(|c| c.amount < 0 && c.coin_type != sui_addr)
        .map(|c| c.coin_type.clone())
        .collect();
    if neg_change.len() == 0 {
        return Err(anyhow!("No negative balance changes found"));
    }
    if neg_change.len() >= 2 {
        return Err(anyhow!("Too many negative balance changes"));
    }
    let Some(token) = neg_change.first().cloned() else {
        return Ok(sui_addr);
    };
    Ok(token)
}

fn transfer_from(balance_changes: &Vec<BalanceChange>) -> Option<SuiAddress> {
    for c in balance_changes {
        if c.amount.is_negative() {
            return c.owner.get_owner_address().ok();
        }
    }
    return None;
}

fn transfer_to(balance_changes: &Vec<BalanceChange>) -> Result<SuiAddress> {
    if balance_changes.len() == 0 {
        return Err(anyhow!("No balance changes provided"));
    }
    let receiver_count: Vec<_> = balance_changes.iter().filter(|c| c.amount > 0).collect();
    if receiver_count.len() >= 2 {
        return Err(anyhow!("Too many receivers"));
    }

    if balance_changes.len() == 1 {
        let res = balance_changes
            .first()
            .map(|c| c.owner.get_owner_address())
            .context(here!())?;
        return res.context(here!());
    }
    if balance_changes.len() == 2 || balance_changes.len() == 3 {
        let res = balance_changes
            .iter()
            .filter(|c| c.amount > 0)
            .next()
            .map(|c| c.owner.get_owner_address())
            .context(here!())?;
        return res.context(here!());
    }

    Err(anyhow!("Unknown transfer type!"))
}

pub fn decode_transfer(
    balance_changes: Vec<BalanceChange>,
    receiver: Option<SuiAddress>,
) -> Result<TransferEvent> {
    let transfer_from = transfer_from(&balance_changes).context(here!())?;
    let transfer_to = if let Some(_transfer_to) = receiver {
        _transfer_to
    } else {
        transfer_to(&balance_changes).context(here!())?
    };
    let transfer_token = transfer_token(&balance_changes).context(here!())?;
    let amount =
        transfer_amount(&balance_changes, transfer_to, transfer_token.clone()).context(here!())?;

    return Ok(TransferEvent {
        amount: BigDecimal::from(amount),
        token: transfer_token,
        sender: transfer_from.to_string(),
        receiver: transfer_to.to_string(),
        timestamp_ms: 0,
    });

    // if balance_changes.len() == 1 {
    //     let balance_change = balance_changes.first().unwrap();
    //     return Ok(TransferEvent {
    //         amount: BigDecimal::from(balance_change.amount).abs(),
    //         token: transfer_token,
    //         sender: transfer_from.to_string(),
    //         receiver: transfer_to.to_string(),
    //         timestamp_ms: 0,
    //     });
    // }

    // if balance_changes.len() == 2 {
    //     // transfer sui
    //     let mut send_changes: Vec<_> = balance_changes.iter().filter(|b| b.amount < 0).collect();
    //     let mut receive_change: Vec<_> = balance_changes.iter().filter(|b| b.amount > 0).collect();
    //     let send_changes = send_changes.pop().unwrap();
    //     let receive_change = receive_change.pop().unwrap();
    //     assert_eq!(send_changes.coin_type, receive_change.coin_type);

    //     Ok(TransferEvent {
    //         amount: BigDecimal::from(receive_change.amount),
    //         token: transfer_token,
    //         sender: transfer_from.to_string(),
    //         receiver: transfer_to.to_string(),
    //         timestamp_ms: 0,
    //     })
    // } else if balance_changes.len() == 3 {
    //     let mut coin_count = HashMap::new();
    //     for c in &balance_changes {
    //         coin_count
    //             .entry(c.coin_type.clone())
    //             .and_modify(|v| *v += 1)
    //             .or_insert(1);
    //     }
    //     if coin_count.len() != 2 {
    //         bail!("Unknown coin_count: {}", coin_count.len())
    //     }

    //     let mut send_token = TypeTag::Bool;
    //     for coin in &coin_count {
    //         if *coin.1 == 2 {
    //             send_token = coin.0.clone();
    //         }
    //     }

    //     let mut amount = BigDecimal::zero();
    //     let mut sender = String::new();
    //     let mut receiver = String::new();
    //     let mut token = TypeTag::U128;

    //     for c in &balance_changes {
    //         if c.coin_type == send_token {
    //             amount = BigDecimal::from(c.amount.abs());
    //             if c.amount.is_negative() {
    //                 token = c.coin_type.clone();
    //                 sender = c.owner.get_owner_address().unwrap().to_string();
    //             } else {
    //                 token = c.coin_type.clone();
    //                 receiver = c.owner.get_owner_address().unwrap().to_string();
    //             }
    //         }
    //     }
    //     Ok(TransferEvent {
    //         amount,
    //         token,
    //         sender,
    //         receiver,
    //         timestamp_ms: 0,
    //     })
    // } else {
    //     todo!()
    // }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bigdecimal::BigDecimal;
    use sui_sdk::rpc_types::BalanceChange;
    use sui_types::{TypeTag, balance_change, base_types::SuiAddress, object::Owner};

    use super::{TransferEvent, decode_transfer};

    #[rustfmt::skip]
    fn receiver() -> Option<SuiAddress> {
        Some(SuiAddress::from_str("0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff").unwrap())
    }

    #[test]
    #[rustfmt::skip]
    fn test_decode_self_transfer() {
        let balance_changes = vec![BalanceChange {
            owner: Owner::AddressOwner(SuiAddress::from_str("0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff",).unwrap()),
            coin_type: TypeTag::from_str("0x2::sui::SUI").unwrap(),
            amount: "-2095504".parse::<i128>().unwrap(),
        }];

        assert_eq!(
            decode_transfer(balance_changes, receiver()).unwrap(),
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
            decode_transfer(balance_changes, receiver()).unwrap(),
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
            decode_transfer(balance_changes, receiver()).unwrap(),
            TransferEvent {
                sender: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff".to_string(),
                receiver: "0xef6bb8190f8caaa2e67ac0d91389777b0a0c6a7d0feddfcbfc72f40343fb522b".to_string(),
                amount: BigDecimal::from(65403000000i128),
                token: TypeTag::from_str("0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC").unwrap(),
                timestamp_ms: 0,
            }
        )
    }

    // FIXME: 处理sui sponser
    #[test]
    fn test_decode_binance_transfer() {
        let balance_changes = r#"
            [{
              "owner": {"AddressOwner": "0x0f100cc7b22236c5993a35e2319b1d4405692c57da63f3b5a1c7e0fd3fc891b1"},
              "coinType": "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC",
              "amount": "15999900000"
            },
            {
              "owner": {"AddressOwner": "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff"},
              "coinType": "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC",
              "amount": "155567900000"
            },
            {
              "owner": {"AddressOwner": "0x87c9e076815e78ee63b7dc225704c428b8c51072ccead4304ae07f6c68fe1b92"},
              "coinType": "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC",
              "amount": "499999900000"
            },
            {
              "owner": {"AddressOwner": "0x935029ca5219502a47ac9b69f556ccf6e2198b5e7815cf50f68846f723739cbd"},
              "coinType": "0x2::sui::SUI",
              "amount": "-4508528"
            },
            {
              "owner": {"AddressOwner": "0x935029ca5219502a47ac9b69f556ccf6e2198b5e7815cf50f68846f723739cbd"},
              "coinType": "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC",
              "amount": "-676167680000"
            },
            {
              "owner": {"AddressOwner": "0xdb2bdf1a5b381884578fbe67f94f424abf146952672b45def30adedea8df0107"},
              "coinType": "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC",
              "amount": "4599980000"
            }]"#;

        let balance_changes: Vec<BalanceChange> = serde_json::from_str(balance_changes).unwrap();
        assert_eq!(
            decode_transfer(balance_changes, receiver()).unwrap(),
            TransferEvent {
                sender: "0x935029ca5219502a47ac9b69f556ccf6e2198b5e7815cf50f68846f723739cbd"
                    .to_string(),
                receiver: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff"
                    .to_string(),
                amount: BigDecimal::from(155567900000i128),
                token: TypeTag::from_str(
                    "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC"
                )
                .unwrap(),
                timestamp_ms: 0,
            }
        );
    }
}
