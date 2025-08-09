use anyhow::{Context, Result, anyhow, bail};
use bigdecimal::BigDecimal;
use mini_macro::here;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use sui_sdk::rpc_types::BalanceChange;
use sui_types::{TypeTag, base_types::SuiAddress};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransferEvent {
    pub amount: BigDecimal,
    pub token: String,
    pub sender: String,
    pub receiver: String,
    pub timestamp_ms: i64,
}

fn transfer_amount(
    balance_changes: &[BalanceChange],
    receiver: SuiAddress,
    token: TypeTag,
) -> Result<i128> {
    let receiver_changes = balance_changes
        .iter()
        .filter(|c| c.owner.get_owner_address().unwrap() == receiver && c.coin_type == token)
        .collect::<Vec<_>>();
    if receiver_changes.is_empty() {
        bail!("NotFoundOfEmptyChanges")
    }
    if receiver_changes.len() == 1 {
        return Ok(receiver_changes.first().unwrap().amount.abs());
    }
    if receiver_changes.len() == 2 {
        let c_except_sui = receiver_changes
            .iter()
            .filter(|c| c.coin_type != sui_addr())
            .collect::<Vec<_>>();

        if c_except_sui.len() != 1 {
            bail!("Unknown transaction with too many transfer")
        }
        return Ok(c_except_sui.first().unwrap().amount.abs());
    }

    bail!("NotFound")
}

fn sui_addr() -> TypeTag {
    TypeTag::from_str("0x2::sui::SUI").unwrap()
}

fn transfer_token(balance_changes: &[BalanceChange]) -> Result<TypeTag> {
    if balance_changes.len() == 1 {
        return Ok(balance_changes.first().unwrap().coin_type.clone());
    }

    let sui_addr = TypeTag::from_str("0x2::sui::SUI").context(here!())?;
    let neg_change = balance_changes
        .iter()
        .filter(|c| c.amount < 0)
        .collect::<Vec<_>>();
    if neg_change.is_empty() {
        return Err(anyhow!("No negative balance changes found"));
    }
    if neg_change.len() == 1 {
        return Ok(neg_change.first().unwrap().coin_type.clone());
    }
    if neg_change.len() == 2 {
        let neg_except_sui = neg_change
            .iter()
            .filter(|c| c.coin_type != sui_addr)
            .collect::<Vec<_>>();
        if neg_except_sui.len() != 1 {
            return Err(anyhow!("Unknown transaction without sui changes"));
        }
        return Ok(neg_except_sui.first().unwrap().coin_type.clone());
    }
    Err(anyhow!("Unknown transaction with two many token changes"))
}

fn transfer_from(balance_changes: &Vec<BalanceChange>) -> Option<SuiAddress> {
    if balance_changes.len() == 1 {
        return balance_changes.first()?.owner.get_owner_address().ok();
    }

    for c in balance_changes {
        if c.amount.is_negative() {
            return c.owner.get_owner_address().ok();
        }
    }
    None
}

fn transfer_to(balance_changes: &[BalanceChange], transfer_from: SuiAddress) -> Result<SuiAddress> {
    if balance_changes.is_empty() {
        return Err(anyhow!("No balance changes provided"));
    }
    let c_except_from = balance_changes
        .iter()
        .filter(|c| c.owner.get_owner_address().unwrap() != transfer_from)
        .collect::<Vec<_>>();
    if c_except_from.is_empty() {
        return Ok(transfer_from);
    }
    if c_except_from.len() == 1 {
        return Ok(c_except_from
            .first()
            .unwrap()
            .owner
            .get_owner_address()
            .unwrap());
    }

    // let receiver_count: Vec<_> = balance_changes.iter().filter(|c| c.amount > 0).collect();
    if c_except_from.len() >= 2 {
        return Err(anyhow!("Too many receivers"));
    }

    Err(anyhow!("Unknown transfer type!"))
}

pub fn decode_transfer(
    balance_changes: Vec<BalanceChange>,
    user: Option<SuiAddress>,
) -> Result<TransferEvent> {
    let transfer_from = transfer_from(&balance_changes).context(here!())?;

    let transfer_to = match user {
        Some(user) => {
            if user != transfer_from {
                user
            } else {
                transfer_to(&balance_changes, transfer_from).context(here!())?
            }
        }
        None => transfer_to(&balance_changes, transfer_from).context(here!())?,
    };

    let transfer_token = transfer_token(&balance_changes).context(here!())?;
    let amount =
        transfer_amount(&balance_changes, transfer_to, transfer_token.clone()).context(here!())?;

    Ok(TransferEvent {
        amount: BigDecimal::from(amount),
        token: transfer_token.to_string(),
        sender: transfer_from.to_string(),
        receiver: transfer_to.to_string(),
        timestamp_ms: 0,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bigdecimal::BigDecimal;
    use sui_sdk::rpc_types::BalanceChange;
    use sui_types::{TypeTag, base_types::SuiAddress, object::Owner};

    use super::{TransferEvent, decode_transfer};

    #[rustfmt::skip]
    fn user() -> Option<SuiAddress> {
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
            decode_transfer(balance_changes, user()).unwrap(),
            TransferEvent {
                amount: BigDecimal::from(2095504),
                token: "0x2::sui::SUI".to_string(),
                sender: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff".to_string(),
                receiver: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff".to_string(),
                timestamp_ms: 0,
            }
        );
    }

    #[test]
    fn test_decode_self_transfer2() {
        // Merge coins: CRNzhTtGj6R7JS1wWfjq5H6Xy84dQTVroDfg2dnp2nSu
        let balance_changes = r#"
            [
                {
                  "owner": {"AddressOwner": "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff"},
                  "coinType": "0x2::sui::SUI",
                  "amount": "783072"
                }
            ]"#;

        let balance_changes: Vec<BalanceChange> = serde_json::from_str(balance_changes).unwrap();
        assert_eq!(
            decode_transfer(balance_changes, user()).unwrap(),
            TransferEvent {
                sender: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff"
                    .to_string(),
                receiver: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff"
                    .to_string(),
                amount: BigDecimal::from(783072i128),
                token: "0x2::sui::SUI".to_string(),
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
            decode_transfer(balance_changes, user()).unwrap(),
            TransferEvent {
                sender: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff".to_string(),
                receiver: "0xf261e0419966da973b7964a293fc4fe592727df803b4339ee5460f98e9537946".to_string(),
                amount: BigDecimal::from(12004000000000i128),
                token: "0x2::sui::SUI".to_string(),
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
            decode_transfer(balance_changes, user()).unwrap(),
            TransferEvent {
                sender: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff".to_string(),
                receiver: "0xef6bb8190f8caaa2e67ac0d91389777b0a0c6a7d0feddfcbfc72f40343fb522b".to_string(),
                amount: BigDecimal::from(65403000000i128),
                token: "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC".to_string(),
                timestamp_ms: 0,
            }
        )
    }

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
            decode_transfer(balance_changes, user()).unwrap(),
            TransferEvent {
                sender: "0x935029ca5219502a47ac9b69f556ccf6e2198b5e7815cf50f68846f723739cbd"
                    .to_string(),
                receiver: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff"
                    .to_string(),
                amount: BigDecimal::from(155567900000i128),
                token:
                    "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC"
                        .to_string(),
                timestamp_ms: 0,
            }
        );
    }

    #[test]
    fn test_transfer_sui() {
        // 6SXMXiBmc9huz8xEb8BEh8rhGJPpCqy29FpwTioK4DbB
        let balance_changes = r#"
            [
              {
                "owner": {
                  "AddressOwner": "0x0f100cc7b22236c5993a35e2319b1d4405692c57da63f3b5a1c7e0fd3fc891b1"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "5999940000000"
              },
              {
                "owner": {
                  "AddressOwner": "0x3318d970dad175bc346ceed5f3d68ee4fcf73ae967fff6049ea26c5c330281f9"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "19940000000"
              },
              {
                "owner": {
                  "AddressOwner": "0x35c657377f6fdd839829b8c9ac014af19fd4ce4416c67674948b39981f4b8b1b"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "19940000000"
              },
              {
                "owner": {
                  "AddressOwner": "0x48ca639682930322648588ab160180fbe1a5a1b8fc6a62e5ce1cb2bf19734320"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "99840000000"
              },
              {
                "owner": {
                  "AddressOwner": "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "34939940000000"
              },
              {
                "owner": {
                  "AddressOwner": "0x935029ca5219502a47ac9b69f556ccf6e2198b5e7815cf50f68846f723739cbd"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "-41350639926640"
              },
              {
                "owner": {
                  "AddressOwner": "0x959985d4b222b03cdea3dd1a08b7cb8c0dddbf00aa19ab0b02ce6c9d2f605144"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "19940000000"
              },
              {
                "owner": {
                  "AddressOwner": "0x96833e1f40abe53185fc811788ed2b2190ee57a5aeb09c6b3db0483277be38b9"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "19940000000"
              },
              {
                "owner": {
                  "AddressOwner": "0xa4245ac2ba7cb62c4b24d42341f4a0a1eb206e2578da782926c71ba962963cd0"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "81940000000"
              },
              {
                "owner": {
                  "AddressOwner": "0xa5bfc83be51eed42fcccbae13c1659e9d93f0f92447c63f8c7b77e23bfbecd0c"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "19940000000"
              },
              {
                "owner": {
                  "AddressOwner": "0xce394257b0e4cb0efc996257a8478efaef45480c04a9362af5d4aa5c4e7c0335"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "109330500000"
              },
              {
                "owner": {
                  "AddressOwner": "0xfa78c7d7a40e918d9ad1ca0728c6521f32a283deddb02c483f9f4bb697fc4f97"
                },
                "coinType": "0x2::sui::SUI",
                "amount": "19940000000"
              }
            ]"#;

        let balance_changes: Vec<BalanceChange> = serde_json::from_str(balance_changes).unwrap();
        assert_eq!(
            decode_transfer(balance_changes, user()).unwrap(),
            TransferEvent {
                sender: "0x935029ca5219502a47ac9b69f556ccf6e2198b5e7815cf50f68846f723739cbd"
                    .to_string(),
                receiver: "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff"
                    .to_string(),
                amount: BigDecimal::from(34939940000000i128),
                token: "0x2::sui::SUI".to_string(),
                timestamp_ms: 0,
            }
        );
    }
}
