use anyhow::{Context, Result};
use bigdecimal::{BigDecimal, Zero};
use mini_macro::here;
use serde::{Deserialize, Serialize};
use std::io::prelude::*;
use std::{collections::HashMap, fs::OpenOptions, sync::Arc};
use sui_sdk::{
    SuiClient, SuiClientBuilder,
    rpc_types::{
        BalanceChange, SuiTransactionBlockResponseOptions, SuiTransactionBlockResponseQuery,
        TransactionFilter,
    },
    types::digests::TransactionDigest,
};
use sui_types::TypeTag;
use sui_types::base_types::SuiAddress;

use crate::Fetcher;

pub async fn get_latest_txs(fetcher: &Fetcher, filter: TransactionFilter) -> Result<()> {
    let options = SuiTransactionBlockResponseOptions::new()
        .with_events()
        .with_balance_changes()
        .with_effects();
    let query = SuiTransactionBlockResponseQuery::new(Some(filter), Some(options));

    let txs = fetcher
        .sui_client
        .read_api()
        .query_transaction_blocks(query.clone(), None, None, true)
        .await
        .context(here!())?;

    // let mut out = vec![];

    for tx in txs.data {
        if let Some(status) = tx.status_ok() {
            if !status {
                continue;
            }
        }

        let Some(events) = tx.events else {
            unreachable!()
        };

        if events.data.is_empty() {
            if let Some(balance_change) = tx.balance_changes {
                if balance_change.len() != 2 && balance_change.len() != 3 {
                    tracing::info!(
                        "### unknown tx type: {}, {}",
                        tx.digest,
                        balance_change.len()
                    );
                }
                let transfer_event = decode_transfer(balance_change).unwrap();

                // let warp = WarpEvents::new(
                //     transfer_event,
                //     tx.timestamp_ms.unwrap_or_default(),
                //     tx.digest,
                //     "Transer",
                // )
                // .await;
                // out.push(MonitorEvent::Transfer(warp));
            };
        }

        for event in events.data {
            // match event.type_.to_string().as_ref() {
            //     "0x4a35d3dfef55ed3631b7158544c6322a23bc434fe4fca1234cb680ce0505f82d::pool::CollectRewardEvent" =>
            //     {
            //         let event: CollectRewardEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();

            //         let warp = WarpEvents::new(
            //             event,
            //             tx.timestamp_ms.unwrap_or_default(),
            //             tx.digest,
            //             "CollectRewardEvent",
            //         )
            //         .await;
            //         out.push(MonitorEvent::CollectReward(warp));
            //     }
            //     "0x4a35d3dfef55ed3631b7158544c6322a23bc434fe4fca1234cb680ce0505f82d::pool::RemoveLiquidityEvent" =>
            //     {
            //         let event: RemoveLiquidityEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //         let warp = WarpEvents::new(
            //             event,
            //             tx.timestamp_ms.unwrap_or_default(),
            //             tx.digest,
            //             "RemoveLiquidityEvent",
            //         )
            //         .await;
            //         out.push(MonitorEvent::RemoveLiquidity(warp));
            //     }
            //     "0x4a35d3dfef55ed3631b7158544c6322a23bc434fe4fca1234cb680ce0505f82d::pool::CollectFeeEvent" =>
            //     {
            //         let event: CollectFeeEvent = serde_json::from_value(event.parsed_json).unwrap();
            //         let warp = WarpEvents::new(
            //             event,
            //             tx.timestamp_ms.unwrap_or_default(),
            //             tx.digest,
            //             "CollectFeeEvent",
            //         )
            //         .await;
            //         out.push(MonitorEvent::CollectFee(warp));
            //     }
            //     "0x4a35d3dfef55ed3631b7158544c6322a23bc434fe4fca1234cb680ce0505f82d::pool::ClosePositionEvent" =>
            //     {
            //         let event: ClosePositionEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //         let warp = WarpEvents::new(
            //             event,
            //             tx.timestamp_ms.unwrap_or_default(),
            //             tx.digest,
            //             "ClosePositionEvent",
            //         )
            //         .await;
            //         out.push(MonitorEvent::ClosePosition(warp));
            //         // log_event(&serde_json::to_string(&warp).unwrap(), LOG_FILE_NAME).unwrap();
            //     }
            //     "0x4a35d3dfef55ed3631b7158544c6322a23bc434fe4fca1234cb680ce0505f82d::pool::OpenPositionEvent" =>
            //     {
            //         let event: OpenPositionEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //         let warp = WarpEvents::new(
            //             event,
            //             tx.timestamp_ms.unwrap_or_default(),
            //             tx.digest,
            //             "OpenPositionEvent",
            //         )
            //         .await;
            //         out.push(MonitorEvent::OpenPosition(warp));
            //     }
            //     "0x4a35d3dfef55ed3631b7158544c6322a23bc434fe4fca1234cb680ce0505f82d::pool::AddLiquidityEvent" =>
            //     {
            //         let event: AddLiquidityEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //         let warp = WarpEvents::new(
            //             event,
            //             tx.timestamp_ms.unwrap_or_default(),
            //             tx.digest,
            //             "AddLiquidityEvent",
            //         )
            //         .await;
            //         out.push(MonitorEvent::AddLiquidity(warp));
            //     }
            //     "0x4a35d3dfef55ed3631b7158544c6322a23bc434fe4fca1234cb680ce0505f82d::pool::SwapEvent" =>
            //     {
            //         let event: SwapEvent = serde_json::from_value(event.parsed_json).unwrap();
            //         let warp = WarpEvents::new(
            //             event,
            //             tx.timestamp_ms.unwrap_or_default(),
            //             tx.digest,
            //             "SwapEvent",
            //         )
            //         .await;
            //         out.push(MonitorEvent::Swap(warp));
            //     }
            //     "0x4a35d3dfef55ed3631b7158544c6322a23bc434fe4fca1234cb680ce0505f82d::partner::ReceiveRefFeeEvent" =>
            //     {
            //         let event: ReceiveRefFeeEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //         let warp = WarpEvents::new(
            //             event,
            //             tx.timestamp_ms.unwrap_or_default(),
            //             tx.digest,
            //             "ReceiveRefFeeEvent",
            //         )
            //         .await;
            //         out.push(MonitorEvent::ReceiveRefFee(warp));
            //     }
            //     "0x70285592c97965e811e0c6f98dccc3a9c2b4ad854b3594faab9597ada267b860::liquidity::OpenPositionEvent" =>
            //     {
            //         let _event: MMTOpenPositionEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //     }
            //     "0x70285592c97965e811e0c6f98dccc3a9c2b4ad854b3594faab9597ada267b860::liquidity::AddLiquidityEvent" =>
            //     {
            //         let _event: MMTAddLiquidityEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //     }
            //     "0x70285592c97965e811e0c6f98dccc3a9c2b4ad854b3594faab9597ada267b860::liquidity::RemoveLiquidityEvent" =>
            //     {
            //         let _event: MMTRemoveLiquidityEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //     }
            //     "0x70285592c97965e811e0c6f98dccc3a9c2b4ad854b3594faab9597ada267b860::collect::CollectPoolRewardEvent" =>
            //     {
            //         let _event: MMTCollectPoolRewardEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //     }
            //     "0x70285592c97965e811e0c6f98dccc3a9c2b4ad854b3594faab9597ada267b860::collect::FeeCollectedEvent" =>
            //     {
            //         let _event: MMTFeeCollectedEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //     }

            //     "0x70285592c97965e811e0c6f98dccc3a9c2b4ad854b3594faab9597ada267b860::liquidity::ClosePositionEvent" =>
            //     {
            //         let _event: MMTClosePositionEvent =
            //             serde_json::from_value(event.parsed_json).unwrap();
            //     }
            //     _ => {
            //         unreachable!("{}, {:?}", event.type_.to_string(), event.id)
            //     }
            // }
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransferEvent {
    pub amount: BigDecimal,
    pub token: TypeTag,
    pub sender: String,
    pub receiver: String,
    pub timestamp_ms: i64,
}

// impl TransferEvent {
//     fn log(&self) -> String {
//         format!(
//             "{},{},{},{},{}",
//             self.sender, self.receiver, self.amount, self.token, self.fee
//         )
//     }
// }

fn decode_transfer(balance_change: Vec<BalanceChange>) -> Result<TransferEvent> {
    // TODO: balance_change maybe equals to 1, which is self_transfer
    assert!(balance_change.len() >= 1 && balance_change.len() <= 3);

    if balance_change.len() == 1 {
        let balance_change = balance_change.first().unwrap();
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

    if balance_change.len() == 2 {
        // transfer sui
        let mut send_changes: Vec<_> = balance_change.iter().filter(|b| b.amount < 0).collect();
        let mut receive_change: Vec<_> = balance_change.iter().filter(|b| b.amount > 0).collect();
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
        for c in &balance_change {
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

        for c in &balance_change {
            if c.coin_type == send_token {
                amount = BigDecimal::from(c.amount.abs());
                if c.amount.is_negative() {
                    token = c.coin_type.clone();
                    receiver = c.owner.get_owner_address().unwrap().to_string();
                } else {
                    token = c.coin_type.clone();
                    sender = c.owner.get_owner_address().unwrap().to_string();
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
    use sui_sdk::rpc_types::{BalanceChange, SuiActiveJwk};
    use sui_types::{TypeTag, base_types::SuiAddress, object::Owner};

    use crate::fetcher::{TransferEvent, decode_transfer};

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
