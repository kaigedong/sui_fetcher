use anyhow::{Context, Result};
use mini_macro::here;
use sui_sdk::rpc_types::{
    SuiTransactionBlockResponseOptions, SuiTransactionBlockResponseQuery, TransactionFilter,
};

use super::transfer;
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
                let transfer_event = transfer::decode_transfer(balance_change).unwrap();

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
