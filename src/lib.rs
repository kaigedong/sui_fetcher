pub mod fetcher;

use anyhow::{Context, Result};
use bigdecimal::{BigDecimal, Zero};
use binance_async::ws_streams::stream_events;
use futures::{
    future,
    stream::{Stream, StreamExt},
};
use mini_macro::here;
use serde::{Deserialize, Serialize};
use std::io::prelude::*;
use std::option;
use std::str::FromStr;
use std::{collections::HashMap, fs::OpenOptions, sync::Arc};
use sui_sdk::{
    SuiClient, SuiClientBuilder,
    rpc_types::{
        BalanceChange, SuiExecutionStatus, SuiTransactionBlockEffects,
        SuiTransactionBlockEffectsAPI, SuiTransactionBlockResponse,
        SuiTransactionBlockResponseOptions, SuiTransactionBlockResponseQuery, TransactionFilter,
    },
    types::digests::TransactionDigest,
};
use sui_types::TypeTag;
use sui_types::base_types::SuiAddress;
use tracing::instrument::WithSubscriber;

pub struct Fetcher {
    sui_client: SuiClient,
    who: SuiAddress,
}

impl Fetcher {
    pub async fn new_mainnet(who: &str) -> Result<Self> {
        println!("### a");
        let sui_client = SuiClientBuilder::default().build_mainnet().await?;

        println!("### b");
        Ok(Self {
            sui_client,
            who: SuiAddress::from_str(who).context(here!())?,
        })
    }

    pub async fn fetch_txs(self) -> Result<()> {
        let tx_filter_from = TransactionFilter::FromAddress(self.who);
        let tx_filter_to = TransactionFilter::ToAddress(self.who);

        let options = SuiTransactionBlockResponseOptions {
            show_input: false,
            show_raw_input: false,
            show_effects: true,
            show_events: true,
            show_object_changes: false,
            show_balance_changes: true,
            show_raw_effects: false,
        };
        let filter = SuiTransactionBlockResponseQuery::new(Some(tx_filter_from), Some(options));

        let txs = self
            .sui_client
            .read_api()
            .get_transactions_stream(filter, None, false); // Old first

        txs.for_each(|tx_resp| {
            // println!("{:?}", tx_resp);
            Self::log_sui_tx_resp(tx_resp);
            future::ready(())
        })
        .await;

        // for event in events.pull_next() {}
        Ok(())
    }

    fn log_sui_tx_resp(tx_resp: SuiTransactionBlockResponse) {
        let effects = &tx_resp.effects.as_ref().unwrap();
        let status = &effects.status();
        if status.is_err() {
            return;
        }

        let events = tx_resp.events.as_ref().unwrap();
        if events.data.is_empty() {
            // 这是transfer交易!
            println!("{}", serde_json::to_string(&tx_resp).unwrap());
        } else {
            // 这是dex交易！
            todo!()
        }

        let balance_changes = tx_resp.balance_changes.unwrap();
    }

    // pub async fn get_swap_events_after(self, after_time_ms: u64) -> Result<()> {
    //     // let user = SuiAddress::from_str(self.).unwrap();

    //     let filter_from = TransactionFilter::FromAddress(self.who);
    //     let filter_to = TransactionFilter::ToAddress(self.who);
    //     let mut from_events = fetcher::get_latest_txs(&self, filter_from)
    //         .await
    //         .context(here!())?;
    //     let to_events = get_txs::get_latest_txs(app.clone(), filter_to)
    //         .await
    //         .context(here!())?;

    //     from_events.extend(to_events);
    //     from_events.sort_by_key(|e| e.event_ms());
    //     from_events.retain(|e| e.event_ms() > after_time_ms);
    //     if let Some(last_event) = from_events.last().cloned() {
    //         from_events.retain(|e| e.event_ms() < last_event.event_ms());
    //     }

    //     // TODO: merge events
    //     let mut merge_events: HashMap<u64, Vec<MonitorEvent>> = HashMap::new();
    //     for event in from_events {
    //         merge_events
    //             .entry(event.event_ms())
    //             .and_modify(|v| v.push(event.clone()))
    //             .or_insert(vec![event]);
    //     }
    //     let merged_events: Vec<_> = merge_events.into_iter().collect();

    //     Ok(merged_events)
    // }

    fn tx_gas(effect: SuiTransactionBlockEffects) -> BigDecimal {
        let fee = effect.gas_cost_summary();
        let fee = fee.computation_cost + fee.storage_cost - fee.storage_rebate;
        BigDecimal::from(fee) / 10u128.pow(9)
    }
}

#[cfg(test)]
mod tests {
    use crate::Fetcher;

    #[tokio::test]
    async fn test_log_sui_tx_resp() {
        let fetcher = Fetcher::new_mainnet(
            "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff",
        )
        .await
        .unwrap();

        fetcher.fetch_txs().await.unwrap();

        // let tx_resp = SuiTransactionBlockResponse::default();
        // log_sui_tx_resp(tx_resp);
    }
}
