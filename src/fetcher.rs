use anyhow::{Context, Result, bail};
use bigdecimal::BigDecimal;
use futures::{future, stream::StreamExt};
use mini_macro::here;
use std::str::FromStr;
use sui_sdk::{
    SuiClient, SuiClientBuilder,
    rpc_types::{
        SuiTransactionBlockEffects, SuiTransactionBlockEffectsAPI, SuiTransactionBlockResponse,
        SuiTransactionBlockResponseOptions, SuiTransactionBlockResponseQuery, TransactionFilter,
    },
};
use sui_types::base_types::SuiAddress;

use crate::{Dex, Swap, TxType, errors::DecodeError, transfer};

pub struct Fetcher {
    sui_client: SuiClient,
    who: SuiAddress,
    old_first: bool,
}

impl Fetcher {
    pub async fn new_mainnet(who: &str, old_first: bool) -> Result<Self> {
        let sui_client = SuiClientBuilder::default().build_mainnet().await?;

        Ok(Self {
            sui_client,
            who: SuiAddress::from_str(who).context(here!())?,
            old_first,
        })
    }

    pub async fn fetch_txs(self, by_from: bool) -> Result<()> {
        let filter = if by_from {
            TransactionFilter::FromAddress(self.who)
        } else {
            TransactionFilter::ToAddress(self.who)
        };

        let options = SuiTransactionBlockResponseOptions::default()
            .with_effects()
            .with_events()
            .with_balance_changes();
        let filter = SuiTransactionBlockResponseQuery::new(Some(filter), Some(options));

        let descending_order = if self.old_first { false } else { true };
        let txs =
            self.sui_client
                .read_api()
                .get_transactions_stream(filter, None, descending_order);

        txs.for_each(|tx_resp| {
            // println!("{:?}", tx_resp);
            Self::log_sui_tx_resp(tx_resp);
            future::ready(())
        })
        .await;

        // for event in events.pull_next() {}
        Ok(())
    }

    fn is_err(tx_resp: &SuiTransactionBlockResponse) -> Result<bool> {
        let res = tx_resp
            .effects
            .as_ref()
            .map(|e| e.status().is_err())
            .ok_or(DecodeError::TransactionResponseWithoutEffects)?;
        Ok(res)
    }

    fn log_sui_tx_resp(tx_resp: SuiTransactionBlockResponse) {
        if Self::is_err(&tx_resp).unwrap() {
            return;
        }

        Self::decode_tx_type(tx_resp.clone()).unwrap();
    }

    // TODO: 允许用户自己注册解码代码！
    fn decode_tx_type(tx_resp: SuiTransactionBlockResponse) -> Result<TxType> {
        tracing::info!("{}", tx_resp.timestamp_ms.unwrap());

        let events = tx_resp.events.as_ref().unwrap();
        if events.data.is_empty() {
            let balance_changes = tx_resp.balance_changes.unwrap();
            let transfer_event = transfer::decode_transfer(balance_changes).context(here!())?;

            println!("#### transfer...");

            if transfer_event.sender.eq(&transfer_event.receiver) {
                return Ok(TxType::SelfTransfer(transfer_event));
            }
            return Ok(TxType::Transfer(transfer_event));
        }

        for event in &events.data {
            if event.type_.to_string()
                == "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::SwapEvent"
            {
                println!("CetusSwap");

                let balance_changes = tx_resp.balance_changes.as_ref().unwrap();

                assert!(
                    balance_changes.len() == 2,
                    "{}",
                    serde_json::to_string(&tx_resp).unwrap()
                );
                let asset1_amount = balance_changes[0].amount;
                let asset2_amount = balance_changes[1].amount;
                let (in_token, out_token) = if asset1_amount > asset2_amount {
                    (
                        balance_changes[1].coin_type.to_string(),
                        balance_changes[0].coin_type.to_string(),
                    )
                } else {
                    (
                        balance_changes[0].coin_type.to_string(),
                        balance_changes[1].coin_type.to_string(),
                    )
                };

                let _e = &event.parsed_json;
                return Ok(TxType::Swap(Swap {
                    pool: _e.get("pool").unwrap().as_str().unwrap().to_string(),
                    dex: Dex::Cetus,
                    a2b: event.parsed_json.get("atob").unwrap().as_bool().unwrap(),
                    in_amount: _e
                        .get("amount_in")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .parse::<i128>()
                        .unwrap(),
                    out_amount: _e
                        .get("amount_out")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .parse::<i128>()?,
                    in_token,
                    out_token,
                }));
            } else if event.type_.to_string()
                == "0x3492c874c1e3b3e2984e8c41b589e642d4d0a5d6459e5a9cfc2d52fd7c89c267::events::AssetSwap"
            {
                println!("BluefinSwap");

                let balance_changes = tx_resp.balance_changes.as_ref().unwrap();
                assert!(
                    balance_changes.len() == 2,
                    "{}",
                    serde_json::to_string(&tx_resp).unwrap()
                );
                let asset1_amount = balance_changes[0].amount;
                let asset2_amount = balance_changes[1].amount;
                let (in_token, out_token) = if asset1_amount > asset2_amount {
                    (
                        balance_changes[1].coin_type.to_string(),
                        balance_changes[0].coin_type.to_string(),
                    )
                } else {
                    (
                        balance_changes[0].coin_type.to_string(),
                        balance_changes[1].coin_type.to_string(),
                    )
                };

                let _e = &event.parsed_json;
                return Ok(TxType::Swap(Swap {
                    pool: _e.get("pool_id").unwrap().as_str().unwrap().to_string(),
                    dex: Dex::Bluefin,
                    a2b: event.parsed_json.get("a2b").unwrap().as_bool().unwrap(),
                    in_amount: _e
                        .get("amount_in")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .parse::<i128>()
                        .unwrap(),
                    out_amount: _e
                        .get("amount_out")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .parse::<i128>()?,
                    in_token,
                    out_token,
                }));
            } else {
                println!("### txs: {}", serde_json::to_string(&tx_resp).unwrap());
            }
        }

        println!("### txs: {}", serde_json::to_string(&tx_resp).unwrap());

        bail!("Unknown tx type")
    }

    pub fn tx_gas(effect: SuiTransactionBlockEffects) -> BigDecimal {
        let fee = effect.gas_cost_summary();
        let fee = fee.computation_cost + fee.storage_cost - fee.storage_rebate;
        BigDecimal::from(fee) / 10u128.pow(9)
    }
}
