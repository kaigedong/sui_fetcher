use anyhow::{Context, Result, bail};
use bigdecimal::BigDecimal;
use futures::{future, stream::StreamExt};
use mini_macro::here as h;
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

pub struct ActivityFetcher {
    sui_client: SuiClient,
    who: SuiAddress,
    old_first: bool,
    from: Option<i64>,
    to: Option<i64>,
}

impl ActivityFetcher {
    pub async fn new_mainnet(
        who: &str,
        old_first: bool,
        from: Option<i64>,
        to: Option<i64>,
    ) -> Result<Self> {
        let sui_client = SuiClientBuilder::default()
            .build_mainnet()
            .await
            .context(h!())?;

        Ok(Self {
            sui_client,
            who: SuiAddress::from_str(who).context(h!())?,
            old_first,
            from,
            to,
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
            self.log_sui_tx_resp(tx_resp);
            future::ready(())
        })
        .await;

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

    // TODO: 允许用户自己注册解码代码！
    fn log_sui_tx_resp(&self, tx_resp: SuiTransactionBlockResponse) {
        if let Some(from) = self.from {
            if tx_resp.timestamp_ms.unwrap() / 1000 < from as u64 {
                return;
            }
        }
        if let Some(to) = self.to {
            if tx_resp.timestamp_ms.unwrap() / 1000 > to as u64 {
                return;
            }
        }

        if Self::is_err(&tx_resp).unwrap() {
            return;
        }

        match self.decode_tx_type(tx_resp.clone()) {
            Ok(res) => {
                tracing::info!("{}", serde_json::to_string(&res).unwrap())
            }
            Err(e) => tracing::error!(
                "Failed to decode tx. Err: {:?}. context: {}",
                e,
                serde_json::to_string(&tx_resp).unwrap()
            ),
        };
    }

    fn decode_tx_type(&self, tx_resp: SuiTransactionBlockResponse) -> Result<TxType> {
        tracing::info!("{}", tx_resp.timestamp_ms.context(h!())?);

        let events = tx_resp.events.as_ref().context(h!())?;
        if events.data.is_empty() {
            let balance_changes = tx_resp.balance_changes.unwrap();
            let transfer_event =
                transfer::decode_transfer(balance_changes, Some(self.who)).context(h!())?;

            if transfer_event.sender.eq(&transfer_event.receiver) {
                return Ok(TxType::SelfTransfer(transfer_event));
            } else {
                return Ok(TxType::Transfer(transfer_event));
            }
        }

        tracing::debug!("{}", serde_json::to_string(&tx_resp).unwrap());
        for event in &events.data {
            if event.type_.to_string()
                == "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::SwapEvent"
            {
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
                    before_sqrt_price: _e
                        .get("before_sqrt_price")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string(),
                    after_sqrt_price: _e
                        .get("after_sqrt_price")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string(),
                }));
            } else if event.type_.to_string()
                == "0x3492c874c1e3b3e2984e8c41b589e642d4d0a5d6459e5a9cfc2d52fd7c89c267::events::AssetSwap"
            {
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
                    before_sqrt_price: _e
                        .get("before_sqrt_price")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string(),
                    after_sqrt_price: _e
                        .get("after_sqrt_price")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string(),
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
