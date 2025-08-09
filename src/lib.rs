pub mod errors;
pub mod fetcher;
pub mod objects;
pub mod transfer;

use serde::{Deserialize, Serialize};
use sui_types::digests::TransactionDigest;

use crate::transfer::TransferEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionKind {
    pub tx_type: TxType,
    pub tx_hash: TransactionDigest,
    pub event_timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxType {
    Transfer(TransferEvent),
    SelfTransfer(TransferEvent),
    Swap(Swap),
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Swap {
    pub pool: String,
    pub dex: Dex,
    pub a2b: bool,
    pub in_amount: i128,
    pub out_amount: i128,
    pub in_token: String,
    pub out_token: String,
    pub before_sqrt_price: String,
    pub after_sqrt_price: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Dex {
    Cetus,
    Magma,
    Bluefin,
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    use super::fetcher::ActivityFetcher;

    #[tokio::test]
    async fn test_log_sui_tx_resp() {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_line_number(true)
                    .with_file(true),
            )
            .with(tracing_subscriber::filter::LevelFilter::INFO)
            .init();

        let fetcher = ActivityFetcher::new_mainnet(
            "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff",
            false,
            Some(1751968800),
            Some(1754647200),
        )
        .await
        .unwrap();

        fetcher.fetch_txs(false).await.unwrap();
    }
}
