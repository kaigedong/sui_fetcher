pub mod errors;
pub mod fetcher;
pub mod transfer;

use crate::transfer::TransferEvent;

pub enum TxType {
    Transfer(TransferEvent),
    SelfTransfer(TransferEvent),
    Swap(Swap),
    Unknown,
}

pub struct Swap {
    pub pool: String,
    pub dex: Dex,
    pub a2b: bool,
    pub in_amount: i128,
    pub out_amount: i128,
    pub in_token: String,
    pub out_token: String,
}

pub enum Dex {
    Cetus,
    Magma,
    Bluefin,
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    use super::fetcher::Fetcher;

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

        // tracinglog
        let fetcher = Fetcher::new_mainnet(
            "0x62310ee294108c13f3496ce6895f12f3c2cf3994c74c2911501535e23ccc74ff",
            false,
        )
        .await
        .unwrap();

        fetcher.fetch_txs(true).await.unwrap();
    }
}
