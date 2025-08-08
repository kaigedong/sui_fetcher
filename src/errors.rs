use thiserror::Error;

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("TooManyAccountsOFTransferEvents")]
    TooManyAccount,
    #[error("TransactionResponseWithoutEffects")]
    TransactionResponseWithoutEffects,
}
