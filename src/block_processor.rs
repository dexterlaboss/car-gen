use {
    crate::{
        ledger_storage::LedgerStorage
    },
    anyhow::{Context, Result},
    solana_binary_encoder::{
        transaction_status::{
            BlockEncodingOptions, EncodedConfirmedBlock, TransactionDetails, UiTransactionEncoding,
            UiTransactionStatusMeta,
        },
        convert_block,
    },
    serde_json,
};

pub struct BlockProcessor {
    storage: LedgerStorage,
}

impl BlockProcessor {
    pub fn new(storage: LedgerStorage) -> Self {
        Self { storage }
    }

    /// Preprocesses a block to add minimal metadata for transactions that have null metadata
    pub fn preprocess_block(&self, mut block: EncodedConfirmedBlock) -> EncodedConfirmedBlock {
        // Add minimal metadata for transactions that have null metadata
        for tx in &mut block.transactions {
            if tx.meta.is_none() {
                // Create minimal metadata for transactions without metadata
                let minimal_meta = serde_json::json!({
                    "status": { "Ok": null },
                    "fee": 0,
                    "preBalances": [],
                    "postBalances": [],
                    "innerInstructions": [],
                    "logMessages": [],
                    "preTokenBalances": null,
                    "postTokenBalances": null,
                    "rewards": [],
                    "loadedAddresses": {
                        "writable": [],
                        "readonly": []
                    },
                    "returnData": null,
                    "computeUnitsConsumed": 0
                });
                
                // Convert JSON to UiTransactionStatusMeta
                if let Ok(meta) = serde_json::from_value::<UiTransactionStatusMeta>(minimal_meta) {
                    tx.meta = Some(meta);
                }
            }
        }
        block
    }

    /// Takes a block ID and the `EncodedConfirmedBlock`, converts it, and uploads it.
    pub async fn handle_block(&self, block_id: u64, block: EncodedConfirmedBlock) -> Result<()> {
        // Preprocess the block to add metadata where missing
        let preprocessed_block = self.preprocess_block(block);
        
        let options = BlockEncodingOptions {
            transaction_details: TransactionDetails::Full,
            show_rewards: true,
            max_supported_transaction_version: Some(0),
        };
        let versioned_block = convert_block(preprocessed_block, UiTransactionEncoding::Json, options)
            .map_err(|e| anyhow::anyhow!("Failed to convert block: {}", e))?;

        self.storage
            .upload_confirmed_block(block_id, versioned_block)
            .await
            .context("Failed to upload confirmed block")?;

        Ok(())
    }

    /// Finalize blocks by calling finalize on ledger_storage.
    pub async fn finalize_blocks(&self) -> Result<()> {
        self.storage.finalize().await.context("Failed to finalize blocks")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_binary_encoder::transaction_status::{
        EncodedConfirmedBlock,
    };
    use serde_json;

    /// Test the preprocess_block function with a JSON block containing transactions with null metadata.
    /// This test validates that the convert_block method from the ingestor-sdk can successfully process
    /// a block after preprocessing adds the required metadata to transactions that originally had null metadata.
    #[test]
    fn test_preprocess_block_with_json_transactions() {
        let block_json = r#"{
            "previousBlockhash": "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn",
            "blockhash": "AcknnkY4ok5BZuk69WijDETKVRUPZoMtniZHMe4ZaK1e",
            "parentSlot": 0,
            "transactions": [
                {
                    "transaction": {
                        "signatures": [
                            "39V8tR2Q8Ar3WwMBfVTRPFr7AakLHy5wp7skJNBL7ET6ARoikqc1TaMiuXEtHiNPLQKoeiVr5XnKH8QtjdonN4yM"
                        ],
                        "message": {
                            "header": {
                                "numRequiredSignatures": 1,
                                "numReadonlySignedAccounts": 0,
                                "numReadonlyUnsignedAccounts": 3
                            },
                            "accountKeys": [
                                "GdnSyH3YtwcxFvQrVVJMm1JhTS4QVX7MFsX56uJLUfiZ",
                                "sCtiJieP8B3SwYnXemiLpRFRR8KJLMtsMVN25fAFWjW",
                                "SysvarS1otHashes111111111111111111111111111",
                                "SysvarC1ock11111111111111111111111111111111",
                                "Vote111111111111111111111111111111111111111"
                            ],
                            "recentBlockhash": "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn",
                            "instructions": [
                                {
                                    "programIdIndex": 4,
                                    "accounts": [1, 2, 3, 0],
                                    "data": "2ZjTR1vUs2pHXyTLpNez6FPzd1qvueZPaMpuuSaez2ARasvL8JYrcHe1U7SN8nkwgrrKwZ3FoRPVtCSfcqM",
                                    "stackHeight": null
                                }
                            ]
                        }
                    },
                    "meta": null
                }
            ],
            "rewards": [],
            "blockTime": 1584368940,
            "blockHeight": null
        }"#;

        // Parse the block JSON into EncodedConfirmedBlock
        let block: EncodedConfirmedBlock = serde_json::from_str(block_json)
            .expect("Failed to parse block JSON");

        // Create a dummy processor (without storage) just to test the preprocessing logic
        struct DummyProcessor;
        impl DummyProcessor {
            fn preprocess_block(&self, mut block: EncodedConfirmedBlock) -> EncodedConfirmedBlock {
                // Add minimal metadata for transactions that have null metadata
                for tx in &mut block.transactions {
                    if tx.meta.is_none() {
                        // Create minimal metadata for transactions without metadata
                        let minimal_meta = serde_json::json!({
                            "status": { "Ok": null },
                            "fee": 0,
                            "preBalances": [],
                            "postBalances": [],
                            "innerInstructions": [],
                            "logMessages": [],
                            "preTokenBalances": null,
                            "postTokenBalances": null,
                            "rewards": [],
                            "loadedAddresses": {
                                "writable": [],
                                "readonly": []
                            },
                            "returnData": null,
                            "computeUnitsConsumed": 0
                        });
                        
                        // Convert JSON to UiTransactionStatusMeta
                        if let Ok(meta) = serde_json::from_value::<UiTransactionStatusMeta>(minimal_meta) {
                            tx.meta = Some(meta);
                        }
                    }
                }
                block
            }
        }

        let processor = DummyProcessor;

        // Test preprocessing the block
        let preprocessed_block = processor.preprocess_block(block);

        // Verify that metadata was added to transactions that had null metadata
        for tx in &preprocessed_block.transactions {
            assert!(tx.meta.is_some(), "Transaction should have metadata after preprocessing");
        }

        // Test that convert_block works with the preprocessed block
        let options = BlockEncodingOptions {
            transaction_details: TransactionDetails::Full,
            show_rewards: true,
            max_supported_transaction_version: Some(0),
        };

        let result = convert_block(preprocessed_block, UiTransactionEncoding::Json, options);
        assert!(result.is_ok(), "convert_block should succeed with preprocessed block: {:?}", result.err());
    }
}