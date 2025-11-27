use {
    anyhow::{Context, Result},
    serde_json::Value,
    // solana_block_decoder::transaction_status::EncodedConfirmedBlock,
    // solana_transaction_status::EncodedConfirmedBlock,
    solana_block_decoder::block::encoded_block::EncodedConfirmedBlock,
};

pub trait FormatParser: Send + Sync {
    /// Parse a single record (line) into `(block_id, EncodedConfirmedBlock)` or `None` if invalid.
    fn parse_record(&self, record: &str) -> Result<Option<(u64, EncodedConfirmedBlock)>>;
}

pub struct NdJsonParser;

impl FormatParser for NdJsonParser {
    fn parse_record(&self, record: &str) -> Result<Option<(u64, EncodedConfirmedBlock)>> {
        let trimmed = record.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        // We assume a "blockID" field exists in the JSON.
        let value: Value = serde_json::from_str(trimmed)
            .with_context(|| format!("Failed to parse JSON line: {}", trimmed))?;

        let block_id = if let Some(id) = value["blockID"].as_u64() {
            id
        } else if let Some(id_str) = value["blockID"].as_str() {
            id_str
                .parse::<u64>()
                .context("Failed to parse blockID string as u64")?
        } else {
            return Err(anyhow::anyhow!("blockID missing or invalid in JSON line"));
        };

        let block: EncodedConfirmedBlock =
            serde_json::from_value(value).context("Failed to parse EncodedConfirmedBlock")?;

        Ok(Some((block_id, block)))
    }
}
