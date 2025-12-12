use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Datelike, TimeZone, Utc};
use hdfs_native::{Client, WriteOptions};
use log::info;
use solana_sdk::clock::Slot;
use std::fmt::{self, Debug};

/// A trait for generating HDFS paths for the CAR file, e.g.,
/// `/base_path/YYYY/MM/DD/blocks_minSlot_maxSlot.car`
pub trait PathGenerationStrategy: Debug + Send + Sync {
    /// Returns the full HDFS path for blocks in [min_slot..max_slot].
    fn generate_path(&self, min_slot: Slot, max_slot: Slot, block_time: u64) -> String;
}

/// Daily-partitioned directory layout implementation:
/// `/base_path/year=YYYY/month=MM/day=DD/{min_slot}-{max_slot}.blocks.car`
#[derive(Debug)]
pub struct DailyPartitionedPathStrategy {
    pub base_path: String,
}

impl PathGenerationStrategy for DailyPartitionedPathStrategy {
    fn generate_path(&self, min_slot: Slot, max_slot: Slot, block_time: u64) -> String {
        let datetime = Utc
            .timestamp_opt(block_time as i64, 0)
            .single()
            .unwrap_or_else(|| Utc::now());
        format!(
            "{}/year={:04}/month={:02}/day={:02}/{}-{}.blocks.car",
            self.base_path,
            datetime.year(),
            datetime.month(),
            datetime.day(),
            min_slot,
            max_slot
        )
    }
}

/// An async trait for writing CAR files to various backends.
#[async_trait]
pub trait CarFileWriter: Debug + Send + Sync {
    /// Asynchronously write the provided `car_bytes` for blocks in [min_slot..max_slot]
    /// and return the final path where the file was written.
    async fn write_car(
        &self,
        car_bytes: &[u8],
        min_slot: Slot,
        max_slot: Slot,
        block_time: u64,
    ) -> Result<String>;
}

/// A writer that uses `hdfs_native::Client` + a `PathGenerationStrategy`.
pub struct HdfsWriter<S: PathGenerationStrategy> {
    client: Client,
    strategy: S,
}

impl<S: PathGenerationStrategy> HdfsWriter<S> {
    /// Create a new `HdfsWriter` with a given HDFS client and path generation strategy.
    pub fn new(client: Client, strategy: S) -> Self {
        HdfsWriter { client, strategy }
    }
}

/// Implement `CarFileWriter` for `HdfsWriter`
#[async_trait]
impl<S: PathGenerationStrategy + Send + Sync> CarFileWriter for HdfsWriter<S> {
    async fn write_car(
        &self,
        car_bytes: &[u8],
        min_slot: Slot,
        max_slot: Slot,
        block_time: u64,
    ) -> Result<String> {
        // Generate the file path using the provided block_time
        let path = self.strategy.generate_path(min_slot, max_slot, block_time);
        info!("HdfsWriter: Writing CAR file to path: {}", path);

        // Create a file on HDFS (overwriting if it exists), using default WriteOptions
        let mut file_writer = self
            .client
            .create(&path, WriteOptions::default().overwrite(true))
            .await?;

        // Write car_bytes in chunks
        let mut offset = 0;
        while offset < car_bytes.len() {
            // Adjust chunk size as desired (here we use 8 MB)
            let chunk_size = std::cmp::min(8 * 1024 * 1024, car_bytes.len() - offset);
            let chunk = Bytes::copy_from_slice(&car_bytes[offset..offset + chunk_size]);

            let written = file_writer.write(chunk).await?;
            offset += written;
        }

        // Explicitly close the file
        file_writer.close().await?;

        // Return the path
        Ok(path)
    }
}

impl<S: PathGenerationStrategy> Debug for HdfsWriter<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HdfsWriter")
            .field("strategy", &self.strategy)
            .finish()
    }
}
