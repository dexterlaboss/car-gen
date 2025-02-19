// src/ledger_storage.rs

use {
    crate::{
        hbase::{Error as HBaseError, HBaseConnection},
        hdfs_writer::CarFileWriter,
    },
    solana_binary_encoder::{
        compression::{compress, compress_best, CompressionMethod},
        convert::generated,
        transaction_status::VersionedConfirmedBlock,
    },
    log::{debug, info},
    thiserror::Error,
    tokio::task::JoinError,
    std::{
        collections::HashMap,
        mem,
        str::FromStr,
        sync::{
            Arc,
        },
    },
    tokio::sync::Mutex,
    prost::Message,
    dexter_ipfs_car::{
        types::{RowData, RowKey},
        writer::{BlockIndexEntry, InMemoryCarBuilder},
    },
    solana_sdk::clock::Slot,
};

/// Our custom error type
#[derive(Debug, Error)]
pub enum Error {
    #[error("HBase: {0}")]
    HBaseError(HBaseError),

    #[error("I/O error: {0}")]
    IoError(std::io::Error),

    #[error("Tokio join error: {0}")]
    TokioJoinError(JoinError),

    #[error("Protobuf encode error: {0}")]
    EncodingError(prost::EncodeError),

    #[error("HDFS writer is missing")]
    MissingCarFileWriter,
}

impl From<HBaseError> for Error {
    fn from(err: HBaseError) -> Self {
        Self::HBaseError(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Minimal index entry stored in HBase (one row per block in the .car).
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CarIndexEntry {
    #[prost(uint64, tag = "1")]
    pub slot: u64,
    #[prost(string, tag = "2")]
    pub block_hash: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
    #[prost(uint64, tag = "4")]
    pub length: u64,
    #[prost(uint64, tag = "5")]
    pub start_slot: u64,
    #[prost(uint64, tag = "6")]
    pub end_slot: u64,
    #[prost(message, optional, tag = "7")]
    pub timestamp: ::core::option::Option<UnixTimestamp>,
    #[prost(string, tag = "8")]
    pub previous_block_hash: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "9")]
    pub block_height: ::core::option::Option<BlockHeight>,
    #[prost(message, optional, tag = "10")]
    pub block_time: ::core::option::Option<UnixTimestamp>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnixTimestamp {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockHeight {
    #[prost(uint64, tag = "1")]
    pub block_height: u64,
}

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:9090";
pub const BLOCKS_TABLE_NAME: &str = "blocks_meta";

/// Config for ledger storage
#[derive(Debug)]
pub struct LedgerStorageConfig {
    pub address: String,
    pub namespace: Option<String>,
    pub uploader_config: UploaderConfig,

    /// CAR file writer (no fallback)
    pub car_file_writer: Option<Arc<dyn CarFileWriter + Send + Sync>>,
}

impl Default for LedgerStorageConfig {
    fn default() -> Self {
        Self {
            address: DEFAULT_ADDRESS.to_string(),
            namespace: None,
            uploader_config: UploaderConfig::default(),
            car_file_writer: None,
        }
    }
}

/// Config for block uploads
#[derive(Debug, Clone)]
pub struct UploaderConfig {
    pub blocks_table_name: String,
    pub use_md5_row_key_salt: bool,
    pub use_blocks_compression: bool,
    pub hbase_write_to_wal: bool,
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            blocks_table_name: BLOCKS_TABLE_NAME.to_string(),
            use_md5_row_key_salt: false,
            use_blocks_compression: true,
            hbase_write_to_wal: true,
        }
    }
}

/// Holds block data in memory before finalizing to a .car file.
struct InMemoryCarAccumulator {
    builder: InMemoryCarBuilder,
    min_slot: Option<Slot>,
    max_slot: Option<Slot>,

    // Keep metadata for each block so we can create CarIndexEntry with real info
    block_metadata: HashMap<String, (String, u64, String)>,
    first_block_time: Option<u64>,
}

// MANUAL Default. We cannot `#[derive(Default)]` because InMemoryCarBuilder doesn't implement Default.
impl Default for InMemoryCarAccumulator {
    fn default() -> Self {
        Self {
            builder: InMemoryCarBuilder::new(),
            min_slot: None,
            max_slot: None,
            block_metadata: HashMap::new(),
            first_block_time: None,
        }
    }
}

impl InMemoryCarAccumulator {
    fn new() -> Self {
        Self::default()
    }

    fn is_empty(&self) -> bool {
        self.min_slot.is_none()
    }

    /// Add a block's compressed bytes + metadata
    fn add_block(
        &mut self,
        slot: Slot,
        row_key: &RowKey,
        data: &RowData,
        block_hash: &str,
        block_time: u64,
        previous_block_hash: &str,
    ) -> Result<()> {
        self.min_slot = Some(self.min_slot.map_or(slot, |s| s.min(slot)));
        self.max_slot = Some(self.max_slot.map_or(slot, |s| s.max(slot)));

        if self.first_block_time.is_none() {
            self.first_block_time = Some(block_time);
        }

        self.builder
            .add_row(row_key, data)
            .map_err(|e| Error::IoError(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        // Store metadata for final CarIndexEntry
        self.block_metadata.insert(
            row_key.to_string(),
            (
                block_hash.to_string(),
                block_time,
                previous_block_hash.to_string(),
            ),
        );

        Ok(())
    }

    /// Finalize the CAR, returning the built bytes + index + the slot range
    fn finalize_car(&mut self) -> Result<(Vec<u8>, Vec<BlockIndexEntry>, Slot, Slot, u64)> {
        let min_slot = self.min_slot.unwrap_or(0);
        let max_slot = self.max_slot.unwrap_or(0);
        let first_block_time = self.first_block_time.ok_or_else(|| {
            Error::IoError(std::io::Error::new(std::io::ErrorKind::Other, "Missing first block time"))
        })?;

        let old_builder = mem::replace(&mut self.builder, InMemoryCarBuilder::new());
        let (car_bytes, index) = old_builder
            .finalize()
            .map_err(|e| Error::IoError(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        self.min_slot = None;
        self.max_slot = None;
        self.first_block_time = None;

        Ok((car_bytes, index, min_slot, max_slot, first_block_time))
    }
}

/// The LedgerStorage that accumulates blocks, finalizes them,
/// writes the .car file, and indexes them in HBase.
#[derive(Clone)]
pub struct LedgerStorage {
    connection: HBaseConnection,
    uploader_config: UploaderConfig,
    accumulator: Arc<Mutex<InMemoryCarAccumulator>>,

    /// Must be present for writing to HDFS
    car_file_writer: Arc<dyn CarFileWriter + Send + Sync>,
}

impl LedgerStorage {
    /// Create with config. Fails if no `car_file_writer`.
    pub async fn new_with_config(config: LedgerStorageConfig) -> Result<Self> {
        let connection = HBaseConnection::new(&config.address, config.namespace.as_deref()).await;
        let car_file_writer = config.car_file_writer.ok_or(Error::MissingCarFileWriter)?;

        Ok(Self {
            connection,
            uploader_config: config.uploader_config,
            accumulator: Arc::new(Mutex::new(InMemoryCarAccumulator::new())),
            car_file_writer,
        })
    }

    /// Build with defaults (will fail if `car_file_writer` is None).
    pub async fn new() -> Result<Self> {
        Self::new_with_config(LedgerStorageConfig::default()).await
    }

    /// Convert `VersionedConfirmedBlock` to Protobuf + optional compression,
    /// then add it to the in-memory accumulator with real block metadata.
    pub async fn upload_confirmed_block(
        &self,
        slot: Slot,
        confirmed_block: VersionedConfirmedBlock,
    ) -> Result<()> {
        info!("upload_confirmed_block: slot={}", slot);

        let proto_block: generated::ConfirmedBlock = confirmed_block.into();

        // We do NOT consume .block_time, we borrow it:
        let block_time_i64 = proto_block
            .block_time
            .as_ref()
            .map_or(0, |ts| ts.timestamp);

        // Convert i64 -> u64 by clamping negative to zero
        let block_time = if block_time_i64 < 0 {
            0
        } else {
            block_time_i64 as u64
        };

        // Copy the blockhash and previous_blockhash
        let block_hash = proto_block.blockhash.clone();
        let previous_block_hash = proto_block.previous_blockhash.clone();

        // Now we can still call proto_block.encoded_len() without partial move:
        let mut buf = Vec::with_capacity(proto_block.encoded_len());
        proto_block.encode(&mut buf).map_err(Error::EncodingError)?;

        let final_bytes = if self.uploader_config.use_blocks_compression {
            compress_best(&buf).map_err(Error::IoError)?
        } else {
            compress(CompressionMethod::NoCompression, &buf).map_err(Error::IoError)?
        };

        let row_key = format!("{:016x}", slot);

        let mut acc = self.accumulator.lock().await;
        acc.add_block(
            slot,
            &row_key,
            &final_bytes,
            &block_hash,
            block_time,
            &previous_block_hash,
        )?;

        Ok(())
    }

    /// Finalize the .car file, write it via `CarFileWriter`, then store index in HBase.
    pub async fn finalize(&self) -> Result<()> {
        let mut acc = self.accumulator.lock().await;
        if acc.is_empty() {
            debug!("finalize: no blocks to store");
            return Ok(());
        }

        let (
            car_bytes,
            index_entries,
            min_slot,
            max_slot,
            first_block_time
        ) = acc.finalize_car()?;

        drop(acc);

        // Always writes to HDFS
        let car_path = self
            .car_file_writer
            .write_car(&car_bytes, min_slot, max_slot, first_block_time)
            .await
            .map_err(|e| Error::IoError(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        info!(
            "Wrote .car file for slots [{}, {}] -> {}",
            min_slot, max_slot, car_path
        );

        // We'll store rows into the table from config
        let table_name = self.uploader_config.blocks_table_name.clone();
        let mut tasks = Vec::with_capacity(index_entries.len());

        // Retrieve metadata from accumulator
        let mut acc = self.accumulator.lock().await;

        for be in index_entries {
            let slot_val = u64::from_str_radix(&be.row_key, 16).unwrap_or(0);

            let (block_hash, block_time, previous_block_hash) = acc
                .block_metadata
                .remove(&be.row_key)
                .unwrap_or_else(|| {
                    ("unknown_hash".to_string(), 0, "unknown_hash".to_string())
                });

            let index_proto = CarIndexEntry {
                slot: slot_val,
                block_hash,
                offset: be.offset,
                length: be.length,
                start_slot: min_slot,
                end_slot: max_slot,
                timestamp: Some(UnixTimestamp { timestamp: first_block_time as i64 }),
                previous_block_hash,
                block_height: None, // Assuming we don't have block height available
                block_time: Some(UnixTimestamp { timestamp: block_time as i64 }),
            };

            let row_key = be.row_key;
            let conn = self.connection.clone();
            let table_name_cloned = table_name.clone();
            let write_to_wal = self.uploader_config.hbase_write_to_wal;

            tasks.push(tokio::spawn(async move {
                conn.put_protobuf_cells_with_retry::<CarIndexEntry>(
                    &table_name_cloned,
                    &[(row_key, index_proto)],
                    false,
                    write_to_wal,
                ).await
            }));
        }
        drop(acc); // done reading metadata

        // Await all tasks
        let results = futures::future::join_all(tasks).await;
        for res in results {
            match res {
                Ok(Err(hbase_err)) => return Err(Error::HBaseError(hbase_err)),
                Err(join_err) => return Err(Error::TokioJoinError(join_err)),
                _ => (),
            }
        }

        Ok(())
    }
}