
use {
    crate::{
        block_processor::BlockProcessor,
        cli::block_uploader_app,
        config::Config,
        decompressor::{Decompressor, GzipDecompressor},
        file_processor::FileProcessor,
        file_storage::HdfsStorage,
        format_parser::{FormatParser, NdJsonParser},
        ingestor::Ingestor,
        ledger_storage::{LedgerStorage, LedgerStorageConfig},
        message_decoder::{JsonMessageDecoder, MessageDecoder},
        queue_consumer::{KafkaConfig, KafkaQueueConsumer, QueueConsumer},
        queue_producer::KafkaQueueProducer,
    },
    ledger_storage::UploaderConfig,
    std::{collections::HashSet, sync::Arc},
    anyhow::{Context, Result},
    clap::{value_t_or_exit, values_t, ArgMatches},
    log::info,
    solana_sdk::pubkey::Pubkey,
    hdfs_native::Client,
    rdkafka::config::RDKafkaLogLevel,
};

// Our modules
mod block_processor;
mod cli;
mod config;
mod decompressor;
mod file_storage;
mod format_parser;
mod ingestor;
mod ledger_storage;
mod message_decoder;
mod file_processor;
mod queue_consumer;
mod queue_producer;
mod record_stream;

// The new modules from your snippet
mod hdfs_writer;
mod hbase;

mod mem_utils;

const SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Parse CLI
    let cli_app = block_uploader_app(SERVICE_VERSION);
    let matches = cli_app.get_matches();

    // 2. Initialize logging
    env_logger::init();
    info!("Starting the Solana block ingestor (Version: {})", SERVICE_VERSION);

    // 3. Load main config
    let config = Arc::new(Config::new());  // your custom config struct

    // 4. Create HDFS client for reading source data
    let hdfs_client_source = Client::new(&config.hdfs_url)
        .context("Failed to create source HDFS client")?;

    // 5. Create HDFS client + writer for .car files
    //    (You could reuse the same client as in step #4 if you want.)
    let hdfs_client_writer = Client::new(&config.hdfs_url)
        .context("Failed to create writer HDFS client")?;

    // 6. Build an HdfsWriter with your chosen path strategy
    use crate::hdfs_writer::{HdfsWriter, DailyPartitionedPathStrategy, CarFileWriter};
    let strategy = DailyPartitionedPathStrategy {
        base_path: "/chain-archives/sol/car_test".to_string(),
    };
    let hdfs_writer = Arc::new(HdfsWriter::new(hdfs_client_writer, strategy))
        as Arc<dyn CarFileWriter + Send + Sync>;

    // 7. Create a file storage for reading data from HDFS
    let file_storage = HdfsStorage::new(hdfs_client_source);

    // 8. Create decompressor, message decoder, format parser
    let decompressor: Box<dyn Decompressor + Send + Sync> = Box::new(GzipDecompressor {});
    let message_decoder: Arc<dyn MessageDecoder + Send + Sync> = Arc::new(JsonMessageDecoder {});
    let format_parser: Arc<dyn FormatParser + Send + Sync> = Arc::new(NdJsonParser {});

    // 9. Build ledger storage config, injecting the HdfsWriter
    let ledger_storage_config = LedgerStorageConfig {
        address: config.hbase_address.clone(),
        namespace: None,
        uploader_config: Default::default(),
        car_file_writer: Some(hdfs_writer), // or however you create the writer
        ..Default::default()
    };

    // 10. Create LedgerStorage
    let ledger_storage = LedgerStorage::new_with_config(ledger_storage_config)
        .await
        .expect("Failed to create LedgerStorage");

    // 11. Create the block processor using ledger_storage
    let block_processor = BlockProcessor::new(ledger_storage);

    // 12. Create the file processor
    let file_processor = Arc::new(FileProcessor::new(
        file_storage,
        format_parser,
        block_processor,
        decompressor,
    ));

    // 13. Kafka consumer config
    let kafka_config = KafkaConfig {
        group_id: config.kafka_group_id.clone(),
        bootstrap_servers: config.kafka_brokers.clone(),
        enable_partition_eof: false,
        session_timeout_ms: 10_000,
        enable_auto_commit: true,
        auto_offset_reset: "earliest".to_string(),
        max_partition_fetch_bytes: 10 * 1024 * 1024,
        max_in_flight_requests_per_connection: 1,
        log_level: RDKafkaLogLevel::Debug,
    };

    // 14. Create consumer
    let consumer: Box<dyn QueueConsumer + Send + Sync> = Box::new(
        KafkaQueueConsumer::new(kafka_config, &[&config.kafka_consume_topic])?
    );

    // 15. Create producer
    let kafka_producer = KafkaQueueProducer::new(
        &config.kafka_brokers,
        &config.kafka_produce_error_topic
    )?;

    // 16. Create the ingestor
    let mut ingestor = Ingestor::new(
        consumer,
        kafka_producer,
        file_processor,
        message_decoder,
    );

    // 17. Run ingestion loop
    ingestor.run().await?;

    Ok(())
}


/// Process uploader-related CLI arguments
fn process_uploader_arguments(matches: &ArgMatches) -> UploaderConfig {
    let use_md5_row_key_salt = matches.is_present("use_md5_row_key_salt");
    let use_blocks_compression = !matches.is_present("disable_block_compression");
    let hbase_write_to_wal = !matches.is_present("hbase_skip_wal");

    UploaderConfig {
        use_md5_row_key_salt,
        use_blocks_compression,
        hbase_write_to_wal,
        ..Default::default()
    }
}
