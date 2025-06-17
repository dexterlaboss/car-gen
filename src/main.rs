
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
        hdfs_writer::{HdfsWriter, DailyPartitionedPathStrategy, CarFileWriter},
    },
    ledger_storage::UploaderConfig,
    std::{sync::Arc},
    anyhow::{Context, Result},
    clap::{ArgMatches},
    log::info,
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

mod hdfs_writer;
mod hbase;

mod mem_utils;

const SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI
    let cli_app = block_uploader_app(SERVICE_VERSION);
    let _matches = cli_app.get_matches();

    // Initialize logging
    env_logger::init();
    info!("Starting the Solana block ingestor (Version: {})", SERVICE_VERSION);

    // Load main config
    let config = Arc::new(Config::new());  // your custom config struct

    // Create HDFS reader client
    let hdfs_client_source = Client::new(&config.hdfs_url)
        .context("Failed to create source HDFS client")?;

    // Create HDFS writer client
    let hdfs_client_writer = Client::new(&config.hdfs_url)
        .context("Failed to create writer HDFS client")?;

    // Build an HdfsWriter with appropriate path generation strategy
    let strategy = DailyPartitionedPathStrategy {
        base_path: config.hdfs_path.clone(),
    };
    let hdfs_writer = Arc::new(HdfsWriter::new(hdfs_client_writer, strategy))
        as Arc<dyn CarFileWriter + Send + Sync>;

    // Create a file storage for reading data from HDFS
    let file_storage = HdfsStorage::new(hdfs_client_source);

    // Create decompressor, message decoder, format parser
    let decompressor: Box<dyn Decompressor + Send + Sync> = Box::new(GzipDecompressor {});
    let message_decoder: Arc<dyn MessageDecoder + Send + Sync> = Arc::new(JsonMessageDecoder {});
    let format_parser: Arc<dyn FormatParser + Send + Sync> = Arc::new(NdJsonParser {});

    // Build ledger storage config, injecting the HdfsWriter
    let ledger_storage_config = LedgerStorageConfig {
        address: config.hbase_address.clone(),
        namespace: None,
        uploader_config: Default::default(),
        car_file_writer: Some(hdfs_writer),
        ..Default::default()
    };

    // Create LedgerStorage
    let ledger_storage = LedgerStorage::new_with_config(ledger_storage_config)
        .await
        .expect("Failed to create LedgerStorage");

    // Create the block processor using ledger_storage
    let block_processor = BlockProcessor::new(ledger_storage);

    // Create the file processor
    let file_processor = Arc::new(FileProcessor::new(
        file_storage,
        format_parser,
        block_processor,
        decompressor,
    ));

    // Create Kafka consumer config
    let kafka_config = KafkaConfig {
        group_id: config.kafka_group_id.clone(),
        bootstrap_servers: config.kafka_brokers.clone(),
        enable_partition_eof: false,
        session_timeout_ms: 10_000,
        enable_auto_commit: true,
        auto_offset_reset: "earliest".to_string(),
        max_partition_fetch_bytes: 10 * 1024 * 1024,
        max_in_flight_requests_per_connection: 1,
        max_poll_interval_ms: config.kafka_max_poll_interval_ms,
        log_level: RDKafkaLogLevel::Debug,
    };

    // Create queue consumer
    let consumer: Box<dyn QueueConsumer + Send + Sync> = Box::new(
        KafkaQueueConsumer::new(kafka_config, &[&config.kafka_consume_topic])?
    );

    // Create queue producer
    let kafka_producer = KafkaQueueProducer::new(
        &config.kafka_brokers,
        &config.kafka_produce_error_topic
    )?;

    // Create the ingestor
    let mut ingestor = Ingestor::new(
        consumer,
        kafka_producer,
        file_processor,
        message_decoder,
    );

    // Run ingestion loop
    ingestor.run().await?;

    Ok(())
}


/// Process uploader-related CLI arguments
#[allow(dead_code)]
fn process_uploader_arguments(matches: &ArgMatches) -> UploaderConfig {
    let use_md5_row_key_salt = matches.is_present("use_md5_row_key_salt");
    let use_blocks_compression = !matches.is_present("disable_block_compression");
    let hbase_write_to_wal = !matches.is_present("hbase_skip_wal");

    UploaderConfig {
        _use_md5_row_key_salt: use_md5_row_key_salt,
        use_blocks_compression,
        hbase_write_to_wal,
        ..Default::default()
    }
}
