use clap::{App, Arg};

pub fn block_uploader_app<'a>(version: &'a str) -> App<'a, 'a> {
    return App::new("solana-block-uploader-service")
        .about("Solana Block Uploader Service")
        .version(version)
        .arg(
            Arg::with_name("disable_blocks_compression")
                .long("disable-blocks-compression")
                .takes_value(false)
                .help("Disables blocks table compression."),
        )
        .arg(
            Arg::with_name("hbase_skip_wal")
                .long("hbase-skip-wal")
                .takes_value(false)
                .help("If HBase should skip WAL when writing new data."),
        );
}
