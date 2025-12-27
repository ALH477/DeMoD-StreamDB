//! StreamDB CLI tool

#[cfg(feature = "cli")]
fn main() {
    use clap::{Parser, Subcommand};
    use streamdb::{StreamDb, Config, Result};
    
    #[derive(Parser)]
    #[command(name = "streamdb")]
    #[command(about = "StreamDB command-line interface", version)]
    struct Cli {
        /// Database file path
        #[arg(short, long, default_value = "streamdb.dat")]
        database: String,
        
        #[command(subcommand)]
        command: Commands,
    }
    
    #[derive(Subcommand)]
    enum Commands {
        /// Insert a key-value pair
        Insert {
            /// Key
            key: String,
            /// Value
            value: String,
        },
        /// Get a value by key
        Get {
            /// Key
            key: String,
        },
        /// Delete a key
        Delete {
            /// Key
            key: String,
        },
        /// Search for keys by suffix
        Search {
            /// Suffix to search for
            suffix: String,
        },
        /// Show database statistics
        Stats,
    }
    
    fn run() -> Result<()> {
        let cli = Cli::parse();
        
        let db = StreamDb::open(&cli.database, Config::default())?;
        
        match cli.command {
            Commands::Insert { key, value } => {
                let id = db.insert(key.as_bytes(), value.as_bytes())?;
                println!("Inserted with ID: {}", id);
            }
            Commands::Get { key } => {
                match db.get(key.as_bytes())? {
                    Some(value) => {
                        match std::str::from_utf8(&value) {
                            Ok(s) => println!("{}", s),
                            Err(_) => println!("{:?}", value),
                        }
                    }
                    None => {
                        eprintln!("Key not found");
                        std::process::exit(1);
                    }
                }
            }
            Commands::Delete { key } => {
                if db.delete(key.as_bytes())? {
                    println!("Deleted");
                } else {
                    eprintln!("Key not found");
                    std::process::exit(1);
                }
            }
            Commands::Search { suffix } => {
                let results = db.suffix_search(suffix.as_bytes())?;
                if results.is_empty() {
                    println!("No results");
                } else {
                    for result in results {
                        let key_str = std::str::from_utf8(&result.key)
                            .unwrap_or("<binary>");
                        println!("{} -> {}", key_str, result.id);
                    }
                }
            }
            Commands::Stats => {
                let stats = db.stats()?;
                println!("Keys: {}", stats.key_count);
                println!("Total size: {} bytes", stats.total_size);
                println!("Cache hits: {}", stats.cache_stats.hits);
                println!("Cache misses: {}", stats.cache_stats.misses);
                println!("Hit ratio: {:.2}%", stats.cache_stats.hit_ratio() * 100.0);
            }
        }
        
        db.flush()?;
        Ok(())
    }
    
    if let Err(e) = run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

#[cfg(not(feature = "cli"))]
fn main() {
    eprintln!("CLI feature not enabled. Rebuild with --features cli");
    std::process::exit(1);
}
