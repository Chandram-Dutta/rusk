use std::env;
use std::process;

use clap::{Parser, Subcommand};
use rusk::{Result, RuskStore};

#[derive(Parser)]
#[command(name = "rusk")]
#[command(about = "A Bitcask-style key-value store", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Set a key-value pair
    Set { key: String, value: String },
    /// Get the value for a key
    Get { key: String },
    /// Remove a key
    Rm { key: String },
    /// Manually trigger compaction
    Compact,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let current_dir = env::current_dir()?;
    let mut store = RuskStore::open(current_dir)?;

    match cli.command {
        Commands::Set { key, value } => {
            store.set(key, value)?;
        }
        Commands::Get { key } => match store.get(key)? {
            Some(value) => println!("{}", value),
            None => println!("Key not found"),
        },
        Commands::Rm { key } => {
            if let Err(e) = store.remove(key) {
                eprintln!("{}", e);
                process::exit(1);
            }
        }
        Commands::Compact => {
            store.compact()?;
            println!("Compaction complete");
        }
    }

    Ok(())
}
