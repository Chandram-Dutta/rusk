use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{Result, RuskError};

const LOG_FILE_NAME: &str = "data.log";
const COMPACTION_THRESHOLD: u64 = 1024 * 1024; // 1MB threshold for compaction

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Debug, Clone, Copy)]
struct CommandPos {
    offset: u64,
    length: u64,
}

/// The Bitcask-style key-value store engine.
/// Each entry on disk is written as:
/// ```text
/// [4 bytes: length (u32 big-endian)] [N bytes: JSON-serialized Command]
/// ```
pub struct RuskStore {
    path: PathBuf,
    index: HashMap<String, CommandPos>,
    writer: BufWriter<File>,
    current_pos: u64,
    uncompacted: u64,
}

impl RuskStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let log_path = path.join(LOG_FILE_NAME);

        let writer_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;

        let mut store = RuskStore {
            path,
            index: HashMap::new(),
            writer: BufWriter::new(writer_file),
            current_pos: 0,
            uncompacted: 0,
        };

        store.replay_log()?;

        Ok(store)
    }

    fn replay_log(&mut self) -> Result<()> {
        let log_path = self.path.join(LOG_FILE_NAME);

        if !log_path.exists() {
            return Ok(());
        }

        let file = File::open(&log_path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let mut pos: u64 = 0;

        let mut previous_positions: HashMap<String, u64> = HashMap::new();

        while pos < file_len {
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let data_len = u32::from_be_bytes(len_buf) as u64;

            let mut data_buf = vec![0u8; data_len as usize];
            reader.read_exact(&mut data_buf)?;

            let cmd: Command = serde_json::from_slice(&data_buf)?;

            let entry_len = 4 + data_len;

            match &cmd {
                Command::Set { key, .. } => {
                    if let Some(old_len) = previous_positions.insert(key.clone(), entry_len) {
                        self.uncompacted += old_len;
                    }
                    self.index.insert(
                        key.clone(),
                        CommandPos {
                            offset: pos,
                            length: entry_len,
                        },
                    );
                }
                Command::Remove { key } => {
                    if let Some(old_pos) = self.index.remove(key) {
                        self.uncompacted += old_pos.length;
                    }
                    self.uncompacted += entry_len;
                    previous_positions.remove(key);
                }
            }

            pos += entry_len;
        }

        self.current_pos = pos;
        Ok(())
    }

    /// Sets a key-value pair.
    ///
    /// If the key already exists, the old value is overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };

        let pos = self.write_command(&cmd)?;

        if let Some(old_pos) = self.index.insert(key, pos) {
            self.uncompacted += old_pos.length;
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Gets the value for a key.
    ///
    /// Returns `None` if the key doesn't exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(&cmd_pos) = self.index.get(&key) {
            let log_path = self.path.join(LOG_FILE_NAME);
            let file = File::open(&log_path)?;
            let mut reader = BufReader::new(file);

            reader.seek(SeekFrom::Start(cmd_pos.offset))?;

            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let data_len = u32::from_be_bytes(len_buf) as usize;

            let mut data_buf = vec![0u8; data_len];
            reader.read_exact(&mut data_buf)?;

            let cmd: Command = serde_json::from_slice(&data_buf)?;
            match cmd {
                Command::Set { value, .. } => Ok(Some(value)),
                Command::Remove { .. } => Err(RuskError::UnexpectedCommand),
            }
        } else {
            Ok(None)
        }
    }

    /// Removes a key from the store.
    ///
    /// Returns an error if the key doesn't exist.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(RuskError::KeyNotFound);
        }

        let cmd = Command::Remove { key: key.clone() };
        let pos = self.write_command(&cmd)?;

        if let Some(old_pos) = self.index.remove(&key) {
            self.uncompacted += old_pos.length;
        }
        self.uncompacted += pos.length;

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    fn write_command(&mut self, cmd: &Command) -> Result<CommandPos> {
        let data = serde_json::to_vec(cmd)?;
        let data_len = data.len() as u32;

        let offset = self.current_pos;

        self.writer.write_all(&data_len.to_be_bytes())?;
        self.writer.write_all(&data)?;
        self.writer.flush()?;

        let entry_len = 4 + data.len() as u64;
        self.current_pos += entry_len;

        Ok(CommandPos {
            offset,
            length: entry_len,
        })
    }

    /// Compacts the log by rewriting only the live entries.
    ///
    /// This removes all dead space from overwritten or deleted keys.
    pub fn compact(&mut self) -> Result<()> {
        let compaction_path = self.path.join("data.compact");
        let log_path = self.path.join(LOG_FILE_NAME);

        let compact_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&compaction_path)?;
        let mut compact_writer = BufWriter::new(compact_file);

        let reader_file = File::open(&log_path)?;
        let mut reader = BufReader::new(reader_file);

        let mut new_index = HashMap::new();
        let mut new_pos: u64 = 0;

        for (key, cmd_pos) in &self.index {
            reader.seek(SeekFrom::Start(cmd_pos.offset))?;

            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let data_len = u32::from_be_bytes(len_buf) as usize;

            let mut data_buf = vec![0u8; data_len];
            reader.read_exact(&mut data_buf)?;

            compact_writer.write_all(&len_buf)?;
            compact_writer.write_all(&data_buf)?;

            let entry_len = 4 + data_len as u64;
            new_index.insert(
                key.clone(),
                CommandPos {
                    offset: new_pos,
                    length: entry_len,
                },
            );
            new_pos += entry_len;
        }

        compact_writer.flush()?;
        drop(compact_writer);
        drop(reader);

        fs::rename(&compaction_path, &log_path)?;

        let writer_file = OpenOptions::new().append(true).open(&log_path)?;

        self.writer = BufWriter::new(writer_file);
        self.index = new_index;
        self.current_pos = new_pos;
        self.uncompacted = 0;

        Ok(())
    }
}
