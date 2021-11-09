use std::{ffi::OsStr, fs::File, path::PathBuf};

use anyhow::{format_err, Result};
use polars::prelude::{CsvReader, DataFrame, IpcReader, ParquetReader, SerReader};

#[derive(Debug)]
pub enum InputFormat {
    Csv,
    Ipc,
    Parquet,
}

pub fn input_format(path: &PathBuf) -> Result<InputFormat> {
    let ext = path.extension();

    if let Some(ext) = ext {
        if ext == OsStr::new("csv") {
            Ok(InputFormat::Csv)
        } else if ext == OsStr::new("ipc") {
            Ok(InputFormat::Ipc)
        } else if ext == OsStr::new("parquet") {
            Ok(InputFormat::Parquet)
        } else {
            Err(format_err!(
                "Not Supported Format: {:?}. Please specify from csv, ipc or parquet",
                ext
            ))
        }
    } else {
        Err(format_err!(
            "No format is specified. Please specify format from csv, ipc or prequet"
        ))
    }
}

pub fn read_dataframe(path: &PathBuf) -> Result<DataFrame> {
    let format = input_format(path)?;

    match format {
        InputFormat::Csv => Ok(CsvReader::from_path(path)?.has_header(true).finish()?),
        InputFormat::Ipc => {
            let f = File::open(path)?;
            Ok(IpcReader::new(f).finish()?)
        }
        InputFormat::Parquet => {
            let f = File::open(path)?;
            Ok(ParquetReader::new(f).finish()?)
        }
    }
}
