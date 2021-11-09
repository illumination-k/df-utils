use std::{ffi::OsStr, fs::File, io::BufWriter, path::PathBuf};

use anyhow::{format_err, Result};
use polars::prelude::*;

#[derive(Debug)]
pub enum OutputFormat {
    Csv,
    CsvGz,
    Ipc,
    Parquet,
}

pub fn output_format(path: &PathBuf) -> Result<OutputFormat> {
    if let Some(ext) = path.extension() {
        if ext == OsStr::new("csv") {
            Ok(OutputFormat::Csv)
        } else if ext == OsStr::new("ipc") {
            Ok(OutputFormat::Ipc)
        } else if ext == OsStr::new("parquet") {
            Ok(OutputFormat::Parquet)
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

pub fn write_dataframe(path: Option<&PathBuf>, df: &DataFrame) -> Result<()> {
    if path.is_none() {
        let mut buf = BufWriter::new(std::io::stdout());
        CsvWriter::new(&mut buf).has_header(true).finish(df)?;
        return Ok(());
    }
    let path = path.unwrap();
    let format = output_format(path)?;
    let mut f = File::create(path)?;

    match format {
        OutputFormat::Csv => {
            CsvWriter::new(&mut f).has_header(true).finish(df)?;
        }
        OutputFormat::Ipc => {
            IpcWriter::new(&mut f).finish(df)?;
        }
        OutputFormat::Parquet => {
            ParquetWriter::new(&mut f).finish(df)?;
        }
        _ => unimplemented!(),
    }

    Ok(())
}
