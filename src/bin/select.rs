use anyhow::Result;
use std::path::PathBuf;
use structopt::StructOpt;

use df_utils::{reader::read_dataframe, writer::write_dataframe};

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(
        name = "INPUT FILE",
        help = "input file. - means stdio. only csv is supported for stdio."
    )]
    input_path: PathBuf,
    #[structopt(name = "COLUMNS", help = "columns to select")]
    cols: Vec<String>,
    #[structopt(short = "-o", long = "--out", help = "output path. default is stdout")]
    out_path: Option<PathBuf>,
    #[structopt(
        short = "-p",
        long = "prt",
        help = "write formatted table instead of raw csv."
    )]
    pretty: bool,
    #[structopt(
        short = "-d",
        long = "--dilm",
        help = "delim for csv writer",
        default_value = ","
    )]
    dilm: char,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    dbg!(&opt);
    let mut df = read_dataframe(&opt.input_path)?;

    if opt.cols.len() != 0 {
        df = df.select(&opt.cols)?;
    }

    if opt.pretty && opt.out_path.is_none() {
        println!("{:?}", df);
    } else {
        write_dataframe(opt.out_path.as_ref(), &df)?;
    }

    Ok(())
}
