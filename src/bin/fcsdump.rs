//! fcsdump — inspect an FCS file or dump its values as CSV (parity harness helper).
//!
//!   fcsdump meta <file>                      # version, datatype, byte order, $TOT, params
//!   fcsdump csv  <file> [--no-truncate] [--linearize] [--threads N] [--rows N] > out.csv
//!   fcsdump bench <file> [--threads N]       # decode timing only
use std::io::Write;
use std::time::Instant;

use anyhow::{Result, bail};
use read_fcs_operator::{ReadOptions, read_file};

fn main() -> Result<()> {
    // `fcsdump meta … | head` must not panic on the closed pipe.
    #[cfg(unix)]
    unsafe {
        libc_sigpipe_default();
    }
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        bail!(
            "usage: fcsdump <meta|csv|bench> <file> [--no-truncate] [--linearize] [--threads N] [--rows N] [--dataset K]"
        );
    }
    let cmd = args[1].as_str();
    let path = &args[2];
    let mut opts = ReadOptions::default();
    let mut rows: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--no-truncate" => opts.truncate_max_range = false,
            "--linearize" => opts.linearize = true,
            "--empty-value" => opts.empty_value = true,
            "--threads" => {
                opts.threads = Some(args[i + 1].parse()?);
                i += 1;
            }
            "--rows" => {
                rows = Some(args[i + 1].parse()?);
                i += 1;
            }
            "--dataset" => {
                opts.dataset = args[i + 1].parse()?;
                i += 1;
            }
            other => bail!("unknown flag {other}"),
        }
        i += 1;
    }
    let t0 = Instant::now();
    let d = read_file(path, &opts)?;
    let dt = t0.elapsed();
    match cmd {
        "meta" => {
            println!(
                "version={} datatype={:?} endian={:?} events={} params={} datasets={} read={:.3}s",
                d.version,
                d.datatype,
                d.endian,
                d.n_events,
                d.n_params(),
                d.n_datasets,
                dt.as_secs_f64()
            );
            for p in &d.params {
                println!(
                    "  P{:<3} {:<28} {:<28} bits={:<3} range={:<12} E={:?} G={}",
                    p.index, p.name, p.desc, p.bits, p.range, p.e, p.g
                );
            }
            for k in [
                "$SPILLOVER",
                "SPILL",
                "$CYT",
                "$CYTSN",
                "$DATE",
                "$BTIM",
                "$ETIM",
                "$BEGINDATA",
                "$ENDDATA",
                "$NEXTDATA",
            ] {
                if let Some(v) = d.keyword(k) {
                    let v = if v.len() > 80 {
                        format!("{}…", &v[..80])
                    } else {
                        v.to_string()
                    };
                    println!("  {k} = {v}");
                }
            }
        }
        "bench" => {
            println!(
                "{}: {} events × {} params decoded in {:.3}s ({:.1} M values/s)",
                path,
                d.n_events,
                d.n_params(),
                dt.as_secs_f64(),
                (d.n_events * d.n_params()) as f64 / dt.as_secs_f64() / 1e6
            );
        }
        "csv" => {
            let out = std::io::stdout();
            let mut w = std::io::BufWriter::with_capacity(1 << 20, out.lock());
            let names: Vec<String> = d.params.iter().map(|p| p.name.replace(',', "_")).collect();
            writeln!(w, "{}", names.join(","))?;
            let n = rows.unwrap_or(d.n_events).min(d.n_events);
            let mut line = String::with_capacity(d.n_params() * 24);
            for ev in 0..n {
                line.clear();
                for (p, col) in d.columns.iter().enumerate() {
                    if p > 0 {
                        line.push(',');
                    }
                    // shortest round-trip repr, like Python's repr(float)
                    line.push_str(&format!("{:?}", col[ev]));
                }
                writeln!(w, "{line}")?;
            }
        }
        other => bail!("unknown command {other}"),
    }
    Ok(())
}

#[cfg(unix)]
unsafe fn libc_sigpipe_default() {
    unsafe extern "C" {
        fn signal(signum: i32, handler: usize) -> usize;
    }
    const SIGPIPE: i32 = 13;
    const SIG_DFL: usize = 0;
    unsafe {
        signal(SIGPIPE, SIG_DFL);
    }
}
