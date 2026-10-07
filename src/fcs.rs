//! FCS 2.0 / 3.0 / 3.1 reader.
//!
//! Port of the reading semantics of flowCore `read.FCS(transformation = FALSE, emptyValue = FALSE,
//! truncate_max_range = ...)` (the settings tercen/read_fcs_operator uses) cross-checked against
//! Python `fcsparser` 0.2.8. Differences from those references are listed in `CLAUDE.md`.
//!
//! Layout of an FCS file: 58-byte HEADER (version + six 8-char ASCII offsets: TEXT start/end,
//! DATA start/end, ANALYSIS start/end) → TEXT (delimited keyword/value pairs) → DATA (list mode,
//! events × parameters, `$DATATYPE` F/D/I, `$BYTEORD`) → optional ANALYSIS; `$NEXTDATA` may chain
//! further data sets. FCS 3.x files larger than 99,999,999 bytes put `0` in the HEADER data offsets
//! and the true offsets in `$BEGINDATA`/`$ENDDATA`.

use std::fs::File;
use std::path::Path;

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use indexmap::IndexMap;
use memmap2::Mmap;
use rayon::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FcsError {
    #[error("not an FCS file (version field {0:?})")]
    NotFcs(String),
    #[error("header: {0}")]
    Header(String),
    #[error("TEXT segment: {0}")]
    Text(String),
    #[error("DATA segment: {0}")]
    Data(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("dataset {requested} requested but file has {available}")]
    NoSuchDataset { requested: usize, available: usize },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, FcsError>;

/// One parameter (channel) as declared in TEXT.
#[derive(Debug, Clone, PartialEq)]
pub struct Param {
    /// 1-based index n of `$PnN`.
    pub index: usize,
    /// `$PnN` — short name (the channel name Tercen uses).
    pub name: String,
    /// `$PnS` — long name / description (marker), may be empty.
    pub desc: String,
    /// `$PnB` — bits per value.
    pub bits: u32,
    /// `$PnR` — range, as written (may be a float string on spectral instruments).
    pub range: f64,
    /// `$PnE` — (decades, offset); (0, 0) = linear.
    pub e: (f64, f64),
    /// `$PnG` — gain, 1 if absent.
    pub g: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Float32,
    Float64,
    Integer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endian {
    Little,
    Big,
}

/// Reading options — defaults reproduce `tercen/read_fcs_operator` (flowCore with
/// `transformation = FALSE`, `truncate_max_range = TRUE`, `emptyValue = FALSE`).
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Clamp values above `$PnR` to `$PnR` (flowCore `truncate_max_range`).
    pub truncate_max_range: bool,
    /// Apply `$PnE` log→linear and `$PnG` scaling (flowCore `transformation = "linearize"`).
    /// Off by default: the R operator reads raw values.
    pub linearize: bool,
    /// TEXT parsing: `false` = a doubled delimiter is an escaped delimiter (FCS 3.1 rule,
    /// flowCore `emptyValue = FALSE`); `true` = allow empty values, so `//` means an empty value.
    pub empty_value: bool,
    /// Which data set to read when `$NEXTDATA` chains several (0-based). The R operator passes
    /// flowCore `dataset = 2`, i.e. the **second** set when present (Coulter LMD files carry an
    /// FCS2.0 set followed by an FCS3.0 set), falling back to the first otherwise. Default 1.
    pub dataset: usize,
    /// `true` (flowCore `ignore.text.offset = TRUE`, the operator's setting): DATA offsets come from
    /// the HEADER, and TEXT `$BEGINDATA`/`$ENDDATA` are consulted only when the header fields are 0.
    pub ignore_text_offset: bool,
    /// Decode with this many rayon threads (None = rayon default).
    pub threads: Option<usize>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            truncate_max_range: true,
            linearize: false,
            empty_value: false,
            dataset: 1,
            ignore_text_offset: true,
            threads: None,
        }
    }
}

/// A parsed data set: keywords, parameters and the event matrix stored **column-major**
/// (`columns[p][event]`), which is what a per-channel long-format export wants.
#[derive(Debug, Clone)]
pub struct FcsData {
    pub version: String,
    pub text: IndexMap<String, String>,
    pub params: Vec<Param>,
    pub datatype: DataType,
    pub endian: Endian,
    pub n_events: usize,
    pub columns: Vec<Vec<f64>>,
    /// Number of data sets found in the file (via `$NEXTDATA`).
    pub n_datasets: usize,
    /// True when the DATA segment was shorter than the header/TEXT declared (file truncated) or
    /// held fewer whole rows than `$TOT`; `n_events` is then the number of complete rows read.
    pub truncated: bool,
}

impl FcsData {
    pub fn n_params(&self) -> usize {
        self.params.len()
    }
    pub fn keyword(&self, k: &str) -> Option<&str> {
        self.text.get(k).map(String::as_str)
    }
}

// ---------------------------------------------------------------------------------------------
// HEADER
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Header {
    text_start: usize,
    text_end: usize,
    data_start: usize,
    data_end: usize,
    #[allow(dead_code)]
    ana_start: usize,
    #[allow(dead_code)]
    ana_end: usize,
}

fn parse_offset(field: &[u8]) -> Result<usize> {
    let s = std::str::from_utf8(field)
        .map_err(|_| FcsError::Header("non-ASCII offset field".into()))?
        .trim();
    if s.is_empty() {
        return Ok(0); // blank = "see TEXT" (FCS 3.x large files) — fcsparser treats it as 0 too
    }
    match s.parse::<i64>() {
        Ok(v) if v >= 0 => Ok(v as usize),
        Ok(_) => Ok(0), // "-1" seen in BD FACSAria II headers → treat as "not given"
        Err(_) => Err(FcsError::Header(format!(
            "offset field {s:?} is not an integer"
        ))),
    }
}

fn read_header(bytes: &[u8], base: usize) -> Result<(String, Header)> {
    if bytes.len() < base + 58 {
        return Err(FcsError::Header(
            "file shorter than the 58-byte header".into(),
        ));
    }
    let h = &bytes[base..base + 58];
    let version = String::from_utf8_lossy(&h[0..6]).to_string();
    if !matches!(version.as_str(), "FCS2.0" | "FCS3.0" | "FCS3.1" | "FCS3.2") {
        return Err(FcsError::NotFcs(version));
    }
    // bytes 6..10 are four spaces; flowCore asserts, fcsparser skips — we skip.
    let f = |i: usize| parse_offset(&h[10 + 8 * i..18 + 8 * i]);
    let mut hd = Header {
        text_start: f(0)? + base,
        text_end: f(1)? + base,
        data_start: f(2)? + base,
        data_end: f(3)? + base,
        ana_start: f(4).unwrap_or(0) + base,
        ana_end: f(5).unwrap_or(0) + base,
    };
    // fcsparser quirk: some writers put text_end == data_start; the last TEXT byte is then the
    // first DATA byte. Shift text_end back by one so the delimiter check does not swallow data.
    if hd.text_end == hd.data_start && hd.data_start != base {
        hd.text_end -= 1;
    }
    if hd.text_start < base + 58 || hd.text_end <= hd.text_start || hd.text_end >= bytes.len() {
        return Err(FcsError::Header(format!(
            "TEXT offsets out of range: {}..{} (file {} bytes)",
            hd.text_start,
            hd.text_end,
            bytes.len()
        )));
    }
    Ok((version, hd))
}

// ---------------------------------------------------------------------------------------------
// TEXT
// ---------------------------------------------------------------------------------------------

/// Parse a delimited TEXT (or supplemental TEXT) segment into ordered keyword → value pairs.
///
/// FCS 3.1 §3.2.16: the first byte is the delimiter; a delimiter inside a key or value is written
/// twice. With `empty_value = false` (flowCore default for `read.FCS` is `TRUE`; the operator
/// passes `FALSE`) a doubled delimiter is always an escape. With `empty_value = true` a doubled
/// delimiter between two keywords is an empty value (Cytek/BD files use this occasionally).
/// Keys are trimmed and stored as written (flowCore uppercases nothing; `$` keys are already upper).
pub fn parse_text(raw: &[u8], empty_value: bool) -> Result<IndexMap<String, String>> {
    if raw.is_empty() {
        return Err(FcsError::Text("empty TEXT segment".into()));
    }
    let delim = raw[0];
    // Decode as latin-1 (flowCore: iconv(..., "latin1")); every byte maps to one char, so
    // positions are preserved and no byte is ever rejected.
    let s: String = raw.iter().map(|&b| b as char).collect();
    let d = delim as char;
    // Strip the leading delimiter; trailing whitespace; the trailing delimiter if present.
    let body = &s[1..];
    let body = body.trim_end_matches([' ', '\0']);
    let body = match body.strip_suffix(d) {
        Some(b) => b,
        None => body, // tolerate a missing final delimiter (fcsparser warns, flowCore tolerates)
    };
    // Tokenise honouring the doubling rule.
    let mut tokens: Vec<String> = Vec::new();
    let mut cur = String::new();
    let chars: Vec<char> = body.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == d {
            let next_is_delim = i + 1 < chars.len() && chars[i + 1] == d;
            if next_is_delim {
                if empty_value && tokens.len().is_multiple_of(2) {
                    // `cur` is a key and is followed by "//": an empty value (fcsparser-compatible
                    // reading; ambiguous with an escaped delimiter inside a key, which is rare).
                    tokens.push(std::mem::take(&mut cur));
                    tokens.push(String::new());
                    i += 2;
                    continue;
                }
                // escaped delimiter inside the current token (FCS 3.1 §3.2.16)
                cur.push(d);
                i += 2;
                continue;
            }
            tokens.push(std::mem::take(&mut cur));
            i += 1;
        } else {
            cur.push(c);
            i += 1;
        }
    }
    if !cur.is_empty() {
        tokens.push(cur);
    }
    if tokens.len() % 2 == 1 {
        // A dangling key with no value (seen in the wild); flowCore's C++ parser drops it.
        tokens.pop();
    }
    let mut map = IndexMap::with_capacity(tokens.len() / 2);
    let mut it = tokens.into_iter();
    while let (Some(k), Some(v)) = (it.next(), it.next()) {
        let k = k.trim().to_string();
        if k.is_empty() {
            continue;
        }
        map.insert(k, v);
    }
    Ok(map)
}

fn get<'a>(text: &'a IndexMap<String, String>, key: &str) -> Result<&'a str> {
    text.get(key)
        .map(String::as_str)
        .ok_or_else(|| FcsError::Text(format!("required keyword {key} missing")))
}

fn parse_num<T: std::str::FromStr>(s: &str, what: &str) -> Result<T> {
    s.trim()
        .parse::<T>()
        .map_err(|_| FcsError::Text(format!("{what} = {s:?} is not a number")))
}

fn parse_params(text: &IndexMap<String, String>) -> Result<Vec<Param>> {
    let n: usize = parse_num(get(text, "$PAR")?, "$PAR")?;
    // `$PAR` is read straight from the file and then drives both an allocation and the loop
    // below, so a corrupt or hostile value (`$PAR 4000000000`) would otherwise hang or abort the
    // container. Every parameter needs at least `$PnB` in TEXT, so the keyword count is a sound
    // upper bound and costs nothing on a valid file.
    if n > text.len() {
        return Err(FcsError::Text(format!(
            "$PAR is {n} but TEXT holds only {} keywords; the file is corrupt \
             (each parameter needs at least $PnB)",
            text.len()
        )));
    }
    let mut params = Vec::with_capacity(n);
    for i in 1..=n {
        let name = text
            .get(&format!("$P{i}N"))
            .cloned()
            .unwrap_or_else(|| format!("P{i}"));
        let desc = text.get(&format!("$P{i}S")).cloned().unwrap_or_default();
        let bits: u32 = parse_num(get(text, &format!("$P{i}B"))?, &format!("$P{i}B"))?;
        // $PnR may be "4194304", "262144.0" or even "1e+06" on some writers → parse as f64.
        let range: f64 = parse_num(get(text, &format!("$P{i}R"))?, &format!("$P{i}R"))?;
        let e = match text.get(&format!("$P{i}E")) {
            Some(v) => {
                let mut it = v.split(',');
                let a: f64 = parse_num(it.next().unwrap_or("0"), &format!("$P{i}E"))?;
                let b: f64 = parse_num(it.next().unwrap_or("0"), &format!("$P{i}E"))?;
                (a, b)
            }
            None => (0.0, 0.0),
        };
        let g: f64 = match text.get(&format!("$P{i}G")) {
            Some(v) if !v.trim().is_empty() => parse_num(v, &format!("$P{i}G"))?,
            _ => 1.0,
        };
        params.push(Param {
            index: i,
            name,
            desc,
            bits,
            range,
            e,
            g,
        });
    }
    Ok(params)
}

// ---------------------------------------------------------------------------------------------
// DATA
// ---------------------------------------------------------------------------------------------

fn decode_into(
    bytes: &[u8],
    params: &[Param],
    datatype: DataType,
    endian: Endian,
    n_events: usize,
    threads: Option<usize>,
) -> Result<Vec<Vec<f64>>> {
    let n_par = params.len();
    let sizes: Vec<usize> = params.iter().map(|p| (p.bits / 8) as usize).collect();
    let row_bytes: usize = sizes.iter().sum();
    if row_bytes == 0 {
        return Err(FcsError::Data("zero-width row".into()));
    }
    if bytes.len() < n_events * row_bytes {
        return Err(FcsError::Data(format!(
            "DATA segment has {} bytes but $TOT × row width needs {}",
            bytes.len(),
            n_events * row_bytes
        )));
    }
    match datatype {
        DataType::Float32 | DataType::Float64 => {
            let uniform = sizes.iter().all(|&s| s == sizes[0]);
            if !uniform {
                return Err(FcsError::Unsupported(
                    "numeric data with mixed $PnB widths".into(),
                ));
            }
            let w = sizes[0];
            match (datatype, w) {
                (DataType::Float32, 4) | (DataType::Float64, 8) => {}
                _ => {
                    return Err(FcsError::Data(format!(
                        "$DATATYPE {:?} with $PnB {} bits",
                        datatype,
                        w * 8
                    )));
                }
            }
        }
        DataType::Integer => {
            if sizes.iter().any(|&s| !(1..=8).contains(&s)) {
                return Err(FcsError::Unsupported(format!(
                    "integer $PnB widths {:?}",
                    params.iter().map(|p| p.bits).collect::<Vec<_>>()
                )));
            }
        }
    }
    // Per-parameter offsets within a row.
    let mut offs = Vec::with_capacity(n_par);
    let mut acc = 0usize;
    for s in &sizes {
        offs.push(acc);
        acc += s;
    }
    // Integer bit masks: values are masked to ceil(log2($PnR)) bits when that is narrower than
    // $PnB (flowCore `dat %% 2^usedBits`, fcsparser `& (2^bits - 1)`).
    let masks: Vec<Option<u64>> = params
        .iter()
        .map(|p| {
            if datatype != DataType::Integer || p.range <= 0.0 {
                return None;
            }
            let used = p.range.log2().ceil() as u32;
            if used < p.bits && used < 64 {
                Some((1u64 << used) - 1)
            } else {
                None
            }
        })
        .collect();

    // Column-major output; decode in row chunks in parallel, each producing its slice of every
    // column, then stitch. Chunk of 65,536 events keeps per-thread buffers small.
    const CHUNK: usize = 65_536;
    let n_chunks = n_events.div_ceil(CHUNK);
    let pool = match threads {
        Some(t) => Some(
            rayon::ThreadPoolBuilder::new()
                .num_threads(t)
                .build()
                .map_err(|e| FcsError::Data(e.to_string()))?,
        ),
        None => None,
    };
    let uniform_float = matches!(datatype, DataType::Float32 | DataType::Float64);
    let work = || -> Vec<Vec<Vec<f64>>> {
        (0..n_chunks)
            .into_par_iter()
            .map(|c| {
                let start = c * CHUNK;
                let end = ((c + 1) * CHUNK).min(n_events);
                let mut cols: Vec<Vec<f64>> = (0..n_par)
                    .map(|_| Vec::with_capacity(end - start))
                    .collect();
                if uniform_float {
                    // Fast path: fixed 4- or 8-byte cells, no masking. Walk the chunk once and push
                    // into each column; the compiler vectorises the byte→float conversion.
                    let w = sizes[0];
                    let slab = &bytes[start * row_bytes..end * row_bytes];
                    match (datatype, endian) {
                        (DataType::Float32, Endian::Little) => {
                            for row in slab.chunks_exact(row_bytes) {
                                for (p, cell) in row.chunks_exact(w).enumerate() {
                                    cols[p].push(f32::from_le_bytes([
                                        cell[0], cell[1], cell[2], cell[3],
                                    ]) as f64);
                                }
                            }
                        }
                        (DataType::Float32, Endian::Big) => {
                            for row in slab.chunks_exact(row_bytes) {
                                for (p, cell) in row.chunks_exact(w).enumerate() {
                                    cols[p].push(f32::from_be_bytes([
                                        cell[0], cell[1], cell[2], cell[3],
                                    ]) as f64);
                                }
                            }
                        }
                        (DataType::Float64, Endian::Little) => {
                            for row in slab.chunks_exact(row_bytes) {
                                for (p, cell) in row.chunks_exact(w).enumerate() {
                                    cols[p].push(f64::from_le_bytes(cell.try_into().unwrap()));
                                }
                            }
                        }
                        (DataType::Float64, Endian::Big) => {
                            for row in slab.chunks_exact(row_bytes) {
                                for (p, cell) in row.chunks_exact(w).enumerate() {
                                    cols[p].push(f64::from_be_bytes(cell.try_into().unwrap()));
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                    return cols;
                }
                for ev in start..end {
                    let row = &bytes[ev * row_bytes..(ev + 1) * row_bytes];
                    for p in 0..n_par {
                        let b = &row[offs[p]..offs[p] + sizes[p]];
                        let v = match datatype {
                            DataType::Float32 => match endian {
                                Endian::Little => LittleEndian::read_f32(b) as f64,
                                Endian::Big => BigEndian::read_f32(b) as f64,
                            },
                            DataType::Float64 => match endian {
                                Endian::Little => LittleEndian::read_f64(b),
                                Endian::Big => BigEndian::read_f64(b),
                            },
                            DataType::Integer => {
                                // Any width 1..=8 bytes (24-bit integers occur on Cytek xP5 files).
                                let u: u64 = match endian {
                                    Endian::Little => LittleEndian::read_uint(b, sizes[p]),
                                    Endian::Big => BigEndian::read_uint(b, sizes[p]),
                                };
                                let u = match masks[p] {
                                    Some(m) => u & m,
                                    None => u,
                                };
                                u as f64
                            }
                        };
                        cols[p].push(v);
                    }
                }
                cols
            })
            .collect()
    };
    let chunks: Vec<Vec<Vec<f64>>> = match &pool {
        Some(p) => p.install(work),
        None => work(),
    };
    let mut columns: Vec<Vec<f64>> = (0..n_par).map(|_| Vec::with_capacity(n_events)).collect();
    for ch in chunks {
        for (p, col) in ch.into_iter().enumerate() {
            columns[p].extend(col);
        }
    }
    Ok(columns)
}

fn apply_post(columns: &mut [Vec<f64>], params: &[Param], datatype: DataType, opts: &ReadOptions) {
    // flowCore order: truncate_max_range (values > $PnR → $PnR) then, if transformation,
    // PnE log→linear for integer data / PnG scaling. With linearize=false only the truncation runs.
    if opts.truncate_max_range {
        columns
            .par_iter_mut()
            .zip(params.par_iter())
            .for_each(|(col, p)| {
                let r = p.range;
                if r.is_finite() {
                    for v in col.iter_mut() {
                        if *v > r {
                            *v = r;
                        }
                    }
                }
            });
    }
    if opts.linearize {
        columns
            .par_iter_mut()
            .zip(params.par_iter())
            .for_each(|(col, p)| {
                let (dec, off) = p.e;
                if dec > 0.0 && datatype == DataType::Integer {
                    let off = if off == 0.0 { 1.0 } else { off };
                    let r = p.range;
                    for v in col.iter_mut() {
                        *v = 10f64.powf((*v / r) * dec) * off;
                    }
                } else if p.g != 1.0 && p.g > 0.0 {
                    for v in col.iter_mut() {
                        *v /= p.g;
                    }
                }
            });
    }
}

// ---------------------------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------------------------

/// Read an FCS file from disk (memory-mapped).
pub fn read_file(path: impl AsRef<Path>, opts: &ReadOptions) -> Result<FcsData> {
    let f = File::open(path)?;
    let mmap = unsafe { Mmap::map(&f)? };
    let out = read_bytes(&mmap, opts);
    // The decoded values are owned by `out`, so the mapped pages are dead weight from here. They
    // are page cache, which a cgroup charges to the operator's --memory limit, and this operator
    // maps every file twice (planning pass, then value pass). Hand them back (pagecache.rs).
    drop(mmap);
    crate::pagecache::release(&f);
    out
}

/// Read an FCS file already in memory.
pub fn read_bytes(bytes: &[u8], opts: &ReadOptions) -> Result<FcsData> {
    // Walk $NEXTDATA to find the requested data set and count them.
    let mut bases = vec![0usize];
    loop {
        let base = *bases.last().unwrap();
        let (_, hd) = read_header(bytes, base)?;
        let text = parse_text(&bytes[hd.text_start..=hd.text_end], opts.empty_value)?;
        let next: usize = text
            .get("$NEXTDATA")
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if next == 0 || base + next >= bytes.len() || bases.len() > 64 {
            break;
        }
        bases.push(base + next);
    }
    let n_datasets = bases.len();
    // flowCore's `dataset` is 1-based and falls back to the first when out of range with a
    // warning; the R operator passes dataset = 2. We follow: request beyond available → first.
    let base = *bases.get(opts.dataset).unwrap_or(&bases[0]);
    let (version, hd) = read_header(bytes, base)?;
    let mut text = parse_text(&bytes[hd.text_start..=hd.text_end], opts.empty_value)?;

    // Supplemental TEXT (FCS 3.x): merge, primary TEXT wins on conflicts.
    if let (Some(bs), Some(es)) = (
        text.get("$BEGINSTEXT").cloned(),
        text.get("$ENDSTEXT").cloned(),
    ) && let (Ok(bs), Ok(es)) = (bs.trim().parse::<usize>(), es.trim().parse::<usize>())
        && bs > 0
        && es > bs
        && base + es < bytes.len()
        && let Ok(sup) = parse_text(&bytes[base + bs..=base + es], opts.empty_value)
    {
        for (k, v) in sup {
            text.entry(k).or_insert(v);
        }
    }

    // DATA offsets: the HEADER wins (flowCore `ignore.text.offset = TRUE`, fcsparser likewise); TEXT
    // `$BEGINDATA`/`$ENDDATA` are used only when the header fields are 0 (FCS 3.x files > 99,999,999
    // bytes). Accuri C6 files carry TEXT offsets that disagree with the header — trusting them reads
    // garbage, which is how FR-FCM-ZZZ4 caught this.
    let (mut ds, mut de) = (hd.data_start, hd.data_end);
    let header_missing = hd.data_start == base || hd.data_end == base;
    if (header_missing || !opts.ignore_text_offset) && text.contains_key("$BEGINDATA") {
        let bd: usize = parse_num(get(&text, "$BEGINDATA")?, "$BEGINDATA")?;
        let ed: usize = parse_num(get(&text, "$ENDDATA")?, "$ENDDATA")?;
        if bd > 0 && ed >= bd {
            ds = base + bd;
            de = base + ed;
        }
    }
    if ds == base || de == base {
        return Err(FcsError::Data("no DATA offsets in header or TEXT".into()));
    }
    // Some writers (Accuri C6, Partec PAS) write $ENDDATA as an exclusive end equal to the file size;
    // truncated files (a BD compensation control in FR-FCM-ZZZ4) point past it. Clamp to what exists
    // and let the $TOT check below decide how many whole rows are usable (flowCore and fcsparser both
    // refuse such files; reading the available rows is the more useful behaviour for an import step).
    let mut truncated_file = false;
    if de >= bytes.len() {
        truncated_file = de > bytes.len();
        de = bytes.len() - 1;
    }
    if ds > de {
        return Err(FcsError::Data(format!(
            "DATA start {ds} after DATA end {de}"
        )));
    }

    let mode = get(&text, "$MODE")?.trim();
    if mode != "L" {
        return Err(FcsError::Unsupported(format!(
            "$MODE {mode} (only list mode)"
        )));
    }
    let datatype = match get(&text, "$DATATYPE")?.trim() {
        "F" => DataType::Float32,
        "D" => DataType::Float64,
        "I" => DataType::Integer,
        other => return Err(FcsError::Unsupported(format!("$DATATYPE {other}"))),
    };
    let endian = match get(&text, "$BYTEORD")?.trim() {
        "1,2,3,4" | "1,2" => Endian::Little,
        "4,3,2,1" | "2,1" => Endian::Big,
        other => return Err(FcsError::Unsupported(format!("$BYTEORD {other}"))),
    };
    let params = parse_params(&text)?;
    let n_events: usize = parse_num(get(&text, "$TOT")?, "$TOT")?;

    let data = &bytes[ds..=de];
    let row_bytes: usize = params.iter().map(|p| (p.bits / 8) as usize).sum();
    // Tolerate a DATA segment declared longer than $TOT rows (trailing padding) or slightly
    // shorter (flowCore warns "data may be truncated" and drops the partial row).
    let n_avail = if row_bytes > 0 {
        data.len() / row_bytes
    } else {
        0
    };
    let n_use = n_events.min(n_avail);
    let mut columns = decode_into(data, &params, datatype, endian, n_use, opts.threads)?;
    apply_post(&mut columns, &params, datatype, opts);

    Ok(FcsData {
        version,
        text,
        params,
        datatype,
        endian,
        n_events: n_use,
        columns,
        n_datasets,
        truncated: truncated_file || n_use < n_events,
    })
}

/// The seed a file's events are ranked with: the operator's `seed` mixed with the file name
/// (FNV-1a), so an event's rank depends on the file it is in and nothing else — not on which
/// other files share the archive or in what order.
pub fn file_seed(seed: u64, filename: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in filename.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    seed ^ h
}

/// Every event's rank in a seeded random order of the file: `ranks[i]` is the 1-based position
/// of event `i` in a ChaCha8 Fisher–Yates permutation. This is the `random_sequence` column, and
/// `which.lines = k` keeps exactly the events with rank ≤ k, so a filter on the column after a
/// full import selects the same cells a subsampled import would have kept.
pub fn event_ranks(n_events: usize, seed: u64) -> Vec<u32> {
    use rand::SeedableRng;
    use rand::seq::SliceRandom;
    let mut order: Vec<u32> = (0..n_events as u32).collect();
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
    order.shuffle(&mut rng);
    let mut ranks = vec![0u32; n_events];
    for (pos, &ev) in order.iter().enumerate() {
        ranks[ev as usize] = pos as u32 + 1;
    }
    ranks
}

/// Deterministic random subsample of event indices (sorted): the events whose rank is ≤ k,
/// the `which.lines` semantics of the R operator (`sample(nr, size = min(k, nr))`) but seeded —
/// the R version is unseeded — and consistent with `event_ranks` by construction.
pub fn sample_indices(n_events: usize, k: usize, seed: u64) -> Vec<usize> {
    let ranks = event_ranks(n_events, seed);
    (0..n_events)
        .filter(|&i| (ranks[i] as usize) <= k)
        .collect()
}

/// Convenience: spillover matrix keyword, if any (`$SPILLOVER` FCS 3.1, `SPILL`/`$COMP` legacy).
pub fn spillover_keyword(text: &IndexMap<String, String>) -> Option<(&str, &str)> {
    for k in ["$SPILLOVER", "SPILL", "SPILLOVER", "$COMP", "COMP"] {
        if let Some(v) = text.get(k) {
            return Some((k, v.as_str()));
        }
    }
    None
}

/// A parsed spillover (compensation) matrix: `n` detector names and an `n x n` row-major matrix.
#[derive(Debug, Clone, PartialEq)]
pub struct Spillover {
    /// Keyword it came from (`$SPILLOVER`, `SPILL`, …) — flowCore checks the same set.
    pub keyword: String,
    pub names: Vec<String>,
    /// Row-major, `names.len()` squared.
    pub values: Vec<f64>,
}

impl Spillover {
    pub fn n(&self) -> usize {
        self.names.len()
    }
    pub fn get(&self, row: usize, col: usize) -> f64 {
        self.values[row * self.n() + col]
    }
}

/// Parse the spillover keyword: `n,name1,…,nameN,v11,v12,…,vNN` (FCS 3.1 §3.2.22).
///
/// Returns `None` rather than an error for anything malformed — a matrix the operator cannot read
/// must not fail an import whose point is the event data, and flowCore likewise refuses some files
/// on a size mismatch. `params` is used for the IntelliCyt iQue3 case the R operator special-cases,
/// where the names are 1-based channel indices rather than detector names.
pub fn parse_spillover(text: &IndexMap<String, String>, params: &[Param]) -> Option<Spillover> {
    let (keyword, raw) = spillover_keyword(text)?;
    let tok: Vec<&str> = raw.split(',').map(str::trim).collect();
    let n: usize = tok.first()?.parse().ok()?;
    if n == 0 || tok.len() != 1 + n + n * n {
        return None; // size mismatch: the same files flowCore rejects
    }
    let mut names: Vec<String> = tok[1..=n].iter().map(|s| s.to_string()).collect();
    // iQue3 writes channel numbers here instead of names; the R operator maps them onto the
    // parameter names, so the output is readable rather than "1", "2", "3".
    if names.iter().all(|s| s.chars().all(|c| c.is_ascii_digit())) {
        let mapped: Option<Vec<String>> = names
            .iter()
            .map(|s| {
                s.parse::<usize>()
                    .ok()
                    .and_then(|i| params.get(i.checked_sub(1)?))
                    .map(|p| p.name.clone())
            })
            .collect();
        if let Some(m) = mapped {
            names = m;
        }
    }
    let values: Option<Vec<f64>> = tok[1 + n..].iter().map(|s| s.parse::<f64>().ok()).collect();
    Some(Spillover {
        keyword: keyword.to_string(),
        names,
        values: values?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_escaped_delimiter() {
        let raw = b"/$PAR/2/$P1N/FSC//A/$P2N/SSC-A/";
        let m = parse_text(raw, false).unwrap();
        assert_eq!(m["$P1N"], "FSC/A");
        assert_eq!(m["$P2N"], "SSC-A");
    }

    #[test]
    fn text_empty_value_mode() {
        let raw = b"|$PAR|1|$P1S||$P1N|X|";
        let m = parse_text(raw, true).unwrap();
        assert_eq!(m["$P1S"], "");
        assert_eq!(m["$P1N"], "X");
    }

    /// Every one of these is something a user can upload. The release profile is
    /// `panic = abort`, so any of them panicking would kill the operator container with no
    /// message the user could act on; they must all come back as errors.
    #[test]
    fn corrupt_files_error_rather_than_panic() {
        let opts = ReadOptions::default();
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("empty", vec![]),
            (
                "not fcs",
                b"hello world, definitely not a cytometry file".to_vec(),
            ),
            ("header only", b"FCS3.1    ".to_vec()),
            ("truncated header", {
                let mut v = b"FCS3.1    ".to_vec();
                v.extend(std::iter::repeat_n(b' ', 20));
                v
            }),
            ("absurd $PAR", {
                // a well-formed header whose TEXT claims four billion parameters: this used to
                // drive both a Vec::with_capacity and a 4e9-iteration loop
                let text = b"/$PAR/4000000000/$TOT/1/$DATATYPE/F/$BYTEORD/1,2,3,4/$MODE/L/";
                let mut v = Vec::new();
                v.extend_from_slice(b"FCS3.1    ");
                let ts = 58usize;
                let te = ts + text.len() - 1;
                for x in [ts, te, 0usize, 0usize, 0usize, 0usize] {
                    v.extend_from_slice(format!("{x:>8}").as_bytes());
                }
                v.resize(ts, b' ');
                v.extend_from_slice(text);
                v.extend_from_slice(&[0u8; 64]);
                v
            }),
        ];
        for (name, bytes) in cases {
            match read_bytes(&bytes, &opts) {
                Err(e) => {
                    let msg = e.to_string();
                    assert!(!msg.is_empty(), "{name}: empty error message");
                }
                Ok(d) => panic!("{name}: expected an error, got {} events", d.n_events),
            }
        }
    }

    /// Truncating a real file at any point must error or return the whole rows that survived —
    /// never panic, and never invent events.
    #[test]
    fn truncation_at_any_offset_is_safe() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fcs_test.zip");
        let dir = tempfile::tempdir().unwrap();
        crate::download::extract_fcs_entries(std::path::Path::new(path), dir.path()).unwrap();
        let f = std::fs::read_dir(dir.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let full = std::fs::read(&f).unwrap();
        let opts = ReadOptions::default();
        let whole = read_bytes(&full, &opts).unwrap();
        for frac in [1, 2, 3, 5, 8, 13, 21, 34, 55, 89] {
            let cut = full.len() * frac / 100;
            // an error is the other acceptable outcome
            if let Ok(d) = read_bytes(&full[..cut], &opts) {
                assert!(
                    d.n_events <= whole.n_events,
                    "truncating to {frac}% produced {} events, more than the whole file's {}",
                    d.n_events,
                    whole.n_events
                );
            }
        }
    }

    #[test]
    fn spillover_parses_the_fcs_layout() {
        // n, then n names, then the n x n matrix row-major (FCS 3.1 section 3.2.22)
        let mut t = IndexMap::new();
        t.insert(
            "$SPILLOVER".to_string(),
            "3,A,B,C,1,0.1,0,0,1,0.2,0,0,1".to_string(),
        );
        let sp = parse_spillover(&t, &[]).expect("parses");
        assert_eq!(sp.names, ["A", "B", "C"]);
        assert_eq!(sp.n(), 3);
        assert_eq!(sp.get(0, 0), 1.0);
        assert_eq!(sp.get(0, 1), 0.1);
        assert_eq!(sp.get(1, 2), 0.2);
        assert_eq!(sp.get(2, 0), 0.0);

        // a size mismatch is ignored rather than fatal: flowCore refuses such files outright,
        // and an unreadable matrix must not fail an import of the event data
        let mut bad = IndexMap::new();
        bad.insert("$SPILLOVER".to_string(), "3,A,B,C,1,0,0".to_string());
        assert!(parse_spillover(&bad, &[]).is_none());
        let mut zero = IndexMap::new();
        zero.insert("$SPILLOVER".to_string(), "0".to_string());
        assert!(parse_spillover(&zero, &[]).is_none());

        // IntelliCyt iQue3 writes 1-based channel numbers; the R operator maps them to names
        let params: Vec<Param> = ["FSC-A", "SSC-A", "FITC-A"]
            .iter()
            .enumerate()
            .map(|(i, n)| Param {
                index: i + 1,
                name: n.to_string(),
                desc: String::new(),
                bits: 32,
                range: 1024.0,
                e: (0.0, 0.0),
                g: 1.0,
            })
            .collect();
        let mut ique = IndexMap::new();
        ique.insert("$SPILLOVER".to_string(), "2,1,3,1,0.05,0,1".to_string());
        let sp = parse_spillover(&ique, &params).expect("parses");
        assert_eq!(sp.names, ["FSC-A", "FITC-A"]);
    }

    /// Real files: the matrix read back must equal the numbers in the keyword itself.
    #[test]
    fn spillover_matches_the_keyword_on_real_files() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
        if !root.exists() {
            return; // fixtures are git-ignored; skip where they are absent
        }
        let opts = ReadOptions::default();
        let mut checked = 0;
        let mut files = Vec::new();
        for d in std::fs::read_dir(&root).unwrap().filter_map(|e| e.ok()) {
            if !d.path().is_dir() {
                continue;
            }
            for e in std::fs::read_dir(d.path()).unwrap().filter_map(|e| e.ok()) {
                files.push(e.path());
            }
        }
        for p in files {
            let ext = p.extension().and_then(|s| s.to_str()).unwrap_or("");
            if !ext.eq_ignore_ascii_case("fcs") && !ext.eq_ignore_ascii_case("lmd") {
                continue;
            }
            let Ok(d) = read_file(&p, &opts) else {
                continue;
            };
            let Some((_, raw)) = spillover_keyword(&d.text) else {
                continue;
            };
            let Some(sp) = parse_spillover(&d.text, &d.params) else {
                continue; // size mismatch: deliberately ignored
            };
            let tok: Vec<&str> = raw.split(',').map(str::trim).collect();
            let n: usize = tok[0].parse().unwrap();
            assert_eq!(sp.n(), n, "{p:?}");
            assert_eq!(sp.values.len(), n * n, "{p:?}");
            for (k, want) in tok[1 + n..].iter().enumerate() {
                let want: f64 = want.parse().unwrap();
                assert!(
                    (sp.values[k] - want).abs() < 1e-12,
                    "{p:?} cell {k}: {} != {want}",
                    sp.values[k]
                );
            }
            checked += 1;
        }
        assert!(checked > 0, "no compensated fixture was checked");
        eprintln!("spillover verified on {checked} fixtures");
    }

    #[test]
    fn sample_is_deterministic_and_sorted() {
        let a = sample_indices(1000, 10, 42);
        let b = sample_indices(1000, 10, 42);
        assert_eq!(a, b);
        assert!(a.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(sample_indices(5, 10, 1), vec![0, 1, 2, 3, 4]);
    }
}
