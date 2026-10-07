//! Download the documentId's bytes via `FileService.download` (streamed to disk) and expand it
//! into a list of FCS file paths.
//!
//! R (`utils.R::download_files` + `prepare_files`): a `.zip` document (by extension, or by mime
//! sniffing when there is no extension) is unzipped and every `*.fcs|*.lmd` entry (case-insensitive,
//! recursive) is read, in `list.files` order; any other document is treated as a single FCS file.
//! Here the archive is detected by its magic bytes, so a mis-named zip also works.
//!
//! Two deliberate extensions beyond R, both from real uploads:
//!
//! * **Archives inside archives are expanded** (to [`MAX_ARCHIVE_DEPTH`]). A user who zips the
//!   folder that holds their zip of FCS files gets what they meant, rather than "contains no
//!   .fcs/.lmd entries". R's `unzip` leaves the inner archive as a file and finds nothing.
//! * **macOS metadata entries are ignored.** Compressing a folder in Finder adds `__MACOSX/…/._name`
//!   AppleDouble entries that carry the original extension, so `._sample.fcs` looks like an FCS file
//!   to any extension test and is not one.
use anyhow::{Context, Result, anyhow, bail};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tercen_rs::context::ContextBase;
use tonic::Request;

/// The downloaded document, expanded.
pub struct Downloaded {
    /// Name of the FileDocument in Tercen (used in log lines and the summary).
    pub doc_name: String,
    /// FCS files to read, sorted by path.
    pub files: Vec<PathBuf>,
    pub bytes: u64,
}

pub async fn fetch_fcs_files(
    ctx: &ContextBase,
    doc_id: &str,
    work_root: &Path,
) -> Result<Downloaded> {
    std::fs::create_dir_all(work_root)
        .with_context(|| format!("create {}", work_root.display()))?;
    let doc_name = file_document_name(ctx, doc_id).await.unwrap_or_else(|e| {
        tracing::warn!("could not read FileDocument name for {doc_id}: {e}");
        doc_id.to_string()
    });
    tracing::info!(doc_id, doc_name, "downloading document");
    let archive = work_root.join("document.bin");
    let bytes = stream_file_to(ctx, doc_id, &archive).await?;
    tracing::info!(bytes, "download complete");

    let mut magic = [0u8; 4];
    let n = std::fs::File::open(&archive)?.read(&mut magic)?;
    let is_zip =
        n >= 4 && (&magic == b"PK\x03\x04" || &magic == b"PK\x05\x06" || &magic == b"PK\x07\x08");

    let files = if is_zip {
        let dest = work_root.join("extracted");
        std::fs::create_dir_all(&dest)?;
        let report = extract_fcs_entries(&archive, &dest)
            .with_context(|| format!("unzip document {doc_name}"))?;
        let _ = std::fs::remove_file(&archive);
        if report.archives > 0 {
            tracing::info!(
                archives = report.archives,
                "expanded nested archive(s) inside the document"
            );
        }
        let mut v = Vec::new();
        walk(&dest, &mut |p| {
            if is_fcs_name(p) && !is_mac_metadata(p) {
                v.push(p.to_path_buf());
            }
        })?;
        v.sort();
        if v.is_empty() {
            bail!(
                "the zip document '{doc_name}' contains no .fcs/.lmd entries{}",
                report.explain()
            );
        }
        v
    } else {
        if n < 3 || &magic[..3] != b"FCS" {
            tracing::warn!(
                "document '{doc_name}' is neither a zip nor starts with 'FCS'; trying to read it as FCS anyway"
            );
        }
        let single = work_root.join(sanitize(&doc_name));
        std::fs::rename(&archive, &single)?;
        vec![single]
    };
    Ok(Downloaded {
        doc_name,
        files,
        bytes,
    })
}

fn sanitize(name: &str) -> String {
    let base = Path::new(name)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("document.fcs");
    if base.is_empty() {
        "document.fcs".to_string()
    } else {
        base.to_string()
    }
}

pub fn is_fcs_name(p: &Path) -> bool {
    p.extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("fcs") || e.eq_ignore_ascii_case("lmd"))
        .unwrap_or(false)
}

fn walk(dir: &Path, f: &mut impl FnMut(&Path)) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read dir {}", dir.display()))? {
        let p = entry?.path();
        if p.is_dir() {
            walk(&p, f)?;
        } else {
            f(&p);
        }
    }
    Ok(())
}

async fn file_document_name(ctx: &ContextBase, doc_id: &str) -> Result<String> {
    use tercen_rs::client::proto::{GetRequest, e_file_document};
    let mut fs = ctx
        .client()
        .file_service()
        .map_err(|e| anyhow!("file service: {e}"))?;
    let doc = fs
        .get(Request::new(GetRequest {
            id: doc_id.to_string(),
            ..Default::default()
        }))
        .await
        .map_err(|e| anyhow!("file_service.get({doc_id}): {e}"))?
        .into_inner();
    match doc.object {
        Some(e_file_document::Object::Filedocument(fd)) => Ok(fd.name),
        None => bail!("EFileDocument has no object"),
    }
}

/// Stream `FileService::download` to `dest`; RAM use is one gRPC chunk.
async fn stream_file_to(ctx: &ContextBase, doc_id: &str, dest: &Path) -> Result<u64> {
    use tercen_rs::client::proto::ReqDownload;
    let mut fs = ctx
        .client()
        .file_service()
        .map_err(|e| anyhow!("file service: {e}"))?;
    let mut stream = fs
        .download(Request::new(ReqDownload {
            file_document_id: doc_id.to_string(),
        }))
        .await
        .map_err(|e| anyhow!("file_service.download({doc_id}): {e}"))?
        .into_inner();
    let mut out = std::io::BufWriter::with_capacity(4 << 20, std::fs::File::create(dest)?);
    let mut total = 0u64;
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|e| anyhow!("download stream: {e}"))?
    {
        out.write_all(&chunk.result)?;
        total += chunk.result.len() as u64;
    }
    out.flush()?;
    if total == 0 {
        bail!("documentId {doc_id} download returned 0 bytes");
    }
    Ok(total)
}

/// How deep to descend into archives nested inside archives. One or two levels is a user who
/// zipped the folder containing their zip; beyond a few it is a mistake or a zip bomb.
pub const MAX_ARCHIVE_DEPTH: usize = 4;

/// What an extraction found, so a failure can say something more useful than "no FCS files".
#[derive(Debug, Default)]
pub struct ExtractReport {
    /// FCS/LMD entries written to disk.
    pub fcs: usize,
    /// Nested archives opened.
    pub archives: usize,
    /// Nested archives left unopened because of [`MAX_ARCHIVE_DEPTH`].
    pub too_deep: usize,
    /// Lower-case extensions of everything else, for the error message.
    pub other: std::collections::BTreeSet<String>,
}

impl ExtractReport {
    /// A human-readable tail for "no FCS files here" — what was there instead.
    pub fn explain(&self) -> String {
        let mut parts = Vec::new();
        if self.too_deep > 0 {
            parts.push(format!(
                "{} nested archive(s) more than {MAX_ARCHIVE_DEPTH} levels deep were not opened",
                self.too_deep
            ));
        }
        if !self.other.is_empty() {
            let mut exts: Vec<&str> = self.other.iter().map(|s| s.as_str()).take(6).collect();
            if self.other.len() > exts.len() {
                exts.push("…");
            }
            parts.push(format!("it holds files of type: {}", exts.join(", ")));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(" ({})", parts.join("; "))
        }
    }
}

/// True for the metadata Finder adds when compressing a folder. These carry the original
/// extension, so `__MACOSX/run1/._sample.fcs` passes every extension test and is not an FCS file.
fn is_mac_metadata(p: &Path) -> bool {
    p.components().any(|c| c.as_os_str() == "__MACOSX")
        || p.file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|n| n.starts_with("._"))
}

/// Extract the FCS/LMD entries of a zip, descending into nested archives (streaming each entry;
/// zip-slip refused).
pub fn extract_fcs_entries(archive: &Path, dest: &Path) -> Result<ExtractReport> {
    let mut report = ExtractReport::default();
    extract_into(archive, dest, 0, &mut report)?;
    Ok(report)
}

fn extract_into(archive: &Path, dest: &Path, depth: usize, out: &mut ExtractReport) -> Result<()> {
    let f = std::fs::File::open(archive)?;
    let mut z = zip::ZipArchive::new(f).context("open zip")?;
    for i in 0..z.len() {
        let mut e = z.by_index(i).with_context(|| format!("zip entry {i}"))?;
        if e.is_dir() {
            continue;
        }
        let name = e
            .enclosed_name()
            .ok_or_else(|| anyhow!("zip entry {i} has an invalid name"))?
            .to_path_buf();
        if is_mac_metadata(&name) {
            continue;
        }
        if is_fcs_name(&name) {
            write_entry(&mut e, &dest.join(&name))?;
            out.fcs += 1;
            continue;
        }
        // Anything else may still be an archive. Sniff rather than trust the extension: zips
        // arrive named .ZIP, .zipx, or with no extension at all.
        let mut head = [0u8; 4];
        let mut got = 0;
        while got < head.len() {
            match e.read(&mut head[got..])? {
                0 => break,
                k => got += k,
            }
        }
        let looks_like_zip = got == 4 && &head == b"PK\x03\x04";
        if !looks_like_zip {
            if let Some(ext) = name.extension().and_then(|s| s.to_str()) {
                out.other.insert(ext.to_ascii_lowercase());
            }
            continue;
        }
        if depth + 1 > MAX_ARCHIVE_DEPTH {
            out.too_deep += 1;
            tracing::warn!(
                entry = %name.display(),
                depth,
                "nested archive deeper than the limit; not opened"
            );
            continue;
        }
        // Stream the nested archive to disk (it can be large), expand it into a sibling directory
        // named after it, then drop it. The `.d` suffix cannot collide with an extracted FCS file.
        let nested = dest.join(&name);
        let mut reader = (&head[..got]).chain(&mut e);
        write_reader(&mut reader, &nested)?;
        let sub = nested.with_file_name(format!(
            "{}.d",
            nested
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("archive")
        ));
        std::fs::create_dir_all(&sub)?;
        out.archives += 1;
        tracing::info!(entry = %name.display(), depth = depth + 1, "expanding nested archive");
        let r = extract_into(&nested, &sub, depth + 1, out);
        let _ = std::fs::remove_file(&nested);
        r.with_context(|| format!("nested archive '{}'", name.display()))?;
    }
    Ok(())
}

fn write_entry(e: &mut zip::read::ZipFile<'_>, out: &Path) -> Result<()> {
    write_reader(e, out)
}

fn write_reader(src: &mut impl Read, out: &Path) -> Result<()> {
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let f = std::fs::File::create(out)?;
    {
        let mut w = std::io::BufWriter::with_capacity(1 << 20, &f);
        std::io::copy(src, &mut w)?;
        w.flush()?;
    }
    // The extracted set is as large as the archive; hand each file back to the kernel so the
    // cgroup does not accumulate it (pagecache.rs).
    crate::pagecache::release(&f);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fcs_test.zip");

    #[test]
    fn extracts_only_fcs_entries_case_insensitively() {
        let dir = tempfile::tempdir().unwrap();
        let n = extract_fcs_entries(Path::new(FIXTURE), dir.path()).unwrap();
        assert_eq!(n.fcs, 1);
        let mut v = Vec::new();
        walk(dir.path(), &mut |p| v.push(p.to_path_buf())).unwrap();
        assert_eq!(v.len(), 1);
        assert!(is_fcs_name(&v[0]));
        assert!(is_fcs_name(Path::new("a/b.LMD")));
        assert!(!is_fcs_name(Path::new("a/b.csv")));
    }

    /// Wrap `payload` as a single entry named `entry` in a new zip at `at`.
    fn wrap(at: &Path, entry: &str, payload: &[u8]) {
        let f = std::fs::File::create(at).unwrap();
        let mut zw = zip::ZipWriter::new(f);
        let opts: zip::write::FileOptions = zip::write::FileOptions::default();
        zw.start_file(entry, opts).unwrap();
        zw.write_all(payload).unwrap();
        zw.finish().unwrap();
    }

    fn fcs_paths(dir: &Path) -> Vec<PathBuf> {
        let mut v = Vec::new();
        walk(dir, &mut |p| {
            if is_fcs_name(p) {
                v.push(p.to_path_buf())
            }
        })
        .unwrap();
        v.sort();
        v
    }

    #[test]
    fn a_zip_of_a_zip_two_folders_down_is_expanded() {
        // What a user produced by zipping the folder that held their zip of FCS files:
        //   folder1.zip → folder1/folder2/inner.zip → *.fcs
        let dir = tempfile::tempdir().unwrap();
        let inner = std::fs::read(FIXTURE).unwrap();
        let outer = dir.path().join("folder1.zip");
        wrap(&outer, "folder1/folder2/inner.zip", &inner);

        let dest = dir.path().join("out");
        std::fs::create_dir_all(&dest).unwrap();
        let r = extract_fcs_entries(&outer, &dest).unwrap();
        assert_eq!(r.fcs, 1, "the FCS file inside the inner zip was not found");
        assert_eq!(r.archives, 1);
        assert_eq!(r.too_deep, 0);
        let found = fcs_paths(&dest);
        assert_eq!(found.len(), 1);
        // the inner archive itself must not survive as a file
        assert!(!dest.join("folder1/folder2/inner.zip").exists());
    }

    #[test]
    fn nesting_beyond_the_limit_is_reported_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        let mut payload = std::fs::read(FIXTURE).unwrap();
        for i in 0..(MAX_ARCHIVE_DEPTH + 2) {
            let at = dir.path().join(format!("layer{i}.zip"));
            wrap(&at, &format!("layer{i}/inner.zip"), &payload);
            payload = std::fs::read(&at).unwrap();
        }
        let outer = dir
            .path()
            .join(format!("layer{}.zip", MAX_ARCHIVE_DEPTH + 1));
        let dest = dir.path().join("out");
        std::fs::create_dir_all(&dest).unwrap();
        let r = extract_fcs_entries(&outer, &dest).unwrap();
        assert_eq!(r.fcs, 0);
        assert_eq!(r.too_deep, 1);
        assert!(
            r.explain().contains("deep"),
            "the message should say why nothing was found: {}",
            r.explain()
        );
    }

    #[test]
    fn mac_metadata_is_not_mistaken_for_fcs() {
        // Finder writes __MACOSX/<dir>/._<name>, which keeps the .fcs extension and is not FCS.
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("mac.zip");
        {
            let f = std::fs::File::create(&archive).unwrap();
            let mut zw = zip::ZipWriter::new(f);
            let opts: zip::write::FileOptions = zip::write::FileOptions::default();
            zw.start_file("run1/sample.fcs", opts).unwrap();
            zw.write_all(b"FCS3.0    ").unwrap();
            zw.start_file("__MACOSX/run1/._sample.fcs", opts).unwrap();
            zw.write_all(b"\x00\x05\x16\x07not an fcs file").unwrap();
            zw.finish().unwrap();
        }
        let dest = dir.path().join("out");
        std::fs::create_dir_all(&dest).unwrap();
        let r = extract_fcs_entries(&archive, &dest).unwrap();
        assert_eq!(r.fcs, 1, "only the real file should be extracted");
        let found = fcs_paths(&dest);
        assert_eq!(found.len(), 1);
        assert!(found[0].ends_with("run1/sample.fcs"));
    }

    #[test]
    fn a_zip_with_no_fcs_says_what_it_found_instead() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("docs.zip");
        {
            let f = std::fs::File::create(&archive).unwrap();
            let mut zw = zip::ZipWriter::new(f);
            let opts: zip::write::FileOptions = zip::write::FileOptions::default();
            zw.start_file("a/readme.txt", opts).unwrap();
            zw.write_all(b"hello").unwrap();
            zw.start_file("a/annotations.csv", opts).unwrap();
            zw.write_all(b"x,y").unwrap();
            zw.finish().unwrap();
        }
        let dest = dir.path().join("out");
        std::fs::create_dir_all(&dest).unwrap();
        let r = extract_fcs_entries(&archive, &dest).unwrap();
        assert_eq!(r.fcs, 0);
        let msg = r.explain();
        assert!(msg.contains("csv") && msg.contains("txt"), "{msg}");
    }

    /// Point `RFCS_TEST_ZIP` at a real upload to check it end to end:
    /// `RFCS_TEST_ZIP=/path/to/user.zip cargo test extraction_of_a_real_archive -- --nocapture`
    #[test]
    fn extraction_of_a_real_archive() {
        let Ok(path) = std::env::var("RFCS_TEST_ZIP") else {
            return; // nothing to check on a normal run
        };
        let dir = tempfile::tempdir().unwrap();
        let r = extract_fcs_entries(Path::new(&path), dir.path()).unwrap();
        let found = fcs_paths(dir.path());
        println!("{path}: {r:?}\n  {} FCS file(s):", found.len());
        for f in &found {
            println!("    {}", f.strip_prefix(dir.path()).unwrap().display());
        }
        assert!(r.fcs > 0, "no FCS files found in {path}");
    }

    #[test]
    fn zip_slip_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("evil.zip");
        {
            let f = std::fs::File::create(&archive).unwrap();
            let mut zw = zip::ZipWriter::new(f);
            let opts: zip::write::FileOptions = zip::write::FileOptions::default();
            zw.start_file("../escaped.fcs", opts).unwrap();
            zw.write_all(b"FCS3.0").unwrap();
            zw.finish().unwrap();
        }
        let dest = dir.path().join("x");
        std::fs::create_dir_all(&dest).unwrap();
        assert!(extract_fcs_entries(&archive, &dest).is_err());
        assert!(!dir.path().join("escaped.fcs").exists());
    }
}
