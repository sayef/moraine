//! The sidecar bundle format, `.osix` (D33).
//!
//! One format wraps every index kind, so validation, caching and collection are shared. All
//! integers are little-endian.
//!
//! ```text
//! offset  size  field
//! 0       4     magic "OSIX"
//! 4       2     bundle format version (1)
//! 6       2     flags (0; readers reject unknown bits)
//! 8       8     kind name, ASCII, NUL-padded
//! 16      8     spec hash (16 hex characters as 8 raw bytes)
//! 24      8     row count of the source file
//! 32      8     size in bytes of the source file
//! 40      8     indexed row count
//! 48      4     section count
//! 52      4     kind format version
//! 56      8     reserved (0)
//! 64      ...   sections, each starting at a multiple of 4,096
//! T       64*n  section table: name [u8; 32] NUL-padded, offset u64, length u64, xxh3-64 u64,
//!               reserved u64
//! end-24  24    footer: table offset u64, xxh3-64 of header + table u64, reserved u32, "XISO"
//! ```
//!
//! The writer streams sections to any `Write + Seek` (a temporary file for builds, memory for
//! small bundles) and patches the header last, so a builder never holds more than its current
//! write buffer. The reader validates structure on open; section checksums are verified once,
//! when the cache installs a downloaded bundle.

use std::io::{self, Seek, SeekFrom, Write};
use std::sync::Arc;

use memmap2::Mmap;
use xxhash_rust::xxh3::{Xxh3, xxh3_64};

/// Magic at the start of a bundle.
pub(crate) const MAGIC: &[u8; 4] = b"OSIX";
/// Magic at the end of a bundle.
pub(crate) const END_MAGIC: &[u8; 4] = b"XISO";
/// The bundle format version this release writes and reads.
pub(crate) const BUNDLE_VERSION: u16 = 1;
/// Sections start at multiples of this, so they can be mapped and used in place.
pub(crate) const ALIGN: u64 = 4096;
const HEADER: usize = 64;
const ENTRY: usize = 64;
const FOOTER: usize = 24;
/// Longest section name.
pub(crate) const MAX_SECTION_NAME: usize = 32;
/// Longest kind name.
pub(crate) const MAX_KIND_NAME: usize = 8;
/// Sections a bundle may have; more is treated as corrupt.
const MAX_SECTIONS: u32 = 65_536;

/// Why a bundle was refused. Every variant means "treat the sidecar as missing".
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BundleError {
    /// The bytes end before the structure they announce.
    #[error("bundle truncated or too short ({0} bytes)")]
    Truncated(u64),
    /// The bytes do not start or end with the bundle magic.
    #[error("not a bundle: bad magic")]
    BadMagic,
    /// A bundle format version, flag or kind format version this release does not read.
    #[error("unsupported bundle: {0}")]
    Unsupported(String),
    /// The bundle is of another index kind.
    #[error("bundle is of kind {found:?}, expected {expected:?}")]
    KindMismatch {
        /// The kind in the header.
        found: String,
        /// The kind the reader expected.
        expected: String,
    },
    /// The bundle was built for another spec.
    #[error("bundle spec hash {found}, expected {expected}")]
    SpecHashMismatch {
        /// The spec hash in the header.
        found: String,
        /// The spec hash the reader expected.
        expected: String,
    },
    /// Source row count or size differ from the data file's `add` action.
    #[error("bundle was built from another file: {0}")]
    SourceMismatch(String),
    /// The structure is inconsistent (offsets, lengths, names).
    #[error("corrupt bundle: {0}")]
    Corrupt(String),
    /// A checksum does not match: of a section, or of the footer.
    #[error("bundle checksum mismatch in {0}")]
    Checksum(String),
    /// Reading or mapping the bundle failed.
    #[error("bundle i/o: {0}")]
    Io(#[source] std::sync::Arc<io::Error>),
}

impl From<io::Error> for BundleError {
    fn from(e: io::Error) -> Self {
        BundleError::Io(std::sync::Arc::new(e))
    }
}

/// What the fixed header says.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct BundleHeader {
    /// The bundle format version.
    pub version: u16,
    /// Format flags (none are defined in version 1).
    pub flags: u16,
    /// The index kind that wrote the bundle.
    pub kind: String,
    /// The spec hash: 16 lowercase hex characters.
    pub spec_hash: String,
    /// Rows of the data file the bundle was built from.
    pub source_rows: u64,
    /// Size in bytes of that data file.
    pub source_size: u64,
    /// Rows the kind indexed (the others are skipped by every route alike).
    pub indexed_rows: u64,
    /// The kind's own format version.
    pub kind_format: u32,
}

/// One entry of the section table.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SectionEntry {
    /// The section's name, given by the kind.
    pub name: String,
    /// Offset of the section in the bundle, 4,096-aligned.
    pub offset: u64,
    /// Length of the section in bytes.
    pub length: u64,
    /// XXH3 of the section's bytes.
    pub checksum: u64,
}

/// What a reader expects of a bundle before using it.
#[derive(Clone, Debug)]
pub struct Expect<'a> {
    /// The index kind.
    pub kind: &'a str,
    /// The kind's format version this client reads.
    pub kind_format: u32,
    /// The spec hash of the index.
    pub spec_hash: &'a str,
    /// The source file's row count, when the snapshot knows it.
    pub source_rows: Option<u64>,
    /// The source file's size in bytes.
    pub source_size: u64,
}

/// The bytes of a bundle, page-aligned: a mapped cache file, or an anonymous mapping holding a
/// copy (memory cache mode). Sections are therefore 4,096-aligned in memory too.
#[derive(Clone)]
pub struct BundleBytes {
    map: Arc<Mmap>,
    /// Logical length (an anonymous mapping cannot be empty).
    len: usize,
}

impl std::fmt::Debug for BundleBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BundleBytes({} bytes)", self.len)
    }
}

impl BundleBytes {
    /// Maps `file` read-only.
    ///
    /// The file must not change while mapped: cache entries are written once, renamed into
    /// place and only ever deleted (on Unix a deleted file stays valid while mapped).
    pub fn map(file: &std::fs::File) -> io::Result<BundleBytes> {
        if file.metadata()?.len() == 0 {
            return Self::copy(&[]);
        }
        // SAFETY: cache files are immutable once installed (written to a temporary directory,
        // renamed, never rewritten); see the cache rules (cases K1 to K8).
        let map = unsafe { Mmap::map(file)? };
        let len = map.len();
        Ok(BundleBytes {
            map: Arc::new(map),
            len,
        })
    }

    /// Copies `bytes` into a fresh page-aligned anonymous mapping.
    pub fn copy(bytes: &[u8]) -> io::Result<BundleBytes> {
        let mut m = memmap2::MmapMut::map_anon(bytes.len().max(1))?;
        m[..bytes.len()].copy_from_slice(bytes);
        Ok(BundleBytes {
            map: Arc::new(m.make_read_only()?),
            len: bytes.len(),
        })
    }

    /// The bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.map[..self.len]
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether there are no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// A validated bundle: header, section table and the bytes.
#[derive(Clone, Debug)]
pub struct BundleView {
    header: BundleHeader,
    sections: Vec<SectionEntry>,
    bytes: BundleBytes,
}

impl BundleView {
    /// Parses and checks a bundle's structure (both magics, versions, flags, the section table,
    /// the footer checksum). Section checksums are not verified here; see
    /// [`BundleView::verify_sections`].
    pub fn parse(bytes: BundleBytes) -> Result<BundleView, BundleError> {
        let (header, sections) = parse_structure(bytes.as_slice())?;
        Ok(BundleView {
            header,
            sections,
            bytes,
        })
    }

    /// [`BundleView::parse`] plus the checks against what the snapshot expects, in this
    /// order: kind, kind format version, spec hash, source row count and size.
    pub fn open(bytes: BundleBytes, expect: &Expect<'_>) -> Result<BundleView, BundleError> {
        let v = Self::parse(bytes)?;
        v.check(expect)?;
        Ok(v)
    }

    /// Checks the header against what the snapshot expects.
    pub fn check(&self, expect: &Expect<'_>) -> Result<(), BundleError> {
        let h = &self.header;
        if h.kind != expect.kind {
            return Err(BundleError::KindMismatch {
                found: h.kind.clone(),
                expected: expect.kind.into(),
            });
        }
        if h.kind_format != expect.kind_format {
            return Err(BundleError::Unsupported(format!(
                "kind format version {} of {:?}, this release reads {}",
                h.kind_format, h.kind, expect.kind_format
            )));
        }
        if h.spec_hash != expect.spec_hash {
            return Err(BundleError::SpecHashMismatch {
                found: h.spec_hash.clone(),
                expected: expect.spec_hash.into(),
            });
        }
        if let Some(rows) = expect.source_rows
            && rows != h.source_rows
        {
            return Err(BundleError::SourceMismatch(format!(
                "{} source rows, the data file has {rows}",
                h.source_rows
            )));
        }
        if h.source_size != expect.source_size {
            return Err(BundleError::SourceMismatch(format!(
                "source size {}, the data file has {}",
                h.source_size, expect.source_size
            )));
        }
        Ok(())
    }

    /// Verifies every section's checksum.
    pub fn verify_sections(&self) -> Result<(), BundleError> {
        for s in &self.sections {
            let b = self.slice(s);
            if xxh3_64(b) != s.checksum {
                return Err(BundleError::Checksum(s.name.clone()));
            }
        }
        Ok(())
    }

    /// The fixed header.
    pub fn header(&self) -> &BundleHeader {
        &self.header
    }

    /// The section table, in the order the sections were written.
    pub fn sections(&self) -> &[SectionEntry] {
        &self.sections
    }

    /// The bytes of section `name`, 4,096-aligned in memory.
    pub fn section(&self, name: &str) -> Option<&[u8]> {
        self.sections
            .iter()
            .find(|s| s.name == name)
            .map(|s| self.slice(s))
    }

    /// Sections whose name starts with `prefix`, in table order.
    pub fn sections_with_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a [u8])> + 'a {
        self.sections
            .iter()
            .filter(move |s| s.name.starts_with(prefix))
            .map(|s| (s.name.as_str(), self.slice(s)))
    }

    /// The whole bundle.
    pub fn bytes(&self) -> &BundleBytes {
        &self.bytes
    }

    /// Total size of the bundle.
    pub fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    /// Whether the bundle has no bytes (never true of a parsed bundle).
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn slice(&self, s: &SectionEntry) -> &[u8] {
        &self.bytes.as_slice()[s.offset as usize..(s.offset + s.length) as usize]
    }
}

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap_or([0; 4]))
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap_or([0; 8]))
}

/// A NUL-padded ASCII name: printable bytes, then only NULs.
fn padded_name(b: &[u8], what: &str) -> Result<String, BundleError> {
    let end = b.iter().position(|c| *c == 0).unwrap_or(b.len());
    if end == 0
        || b[end..].iter().any(|c| *c != 0)
        || !b[..end].iter().all(|c| c.is_ascii_graphic())
    {
        return Err(BundleError::Corrupt(format!("invalid {what} name")));
    }
    Ok(String::from_utf8_lossy(&b[..end]).into_owned())
}

/// Hex text of 8 raw bytes.
pub(crate) fn hash_hex(raw: &[u8]) -> String {
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

/// 8 raw bytes of 16 hex characters.
pub(crate) fn hash_raw(hex: &str) -> Option<[u8; 8]> {
    if hex.len() != 16 || !hex.bytes().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 8];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

fn parse_structure(b: &[u8]) -> Result<(BundleHeader, Vec<SectionEntry>), BundleError> {
    let len = b.len() as u64;
    if b.len() < HEADER + FOOTER {
        return Err(BundleError::Truncated(len));
    }
    if &b[0..4] != MAGIC || &b[b.len() - 4..] != END_MAGIC {
        return Err(BundleError::BadMagic);
    }
    let version = u16_at(b, 4);
    if version != BUNDLE_VERSION {
        return Err(BundleError::Unsupported(format!(
            "bundle format version {version}, this release reads {BUNDLE_VERSION}"
        )));
    }
    let flags = u16_at(b, 6);
    if flags != 0 {
        return Err(BundleError::Unsupported(format!(
            "unknown flags {flags:#06x}"
        )));
    }
    let kind = padded_name(&b[8..16], "kind")?;
    let spec_hash = hash_hex(&b[16..24]);
    let count = u32_at(b, 48);
    let kind_format = u32_at(b, 52);
    if u64_at(b, 56) != 0 {
        return Err(BundleError::Corrupt("reserved header bytes are set".into()));
    }
    if count > MAX_SECTIONS {
        return Err(BundleError::Corrupt(format!("{count} sections")));
    }
    let f = b.len() - FOOTER;
    let table = u64_at(b, f);
    let table_len = count as u64 * ENTRY as u64;
    if table.checked_add(table_len) != Some(f as u64) {
        return Err(BundleError::Corrupt(format!(
            "section table at {table} with {count} entries does not end at the footer ({f})"
        )));
    }
    if table < HEADER as u64 {
        return Err(BundleError::Corrupt(
            "section table inside the header".into(),
        ));
    }
    let t = table as usize;
    let mut digest = Xxh3::new();
    digest.update(&b[..HEADER]);
    digest.update(&b[t..f]);
    if digest.digest() != u64_at(b, f + 8) {
        return Err(BundleError::Checksum("header and section table".into()));
    }
    if u32_at(b, f + 16) != 0 {
        return Err(BundleError::Corrupt("reserved footer bytes are set".into()));
    }
    let mut sections: Vec<SectionEntry> = Vec::with_capacity(count as usize);
    let mut end = HEADER as u64;
    for i in 0..count as usize {
        let e = &b[t + i * ENTRY..t + (i + 1) * ENTRY];
        let name = padded_name(&e[0..32], "section")?;
        let offset = u64_at(e, 32);
        let length = u64_at(e, 40);
        let checksum = u64_at(e, 48);
        if u64_at(e, 56) != 0 {
            return Err(BundleError::Corrupt(
                "reserved section bytes are set".into(),
            ));
        }
        if !offset.is_multiple_of(ALIGN) || offset < end {
            return Err(BundleError::Corrupt(format!(
                "section {name:?} at {offset} is unaligned or overlaps the one before"
            )));
        }
        let stop = offset
            .checked_add(length)
            .filter(|s| *s <= table)
            .ok_or_else(|| BundleError::Corrupt(format!("section {name:?} runs past the table")))?;
        if sections.iter().any(|s| s.name == name) {
            return Err(BundleError::Corrupt(format!("section {name:?} twice")));
        }
        end = stop;
        sections.push(SectionEntry {
            name,
            offset,
            length,
            checksum,
        });
    }
    let header = BundleHeader {
        version,
        flags,
        kind,
        spec_hash,
        source_rows: u64_at(b, 24),
        source_size: u64_at(b, 32),
        indexed_rows: u64_at(b, 40),
        kind_format,
    };
    Ok((header, sections))
}

/// Where a builder writes its sections. Bytes written after `section(name)` belong to that
/// section until the next call.
pub trait SectionSink {
    /// Starts section `name` (at most 32 bytes, unique in the bundle) and returns the writer
    /// its bytes go to.
    fn section(&mut self, name: &str) -> io::Result<&mut dyn Write>;
}

#[doc(hidden)]
/// What the header of a bundle being written says (the indexed row count comes at the end).
#[derive(Clone, Debug)]
pub struct BundleMeta {
    /// The index kind.
    pub kind: String,
    /// The kind's format version.
    pub kind_format: u32,
    /// The spec hash.
    pub spec_hash: String,
    /// Rows of the source data file.
    pub source_rows: u64,
    /// Size in bytes of the source data file.
    pub source_size: u64,
}

struct Current {
    name: String,
    start: u64,
    digest: Xxh3,
}

/// Streams a bundle to `out`. See the module documentation.
pub(crate) struct BundleWriter<W: Write + Seek> {
    out: W,
    pos: u64,
    meta: BundleMeta,
    entries: Vec<SectionEntry>,
    current: Option<Current>,
}

impl<W: Write + Seek> BundleWriter<W> {
    /// Starts a bundle: writes a placeholder header.
    pub fn new(mut out: W, meta: BundleMeta) -> io::Result<Self> {
        let bad = |m: String| io::Error::new(io::ErrorKind::InvalidInput, m);
        if meta.kind.is_empty()
            || meta.kind.len() > MAX_KIND_NAME
            || !meta.kind.bytes().all(|c| c.is_ascii_graphic())
        {
            return Err(bad(format!("invalid kind name {:?}", meta.kind)));
        }
        if hash_raw(&meta.spec_hash).is_none() {
            return Err(bad(format!("invalid spec hash {:?}", meta.spec_hash)));
        }
        out.write_all(&[0u8; HEADER])?;
        Ok(BundleWriter {
            out,
            pos: HEADER as u64,
            meta,
            entries: Vec::new(),
            current: None,
        })
    }

    fn pad_to(&mut self, align: u64) -> io::Result<()> {
        let rem = self.pos % align;
        if rem != 0 {
            let n = (align - rem) as usize;
            let zeros = [0u8; 4096];
            let mut left = n;
            while left > 0 {
                let k = left.min(zeros.len());
                self.out.write_all(&zeros[..k])?;
                left -= k;
            }
            self.pos += n as u64;
        }
        Ok(())
    }

    fn close_section(&mut self) {
        if let Some(c) = self.current.take() {
            self.entries.push(SectionEntry {
                name: c.name,
                offset: c.start,
                length: self.pos - c.start,
                checksum: c.digest.digest(),
            });
        }
    }

    /// Sets the source file's row count and size, for a bundle built while its data file is
    /// still being written (they go into the header at [`BundleWriter::finish`]).
    pub fn set_source(&mut self, rows: u64, size: u64) {
        self.meta.source_rows = rows;
        self.meta.source_size = size;
    }

    /// Ends the last section, writes the table and footer, patches the header and returns the
    /// sink and the bundle's length.
    pub fn finish(mut self, indexed_rows: u64) -> io::Result<(W, u64)> {
        self.close_section();
        let table = self.pos;
        let mut t = Vec::with_capacity(self.entries.len() * ENTRY);
        for e in &self.entries {
            let mut name = [0u8; 32];
            name[..e.name.len()].copy_from_slice(e.name.as_bytes());
            t.extend_from_slice(&name);
            t.extend_from_slice(&e.offset.to_le_bytes());
            t.extend_from_slice(&e.length.to_le_bytes());
            t.extend_from_slice(&e.checksum.to_le_bytes());
            t.extend_from_slice(&0u64.to_le_bytes());
        }
        let mut h = [0u8; HEADER];
        h[0..4].copy_from_slice(MAGIC);
        h[4..6].copy_from_slice(&BUNDLE_VERSION.to_le_bytes());
        let mut kind = [0u8; 8];
        kind[..self.meta.kind.len()].copy_from_slice(self.meta.kind.as_bytes());
        h[8..16].copy_from_slice(&kind);
        h[16..24].copy_from_slice(&hash_raw(&self.meta.spec_hash).unwrap_or_default());
        h[24..32].copy_from_slice(&self.meta.source_rows.to_le_bytes());
        h[32..40].copy_from_slice(&self.meta.source_size.to_le_bytes());
        h[40..48].copy_from_slice(&indexed_rows.to_le_bytes());
        h[48..52].copy_from_slice(&(self.entries.len() as u32).to_le_bytes());
        h[52..56].copy_from_slice(&self.meta.kind_format.to_le_bytes());
        let mut digest = Xxh3::new();
        digest.update(&h);
        digest.update(&t);
        let mut footer = [0u8; FOOTER];
        footer[0..8].copy_from_slice(&table.to_le_bytes());
        footer[8..16].copy_from_slice(&digest.digest().to_le_bytes());
        footer[20..24].copy_from_slice(END_MAGIC);
        self.out.write_all(&t)?;
        self.out.write_all(&footer)?;
        let len = table + t.len() as u64 + FOOTER as u64;
        self.out.seek(SeekFrom::Start(0))?;
        self.out.write_all(&h)?;
        self.out.seek(SeekFrom::Start(len))?;
        self.out.flush()?;
        Ok((self.out, len))
    }
}

impl<W: Write + Seek> SectionSink for BundleWriter<W> {
    fn section(&mut self, name: &str) -> io::Result<&mut dyn Write> {
        if name.is_empty()
            || name.len() > MAX_SECTION_NAME
            || !name.bytes().all(|c| c.is_ascii_graphic())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid section name {name:?}"),
            ));
        }
        if self.entries.iter().any(|e| e.name == name)
            || self.current.as_ref().is_some_and(|c| c.name == name)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("section {name:?} written twice"),
            ));
        }
        self.close_section();
        self.pad_to(ALIGN)?;
        self.current = Some(Current {
            name: name.into(),
            start: self.pos,
            digest: Xxh3::new(),
        });
        Ok(self)
    }
}

impl<W: Write + Seek> Write for BundleWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Some(c) = self.current.as_mut() else {
            return Err(io::Error::other("bytes written outside a section"));
        };
        let n = self.out.write(buf)?;
        c.digest.update(&buf[..n]);
        self.pos += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

#[doc(hidden)]
/// Writes a whole bundle to memory: `sections` in order.
pub fn write_to_vec(
    meta: BundleMeta,
    sections: &[(&str, &[u8])],
    indexed_rows: u64,
) -> io::Result<Vec<u8>> {
    let mut w = BundleWriter::new(io::Cursor::new(Vec::new()), meta)?;
    for (name, bytes) in sections {
        w.section(name)?.write_all(bytes)?;
    }
    let (c, _) = w.finish(indexed_rows)?;
    Ok(c.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> BundleMeta {
        BundleMeta {
            kind: "test-sum".into(),
            kind_format: 1,
            spec_hash: "0123456789abcdef".into(),
            source_rows: 10,
            source_size: 1234,
        }
    }

    fn expect() -> Expect<'static> {
        Expect {
            kind: "test-sum",
            kind_format: 1,
            spec_hash: "0123456789abcdef",
            source_rows: Some(10),
            source_size: 1234,
        }
    }

    fn sample() -> Vec<u8> {
        write_to_vec(
            meta(),
            &[("a", b"hello"), ("b.0", &[7u8; 5000]), ("c", b"")],
            9,
        )
        .unwrap()
    }

    #[test]
    fn round_trip() {
        let b = sample();
        let v = BundleView::open(BundleBytes::copy(&b).unwrap(), &expect()).unwrap();
        v.verify_sections().unwrap();
        assert_eq!(v.header().indexed_rows, 9);
        assert_eq!(v.section("a").unwrap(), b"hello");
        assert_eq!(v.section("b.0").unwrap(), &[7u8; 5000][..]);
        assert_eq!(v.section("c").unwrap(), b"");
        assert!(v.section("d").is_none());
        for s in v.sections() {
            assert_eq!(s.offset % ALIGN, 0);
        }
        let p = v.section("b.0").unwrap().as_ptr() as usize;
        assert_eq!(p % ALIGN as usize, 0, "sections are page-aligned in memory");
        assert_eq!(v.sections_with_prefix("b.").count(), 1);
    }

    #[test]
    fn empty_bundle_is_valid_v4() {
        let b = write_to_vec(meta(), &[], 0).unwrap();
        let v = BundleView::open(BundleBytes::copy(&b).unwrap(), &expect()).unwrap();
        assert_eq!(v.sections().len(), 0);
        assert_eq!(v.header().indexed_rows, 0);
    }

    #[test]
    fn each_validation_failure() {
        let b = sample();
        let open = |b: &[u8], e: &Expect| BundleView::open(BundleBytes::copy(b).unwrap(), e);
        let mut e = expect();
        e.kind = "vec-hnsw";
        assert!(matches!(
            open(&b, &e),
            Err(BundleError::KindMismatch { .. })
        ));
        let mut e = expect();
        e.kind_format = 2;
        assert!(matches!(open(&b, &e), Err(BundleError::Unsupported(_))));
        let mut e = expect();
        e.spec_hash = "ffffffffffffffff";
        assert!(matches!(
            open(&b, &e),
            Err(BundleError::SpecHashMismatch { .. })
        ));
        let mut e = expect();
        e.source_rows = Some(11);
        assert!(matches!(open(&b, &e), Err(BundleError::SourceMismatch(_))));
        let mut e = expect();
        e.source_rows = None;
        assert!(open(&b, &e).is_ok(), "unknown row count: size alone");
        e.source_size = 1;
        assert!(matches!(open(&b, &e), Err(BundleError::SourceMismatch(_))));

        let mut x = b.clone();
        x[0] = b'X';
        assert!(matches!(open(&x, &expect()), Err(BundleError::BadMagic)));
        let mut x = b.clone();
        x[4] = 2;
        assert!(matches!(
            open(&x, &expect()),
            Err(BundleError::Unsupported(_))
        ));
        let mut x = b.clone();
        x[6] = 1;
        assert!(matches!(
            open(&x, &expect()),
            Err(BundleError::Unsupported(_))
        ));
        let mut x = b.clone();
        x[30] ^= 1; // source size, covered by the footer checksum
        assert!(matches!(open(&x, &expect()), Err(BundleError::Checksum(_))));
        let mut x = b.clone();
        x[4096] ^= 1; // section "a"
        let v = open(&x, &expect()).unwrap();
        assert!(matches!(v.verify_sections(), Err(BundleError::Checksum(n)) if n == "a"));
    }

    #[test]
    fn truncation_at_every_byte_is_refused() {
        let b = write_to_vec(meta(), &[("a", b"xyz"), ("b", &[1u8; 300])], 3).unwrap();
        for n in 0..b.len() {
            let r = BundleView::parse(BundleBytes::copy(&b[..n]).unwrap());
            assert!(r.is_err(), "truncated to {n} bytes was accepted");
        }
        let mut longer = b.clone();
        longer.push(0);
        assert!(BundleView::parse(BundleBytes::copy(&longer).unwrap()).is_err());
    }

    #[test]
    fn writer_rejects_bad_names() {
        let mut w = BundleWriter::new(io::Cursor::new(Vec::new()), meta()).unwrap();
        assert!(w.section("").is_err());
        assert!(w.section(&"x".repeat(33)).is_err());
        assert!(w.section("a b").is_err());
        w.section("a").unwrap();
        assert!(w.section("a").is_err());
        let mut m = meta();
        m.kind = "too-long-kind".into();
        assert!(BundleWriter::new(io::Cursor::new(Vec::new()), m).is_err());
        let mut w = BundleWriter::new(io::Cursor::new(Vec::new()), meta()).unwrap();
        assert!(w.write_all(b"x").is_err(), "bytes outside a section");
    }

    #[test]
    fn hex_round_trip() {
        let raw = hash_raw("00ff10a0b0c0d0e9").unwrap();
        assert_eq!(hash_hex(&raw), "00ff10a0b0c0d0e9");
        assert!(hash_raw("xyz").is_none());
    }
}
