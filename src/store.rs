use anyhow::{Context, Result, ensure};
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Segment {
    pub level: u8,
    pub min_txid: u64,
    pub max_txid: u64,
}

impl Segment {
    pub fn new(level: u8, min_txid: u64, max_txid: u64) -> Result<Self> {
        let segment = Self {
            level,
            min_txid,
            max_txid,
        };
        segment.validate()?;
        Ok(segment)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.level <= 9 && self.min_txid > 0 && self.min_txid <= self.max_txid,
            "invalid LTX segment"
        );
        ensure!(
            self.level != 9 || self.min_txid == 1,
            "snapshot must start at transaction one"
        );
        Ok(())
    }

    pub fn filename(&self) -> String {
        format!("{:016x}-{:016x}.ltx", self.min_txid, self.max_txid)
    }

    fn parse(level: u8, name: &str) -> Option<Self> {
        if !name.is_ascii() || name.len() != 37 || &name[16..17] != "-" || !name.ends_with(".ltx") {
            return None;
        }
        Self::new(
            level,
            u64::from_str_radix(&name[..16], 16).ok()?,
            u64::from_str_radix(&name[17..33], 16).ok()?,
        )
        .ok()
    }
}

pub trait SegmentWriter: Write + Send {
    /// Publishes an immutable file atomically and durably; errors may mean publication occurred.
    fn commit(self: Box<Self>) -> Result<()>;
}

pub trait ReplicaStore: Send + Sync {
    fn list(&self, level: u8) -> Result<Vec<Segment>>;
    fn open(&self, segment: &Segment) -> Result<Box<dyn Read + Send>>;
    fn create(&self, segment: &Segment) -> Result<Box<dyn SegmentWriter>>;
    fn remove(&self, segment: &Segment) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct FileStore {
    root: PathBuf,
}

impl FileStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn path(&self, segment: &Segment) -> PathBuf {
        self.root
            .join("ltx")
            .join(segment.level.to_string())
            .join(segment.filename())
    }
}

impl ReplicaStore for FileStore {
    fn list(&self, level: u8) -> Result<Vec<Segment>> {
        ensure!(level <= 9, "invalid LTX level");
        let entries = match fs::read_dir(self.root.join("ltx").join(level.to_string())) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut segments = Vec::new();
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && let Some(segment) = Segment::parse(level, &entry.file_name().to_string_lossy())
            {
                segments.push(segment);
            }
        }
        segments.sort();
        Ok(segments)
    }

    fn open(&self, segment: &Segment) -> Result<Box<dyn Read + Send>> {
        segment.validate()?;
        Ok(Box::new(File::open(self.path(segment))?))
    }

    fn create(&self, segment: &Segment) -> Result<Box<dyn SegmentWriter>> {
        segment.validate()?;
        let path = self.path(segment);
        let parent = path.parent().context("LTX parent missing")?;
        create_directory(parent)?;
        Ok(Box::new(FileWriter {
            file: NamedTempFile::new_in(parent)?,
            path,
        }))
    }

    fn remove(&self, segment: &Segment) -> Result<()> {
        segment.validate()?;
        let path = self.path(segment);
        fs::remove_file(&path)?;
        sync_directory(path.parent().context("LTX parent missing")?)
    }
}

struct FileWriter {
    file: NamedTempFile,
    path: PathBuf,
}

impl Write for FileWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl SegmentWriter for FileWriter {
    fn commit(mut self: Box<Self>) -> Result<()> {
        self.file.flush()?;
        self.file.as_file().sync_all()?;
        self.file
            .persist_noclobber(&self.path)
            .with_context(|| format!("publish {}", self.path.display()))?;
        sync_directory(self.path.parent().context("LTX parent missing")?)
    }
}

pub(crate) fn create_directory(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() || path.is_dir() {
        return Ok(());
    }
    let parent = path.parent().context("directory parent missing")?;
    create_directory(parent)?;
    match fs::create_dir(path) {
        Ok(()) => sync_directory(parent),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    })?
    .sync_all()?;
    Ok(())
}
