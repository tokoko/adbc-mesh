//! Moving Arrow data through sealed memfds.
//!
//! Layout of a batch memfd: `[u64 words describing the array tree][64-byte aligned buffers]`.
//! The consumer maps it read-only and wraps the buffers in place, so the only copy in the
//! whole path is the one from the backend driver's allocation into the memfd.
//!
//! Seals are what make the handoff safe. A read-only fd is not enough: its holder can reopen
//! `/proc/self/fd/N` as `O_RDWR` and then write or truncate. `F_SEAL_WRITE|SHRINK|GROW|SEAL`
//! holds no matter how the file is reopened.

use std::io::{Error, ErrorKind, Result};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::ptr::NonNull;
use std::sync::Arc;

use arrow_array::{make_array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{DataType, Schema, SchemaRef};

const ALIGN: usize = 64;

fn aligned(n: usize) -> usize {
    (n + ALIGN - 1) & !(ALIGN - 1)
}

/// A writable memfd of fixed size that is sealed before anyone else can see it.
struct Arena {
    fd: OwnedFd,
    ptr: *mut u8,
    len: usize,
}

impl Arena {
    fn new(len: usize) -> Result<Self> {
        let raw = unsafe {
            libc::memfd_create(
                c"adbc-mesh".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        if raw < 0 {
            return Err(Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } < 0 {
            return Err(Error::last_os_error());
        }
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }
        Ok(Self { fd, ptr: ptr as *mut u8, len })
    }

    fn write(&mut self, at: usize, bytes: &[u8]) {
        assert!(at + bytes.len() <= self.len);
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr.add(at), bytes.len()) };
    }

    /// Unmap and seal. `F_SEAL_WRITE` fails with `EBUSY` while a writable mapping exists,
    /// which is why the unmap has to come first.
    fn seal(self) -> Result<OwnedFd> {
        unsafe { libc::munmap(self.ptr as *mut _, self.len) };
        let seals = libc::F_SEAL_WRITE | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;
        if unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_ADD_SEALS, seals) } < 0 {
            return Err(Error::last_os_error());
        }
        Ok(self.fd)
    }
}

/// Read-only mapping of a sealed memfd. `on_drop` runs after the last buffer goes away.
pub struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
    on_drop: Option<Box<dyn Fn() + Send + Sync + std::panic::RefUnwindSafe>>,
}

unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Drop for Mapping {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.ptr.as_ptr() as *mut _, self.len) };
        if let Some(f) = &self.on_drop {
            f();
        }
    }
}

fn map_readonly(
    fd: &OwnedFd,
    len: usize,
    on_drop: Option<Box<dyn Fn() + Send + Sync + std::panic::RefUnwindSafe>>,
) -> Result<Arc<Mapping>> {
    let ptr = unsafe {
        libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_SHARED, fd.as_raw_fd(), 0)
    };
    if ptr == libc::MAP_FAILED {
        return Err(Error::last_os_error());
    }
    Ok(Arc::new(Mapping { ptr: NonNull::new(ptr as *mut u8).unwrap(), len, on_drop }))
}

fn describe(data: &ArrayData, next: &mut usize, words: &mut Vec<u64>, slots: &mut Vec<(usize, Buffer)>) {
    let mut place = |buffer: &Buffer, words: &mut Vec<u64>| {
        words.extend([*next as u64, buffer.len() as u64]);
        slots.push((*next, buffer.clone()));
        *next = aligned(*next + buffer.len());
    };
    words.extend([data.len() as u64, data.offset() as u64]);
    match data.nulls() {
        Some(nulls) => {
            words.extend([1, nulls.offset() as u64, nulls.null_count() as u64]);
            place(nulls.buffer(), words);
        }
        None => words.push(0),
    }
    words.push(data.buffers().len() as u64);
    for buffer in data.buffers() {
        place(buffer, words);
    }
    words.push(data.child_data().len() as u64);
    for child in data.child_data() {
        describe(child, next, words, slots);
    }
}

/// Copy a batch into a new sealed memfd. Returns the fd and its length.
pub fn encode(batch: &RecordBatch) -> Result<(OwnedFd, usize)> {
    let datas: Vec<ArrayData> = batch.columns().iter().map(|c| c.to_data()).collect();

    // The header's size depends only on the tree's shape, so measure it first.
    let mut words = vec![batch.num_rows() as u64, datas.len() as u64];
    let mut slots = Vec::new();
    let mut scratch = 0;
    for d in &datas {
        describe(d, &mut scratch, &mut words, &mut slots);
    }
    let header = aligned(words.len() * 8);

    words.truncate(2);
    slots.clear();
    let mut next = header;
    for d in &datas {
        describe(d, &mut next, &mut words, &mut slots);
    }
    let total = next.max(ALIGN);

    let mut arena = Arena::new(total)?;
    let header_bytes: Vec<u8> = words.iter().flat_map(|w| w.to_le_bytes()).collect();
    arena.write(0, &header_bytes);
    for (at, buffer) in &slots {
        arena.write(*at, buffer.as_slice());
    }
    Ok((arena.seal()?, total))
}

struct Cursor<'a> {
    words: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn next(&mut self) -> Result<usize> {
        let end = self.at + 8;
        let bytes = self.words.get(self.at..end).ok_or_else(|| invalid("truncated batch header"))?;
        self.at = end;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()) as usize)
    }
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

fn child_type(parent: &DataType, index: usize) -> Result<DataType> {
    use DataType::*;
    let found = match parent {
        List(f) | LargeList(f) | ListView(f) | LargeListView(f) | FixedSizeList(f, _) | Map(f, _) => {
            Some(f.data_type().clone())
        }
        Struct(fields) => fields.get(index).map(|f| f.data_type().clone()),
        Dictionary(_, value) => Some(value.as_ref().clone()),
        Union(fields, _) => fields.iter().nth(index).map(|(_, f)| f.data_type().clone()),
        RunEndEncoded(runs, values) => {
            Some(if index == 0 { runs.data_type().clone() } else { values.data_type().clone() })
        }
        _ => None,
    };
    found.ok_or_else(|| invalid("batch tree does not match schema"))
}

fn rebuild(cursor: &mut Cursor, body: &Buffer, data_type: DataType) -> Result<ArrayData> {
    let slice = |cursor: &mut Cursor| -> Result<Buffer> {
        let (at, len) = (cursor.next()?, cursor.next()?);
        if at.checked_add(len).is_none_or(|end| end > body.len()) {
            return Err(invalid("buffer outside memfd"));
        }
        Ok(body.slice_with_length(at, len))
    };
    let len = cursor.next()?;
    let offset = cursor.next()?;
    let mut builder = ArrayDataBuilder::new(data_type.clone()).len(len).offset(offset);
    if cursor.next()? == 1 {
        let bit_offset = cursor.next()?;
        let null_count = cursor.next()?;
        let bits = BooleanBuffer::new(slice(cursor)?, bit_offset, len);
        builder = builder.nulls(Some(unsafe { NullBuffer::new_unchecked(bits, null_count) }));
    }
    for _ in 0..cursor.next()? {
        builder = builder.add_buffer(slice(cursor)?);
    }
    for index in 0..cursor.next()? {
        let child = rebuild(cursor, body, child_type(&data_type, index)?)?;
        builder = builder.add_child_data(child);
    }
    // No validation pass: the sidecar is trusted by the consumer, and validating would touch
    // every page of data the consumer may never read.
    Ok(unsafe { builder.build_unchecked() })
}

/// Map a batch memfd and wrap its buffers without copying them.
pub fn decode(
    fd: &OwnedFd,
    len: usize,
    schema: &SchemaRef,
    on_drop: Box<dyn Fn() + Send + Sync + std::panic::RefUnwindSafe>,
) -> Result<RecordBatch> {
    let mapping = map_readonly(fd, len, Some(on_drop))?;
    let body = unsafe { Buffer::from_custom_allocation(mapping.ptr, mapping.len, mapping.clone()) };
    let mut cursor = Cursor { words: body.as_slice(), at: 0 };
    let rows = cursor.next()?;
    let columns = cursor.next()?;
    if columns != schema.fields().len() {
        return Err(invalid("column count does not match schema"));
    }
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| rebuild(&mut cursor, &body, f.data_type().clone()).map(make_array))
        .collect::<Result<_>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(schema.clone(), arrays, &options)
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))
}

/// Put an IPC-encoded schema in a sealed memfd. Schemas go through a memfd rather than the
/// socket because a wide table doesn't fit in one datagram.
pub fn encode_schema(schema: &Schema) -> Result<(OwnedFd, usize)> {
    let generator = arrow_ipc::writer::IpcDataGenerator::default();
    let options = arrow_ipc::writer::IpcWriteOptions::default();
    let encoded = generator.schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut arrow_ipc::writer::DictionaryTracker::new(false),
        &options,
    );
    let mut arena = Arena::new(encoded.ipc_message.len().max(1))?;
    arena.write(0, &encoded.ipc_message);
    Ok((arena.seal()?, encoded.ipc_message.len()))
}

pub fn decode_schema(fd: &OwnedFd, len: usize) -> Result<Schema> {
    let mapping = map_readonly(fd, len.max(1), None)?;
    let bytes = unsafe { std::slice::from_raw_parts(mapping.ptr.as_ptr(), len) };
    let message = arrow_ipc::root_as_message(bytes)
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    let ipc_schema = message.header_as_schema().ok_or_else(|| invalid("expected a schema"))?;
    Ok(arrow_ipc::convert::fb_to_schema(ipc_schema))
}
