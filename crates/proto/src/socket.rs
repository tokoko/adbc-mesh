//! Minimal `SOCK_SEQPACKET` wrapper with fd passing.

use std::io::{Error, ErrorKind, Result};
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::MAX_FRAME;

fn check(rc: libc::c_int) -> Result<libc::c_int> {
    if rc < 0 {
        Err(Error::last_os_error())
    } else {
        Ok(rc)
    }
}

fn sockaddr(path: &Path) -> Result<(libc::sockaddr_un, libc::socklen_t)> {
    use std::os::unix::ffi::OsStrExt;
    let mut addr: libc::sockaddr_un = unsafe { zeroed() };
    let bytes = path.as_os_str().as_bytes();
    if bytes.len() >= addr.sun_path.len() {
        return Err(Error::new(ErrorKind::InvalidInput, "socket path too long"));
    }
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (dst, src) in addr.sun_path.iter_mut().zip(bytes) {
        *dst = *src as libc::c_char;
    }
    Ok((addr, size_of::<libc::sockaddr_un>() as libc::socklen_t))
}

fn new_socket() -> Result<OwnedFd> {
    let fd = check(unsafe {
        libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC, 0)
    })?;
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

pub struct Listener(OwnedFd);

impl Listener {
    pub fn bind(path: &Path) -> Result<Self> {
        let _ = std::fs::remove_file(path);
        let fd = new_socket()?;
        let (addr, len) = sockaddr(path)?;
        check(unsafe { libc::bind(fd.as_raw_fd(), &addr as *const _ as *const libc::sockaddr, len) })?;
        check(unsafe { libc::listen(fd.as_raw_fd(), 64) })?;
        Ok(Self(fd))
    }

    pub fn accept(&self) -> Result<Conn> {
        let fd = check(unsafe {
            libc::accept4(self.0.as_raw_fd(), std::ptr::null_mut(), std::ptr::null_mut(), libc::SOCK_CLOEXEC)
        })?;
        Ok(Conn(unsafe { OwnedFd::from_raw_fd(fd) }))
    }
}

pub struct Conn(OwnedFd);

impl Conn {
    pub fn connect(path: &Path) -> Result<Self> {
        let fd = new_socket()?;
        let (addr, len) = sockaddr(path)?;
        check(unsafe { libc::connect(fd.as_raw_fd(), &addr as *const _ as *const libc::sockaddr, len) })?;
        Ok(Self(fd))
    }

    /// `(pid, uid, gid)` of the peer as recorded by the kernel at connect time.
    pub fn peer(&self) -> Result<(i32, u32, u32)> {
        let mut cred: libc::ucred = unsafe { zeroed() };
        let mut len = size_of::<libc::ucred>() as libc::socklen_t;
        check(unsafe {
            libc::getsockopt(
                self.0.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                &mut cred as *mut _ as *mut libc::c_void,
                &mut len,
            )
        })?;
        Ok((cred.pid, cred.uid, cred.gid))
    }

    pub fn send<T: Serialize>(&self, msg: &T, fd: Option<RawFd>) -> Result<()> {
        let bytes = serde_json::to_vec(msg)?;
        if bytes.len() > MAX_FRAME {
            return Err(Error::new(ErrorKind::InvalidInput, "frame too large"));
        }
        let mut iov = libc::iovec { iov_base: bytes.as_ptr() as *mut _, iov_len: bytes.len() };
        let mut hdr: libc::msghdr = unsafe { zeroed() };
        hdr.msg_iov = &mut iov;
        hdr.msg_iovlen = 1;

        let mut control = [0u64; 4];
        if let Some(fd) = fd {
            hdr.msg_control = control.as_mut_ptr() as *mut _;
            hdr.msg_controllen = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) } as _;
            unsafe {
                let cmsg = libc::CMSG_FIRSTHDR(&hdr);
                (*cmsg).cmsg_level = libc::SOL_SOCKET;
                (*cmsg).cmsg_type = libc::SCM_RIGHTS;
                (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as _;
                std::ptr::write_unaligned(libc::CMSG_DATA(cmsg) as *mut RawFd, fd);
            }
        }
        // MSG_NOSIGNAL because the shim runs inside a host process we don't control.
        let n = unsafe { libc::sendmsg(self.0.as_raw_fd(), &hdr, libc::MSG_NOSIGNAL) };
        if n < 0 {
            return Err(Error::last_os_error());
        }
        Ok(())
    }

    /// Receive a frame and accept an fd along with it. `None` means the peer hung up.
    pub fn recv_with_fd<T: DeserializeOwned>(&self) -> Result<Option<(T, Option<OwnedFd>)>> {
        let mut buf = vec![0u8; MAX_FRAME];
        let mut iov = libc::iovec { iov_base: buf.as_mut_ptr() as *mut _, iov_len: buf.len() };
        let mut hdr: libc::msghdr = unsafe { zeroed() };
        hdr.msg_iov = &mut iov;
        hdr.msg_iovlen = 1;
        let mut control = [0u64; 4];
        hdr.msg_control = control.as_mut_ptr() as *mut _;
        hdr.msg_controllen = std::mem::size_of_val(&control) as _;

        let n = unsafe { libc::recvmsg(self.0.as_raw_fd(), &mut hdr, libc::MSG_CMSG_CLOEXEC) };
        if n < 0 {
            return Err(Error::last_os_error());
        }
        let mut fd = None;
        let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(&hdr) };
        while !cmsg.is_null() {
            unsafe {
                if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                    let raw = std::ptr::read_unaligned(libc::CMSG_DATA(cmsg) as *const RawFd);
                    fd = Some(OwnedFd::from_raw_fd(raw));
                }
                cmsg = libc::CMSG_NXTHDR(&hdr, cmsg);
            }
        }
        if n == 0 {
            return Ok(None);
        }
        let msg = serde_json::from_slice(&buf[..n as usize])?;
        Ok(Some((msg, fd)))
    }

    /// Receive a frame and refuse any fds. Used by the sidecar: with no control buffer the
    /// kernel discards attached fds, so a hostile client can't exhaust the sidecar's fd table.
    pub fn recv<T: DeserializeOwned>(&self) -> Result<Option<T>> {
        let mut buf = vec![0u8; MAX_FRAME];
        let n = unsafe {
            libc::recv(self.0.as_raw_fd(), buf.as_mut_ptr() as *mut _, buf.len(), 0)
        };
        if n < 0 {
            return Err(Error::last_os_error());
        }
        if n == 0 {
            return Ok(None);
        }
        Ok(Some(serde_json::from_slice(&buf[..n as usize])?))
    }
}
