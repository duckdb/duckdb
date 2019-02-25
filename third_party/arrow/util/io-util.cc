// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Ensure 64-bit off_t for platforms where it matters
#ifdef _FILE_OFFSET_BITS
#undef _FILE_OFFSET_BITS
#endif

#define _FILE_OFFSET_BITS 64

#include "arrow/util/windows_compatibility.h"  // IWYU pragma: keep

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>  // IWYU pragma: keep

// Defines that don't exist in MinGW
#if defined(__MINGW32__)
#define ARROW_WRITE_SHMODE S_IRUSR | S_IWUSR
#elif defined(_MSC_VER)  // Visual Studio

#else  // gcc / clang on POSIX platforms
#define ARROW_WRITE_SHMODE S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH
#endif

// For filename conversion
#if defined(_MSC_VER)
#include <boost/system/system_error.hpp>  // NOLINT
#include <codecvt>
#include <locale>
#endif

// ----------------------------------------------------------------------
// file compatibility stuff

#if defined(__MINGW32__)  // MinGW
// nothing
#elif defined(_MSC_VER)  // Visual Studio
#include <io.h>
#else  // POSIX / Linux
// nothing
#endif

#ifdef _WIN32  // Windows
#include "arrow/io/mman.h"
#undef Realloc
#undef Free
#else  // POSIX-like platforms
#include <sys/mman.h>
#include <unistd.h>
#endif

// POSIX systems do not have this
#ifndef O_BINARY
#define O_BINARY 0
#endif

// define max read/write count
#if defined(_MSC_VER)
#define ARROW_MAX_IO_CHUNKSIZE INT32_MAX
#else

#ifdef __APPLE__
// due to macOS bug, we need to set read/write max
#define ARROW_MAX_IO_CHUNKSIZE INT32_MAX
#else
// see notes on Linux read/write manpage
#define ARROW_MAX_IO_CHUNKSIZE 0x7ffff000
#endif

#endif

#include "arrow/status.h"
#include "arrow/util/io-util.h"

namespace arrow {
namespace internal {

#define CHECK_LSEEK(retval) \
  if ((retval) == -1) return Status::IOError("lseek failed");

static inline int64_t lseek64_compat(int fd, int64_t pos, int whence) {
#if defined(_MSC_VER)
  return _lseeki64(fd, pos, whence);
#else
  return lseek(fd, pos, whence);
#endif
}

static inline Status CheckFileOpResult(int ret, int errno_actual,
                                       const PlatformFilename& file_name,
                                       const char* opname) {
  if (ret == -1) {
    return Status::IOError("Failed to ", opname, " file: ", file_name.string(),
                           " , error: ", std::strerror(errno_actual));
  }
  return Status::OK();
}

//
// File name handling
//

Status FileNameFromString(const std::string& file_name, PlatformFilename* out) {
#if defined(_MSC_VER)
  try {
    std::codecvt_utf8_utf16<wchar_t> utf16_converter;
    out->assign(file_name, utf16_converter);
  } catch (boost::system::system_error& e) {
    return Status::Invalid(e.what());
  }
#else
  *out = internal::PlatformFilename(file_name);
#endif
  return Status::OK();
}

//
// Functions for creating file descriptors
//

Status FileOpenReadable(const PlatformFilename& file_name, int* fd) {
  int ret, errno_actual;
#if defined(_MSC_VER)
  errno_actual = _wsopen_s(fd, file_name.wstring().c_str(),
                           _O_RDONLY | _O_BINARY | _O_NOINHERIT, _SH_DENYNO, _S_IREAD);
  ret = *fd;
#else
  ret = *fd = open(file_name.c_str(), O_RDONLY | O_BINARY);
  errno_actual = errno;
#endif

  return CheckFileOpResult(ret, errno_actual, file_name, "open local");
}

Status FileOpenWritable(const PlatformFilename& file_name, bool write_only, bool truncate,
                        bool append, int* fd) {
  int ret, errno_actual;

#if defined(_MSC_VER)
  int oflag = _O_CREAT | _O_BINARY | _O_NOINHERIT;
  int pmode = _S_IWRITE;
  if (!write_only) {
    pmode |= _S_IREAD;
  }

  if (truncate) {
    oflag |= _O_TRUNC;
  }
  if (append) {
    oflag |= _O_APPEND;
  }

  if (write_only) {
    oflag |= _O_WRONLY;
  } else {
    oflag |= _O_RDWR;
  }

  errno_actual = _wsopen_s(fd, file_name.wstring().c_str(), oflag, _SH_DENYNO, pmode);
  ret = *fd;

#else
  int oflag = O_CREAT | O_BINARY;

  if (truncate) {
    oflag |= O_TRUNC;
  }
  if (append) {
    oflag |= O_APPEND;
  }

  if (write_only) {
    oflag |= O_WRONLY;
  } else {
    oflag |= O_RDWR;
  }

  ret = *fd = open(file_name.c_str(), oflag, ARROW_WRITE_SHMODE);
  errno_actual = errno;
#endif
  return CheckFileOpResult(ret, errno_actual, file_name, "open local");
}

Status FileTell(int fd, int64_t* pos) {
  int64_t current_pos;

#if defined(_MSC_VER)
  current_pos = _telli64(fd);
  if (current_pos == -1) {
    return Status::IOError("_telli64 failed");
  }
#else
  current_pos = lseek64_compat(fd, 0, SEEK_CUR);
  CHECK_LSEEK(current_pos);
#endif

  *pos = current_pos;
  return Status::OK();
}

Status CreatePipe(int fd[2]) {
  int ret;
#if defined(_WIN32)
  ret = _pipe(fd, 4096, _O_BINARY);
#else
  ret = pipe(fd);
#endif

  if (ret == -1) {
    return Status::IOError("Error creating pipe: ", std::strerror(errno));
  }
  return Status::OK();
}

static Status StatusFromErrno(const char* prefix) {
#ifdef _WIN32
  errno = __map_mman_error(GetLastError(), EPERM);
#endif
  return Status::IOError(prefix, std::strerror(errno));
}

//
// Compatible way to remap a memory map
//

Status MemoryMapRemap(void* addr, size_t old_size, size_t new_size, int fildes,
                      void** new_addr) {
  // should only be called with writable files
  *new_addr = MAP_FAILED;
#ifdef _WIN32
  // flags are ignored on windows
  HANDLE fm, h;

  if (!UnmapViewOfFile(addr)) {
    return StatusFromErrno("UnmapViewOfFile failed: ");
  }

  h = reinterpret_cast<HANDLE>(_get_osfhandle(fildes));
  if (h == INVALID_HANDLE_VALUE) {
    return StatusFromErrno("Cannot get file handle: ");
  }

  LONG new_size_low = static_cast<LONG>(new_size & 0xFFFFFFFFL);
  LONG new_size_high = static_cast<LONG>((new_size >> 32) & 0xFFFFFFFFL);

  SetFilePointer(h, new_size_low, &new_size_high, FILE_BEGIN);
  SetEndOfFile(h);
  fm = CreateFileMapping(h, NULL, PAGE_READWRITE, 0, 0, "");
  if (fm == NULL) {
    return StatusFromErrno("CreateFileMapping failed: ");
  }
  *new_addr = MapViewOfFile(fm, FILE_MAP_WRITE, 0, 0, new_size);
  CloseHandle(fm);
  if (new_addr == NULL) {
    return StatusFromErrno("MapViewOfFile failed: ");
  }
  return Status::OK();
#else
#ifdef __APPLE__
  // we have to close the mmap first, truncate the file to the new size
  // and recreate the mmap
  if (munmap(addr, old_size) == -1) {
    return StatusFromErrno("munmap failed: ");
  }
  if (ftruncate(fildes, new_size) == -1) {
    return StatusFromErrno("ftruncate failed: ");
  }
  // we set READ / WRITE flags on the new map, since we could only have
  // unlarged a RW map in the first place
  *new_addr = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fildes, 0);
  if (*new_addr == MAP_FAILED) {
    return StatusFromErrno("mmap failed: ");
  }
  return Status::OK();
#else
  if (ftruncate(fildes, new_size) == -1) {
    return StatusFromErrno("ftruncate failed: ");
  }
  *new_addr = mremap(addr, old_size, new_size, MREMAP_MAYMOVE);
  if (*new_addr == MAP_FAILED) {
    return StatusFromErrno("mremap failed: ");
  }
  return Status::OK();
#endif
#endif
}

//
// Closing files
//

Status FileClose(int fd) {
  int ret;

#if defined(_MSC_VER)
  ret = static_cast<int>(_close(fd));
#else
  ret = static_cast<int>(close(fd));
#endif

  if (ret == -1) {
    return Status::IOError("error closing file");
  }
  return Status::OK();
}

//
// Seeking and telling
//

Status FileSeek(int fd, int64_t pos, int whence) {
  int64_t ret = lseek64_compat(fd, pos, whence);
  CHECK_LSEEK(ret);
  return Status::OK();
}

Status FileSeek(int fd, int64_t pos) { return FileSeek(fd, pos, SEEK_SET); }

Status FileGetSize(int fd, int64_t* size) {
#if defined(_MSC_VER)
  struct __stat64 st;
#else
  struct stat st;
#endif
  st.st_size = -1;

#if defined(_MSC_VER)
  int ret = _fstat64(fd, &st);
#else
  int ret = fstat(fd, &st);
#endif

  if (ret == -1) {
    return Status::IOError("error stat()ing file");
  }
  if (st.st_size == 0) {
    // Maybe the file doesn't support getting its size, double-check by
    // trying to tell() (seekable files usually have a size, while
    // non-seekable files don't)
    int64_t position;
    RETURN_NOT_OK(FileTell(fd, &position));
  } else if (st.st_size < 0) {
    return Status::IOError("error getting file size");
  }
  *size = st.st_size;
  return Status::OK();
}

//
// Reading data
//

static inline int64_t pread_compat(int fd, void* buf, int64_t nbytes, int64_t pos) {
#if defined(_WIN32)
  HANDLE handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
  DWORD dwBytesRead = 0;
  OVERLAPPED overlapped = {0};
  overlapped.Offset = static_cast<uint32_t>(pos);
  overlapped.OffsetHigh = static_cast<uint32_t>(pos >> 32);

  // Note: ReadFile() will update the file position
  BOOL bRet =
      ReadFile(handle, buf, static_cast<uint32_t>(nbytes), &dwBytesRead, &overlapped);
  if (bRet || GetLastError() == ERROR_HANDLE_EOF) {
    return dwBytesRead;
  } else {
    return -1;
  }
#else
  return static_cast<int64_t>(
      pread(fd, buf, static_cast<size_t>(nbytes), static_cast<off_t>(pos)));
#endif
}

Status FileRead(int fd, uint8_t* buffer, int64_t nbytes, int64_t* bytes_read) {
  *bytes_read = 0;

  while (*bytes_read < nbytes) {
    int64_t chunksize =
        std::min(static_cast<int64_t>(ARROW_MAX_IO_CHUNKSIZE), nbytes - *bytes_read);
#if defined(_MSC_VER)
    int64_t ret =
        static_cast<int64_t>(_read(fd, buffer, static_cast<uint32_t>(chunksize)));
#else
    int64_t ret = static_cast<int64_t>(read(fd, buffer, static_cast<size_t>(chunksize)));
#endif

    if (ret == -1) {
      *bytes_read = ret;
      break;
    }
    if (ret == 0) {
      // EOF
      break;
    }
    buffer += ret;
    *bytes_read += ret;
  }

  if (*bytes_read == -1) {
    return Status::IOError(std::string("Error reading bytes from file: ") +
                           std::string(strerror(errno)));
  }

  return Status::OK();
}

Status FileReadAt(int fd, uint8_t* buffer, int64_t position, int64_t nbytes,
                  int64_t* bytes_read) {
  *bytes_read = 0;

  while (*bytes_read < nbytes) {
    int64_t chunksize =
        std::min(static_cast<int64_t>(ARROW_MAX_IO_CHUNKSIZE), nbytes - *bytes_read);
    int64_t ret = pread_compat(fd, buffer, chunksize, position);

    if (ret == -1) {
      *bytes_read = ret;
      break;
    }
    if (ret == 0) {
      // EOF
      break;
    }
    buffer += ret;
    position += ret;
    *bytes_read += ret;
  }

  if (*bytes_read == -1) {
    return Status::IOError(std::string("Error reading bytes from file: ") +
                           std::string(strerror(errno)));
  }
  return Status::OK();
}

//
// Writing data
//

Status FileWrite(int fd, const uint8_t* buffer, const int64_t nbytes) {
  int ret = 0;
  int64_t bytes_written = 0;

  while (ret != -1 && bytes_written < nbytes) {
    int64_t chunksize =
        std::min(static_cast<int64_t>(ARROW_MAX_IO_CHUNKSIZE), nbytes - bytes_written);
#if defined(_MSC_VER)
    ret = static_cast<int>(
        _write(fd, buffer + bytes_written, static_cast<uint32_t>(chunksize)));
#else
    ret = static_cast<int>(
        write(fd, buffer + bytes_written, static_cast<size_t>(chunksize)));
#endif

    if (ret != -1) {
      bytes_written += ret;
    }
  }

  if (ret == -1) {
    return Status::IOError(std::string("Error writing bytes from file: ") +
                           std::string(strerror(errno)));
  }
  return Status::OK();
}

Status FileTruncate(int fd, const int64_t size) {
  int ret, errno_actual;

#ifdef _WIN32
  errno_actual = _chsize_s(fd, static_cast<size_t>(size));
  ret = errno_actual == 0 ? 0 : -1;
#else
  ret = ftruncate(fd, static_cast<size_t>(size));
  errno_actual = errno;
#endif

  if (ret == -1) {
    return Status::IOError(std::string("Error truncating file: ") +
                           std::string(strerror(errno_actual)));
  }
  return Status::OK();
}

//
// Environment variables
//

Status GetEnvVar(const char* name, std::string* out) {
#ifdef _WIN32
  // On Windows, getenv() reads an early copy of the process' environment
  // which doesn't get updated when SetEnvironmentVariable() is called.
  constexpr int32_t bufsize = 2000;
  char c_str[bufsize];
  auto res = GetEnvironmentVariableA(name, c_str, bufsize);
  if (res >= bufsize) {
    return Status::CapacityError("environment variable value too long");
  } else if (res == 0) {
    return Status::KeyError("environment variable undefined");
  }
  *out = std::string(c_str);
  return Status::OK();
#else
  char* c_str = getenv(name);
  if (c_str == nullptr) {
    return Status::KeyError("environment variable undefined");
  }
  *out = std::string(c_str);
  return Status::OK();
#endif
}

Status GetEnvVar(const std::string& name, std::string* out) {
  return GetEnvVar(name.c_str(), out);
}

Status SetEnvVar(const char* name, const char* value) {
#ifdef _WIN32
  if (SetEnvironmentVariableA(name, value)) {
    return Status::OK();
  } else {
    return Status::Invalid("failed setting environment variable");
  }
#else
  if (setenv(name, value, 1) == 0) {
    return Status::OK();
  } else {
    return Status::Invalid("failed setting environment variable");
  }
#endif
}

Status SetEnvVar(const std::string& name, const std::string& value) {
  return SetEnvVar(name.c_str(), value.c_str());
}

Status DelEnvVar(const char* name) {
#ifdef _WIN32
  if (SetEnvironmentVariableA(name, nullptr)) {
    return Status::OK();
  } else {
    return Status::Invalid("failed deleting environment variable");
  }
#else
  if (unsetenv(name) == 0) {
    return Status::OK();
  } else {
    return Status::Invalid("failed deleting environment variable");
  }
#endif
}

Status DelEnvVar(const std::string& name) { return DelEnvVar(name.c_str()); }

}  // namespace internal
}  // namespace arrow
