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

// This shim interface to libhdfs (for runtime shared library loading) has been
// adapted from the SFrame project, released under the ASF-compatible 3-clause
// BSD license
//
// Using this required having the $JAVA_HOME and $HADOOP_HOME environment
// variables set, so that libjvm and libhdfs can be located easily

// Copyright (C) 2015 Dato, Inc.
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

#include "arrow/io/hdfs-internal.h"

#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <vector>

#ifndef _WIN32
#include <dlfcn.h>
#endif

#include <boost/filesystem.hpp>  // NOLINT

#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace fs = boost::filesystem;

#ifndef _WIN32
static void* libjvm_handle = NULL;
#else
static HINSTANCE libjvm_handle = NULL;
#endif
/*
 * All the shim pointers
 */

// Helper functions for dlopens
static std::vector<fs::path> get_potential_libjvm_paths();
static std::vector<fs::path> get_potential_libhdfs_paths();
static std::vector<fs::path> get_potential_libhdfs3_paths();
static arrow::Status try_dlopen(std::vector<fs::path> potential_paths, const char* name,
#ifndef _WIN32
                                void*& out_handle);
#else
                                HINSTANCE& out_handle);
#endif

static std::vector<fs::path> get_potential_libhdfs_paths() {
  std::vector<fs::path> libhdfs_potential_paths;
  std::string file_name;

// OS-specific file name
#ifdef _WIN32
  file_name = "hdfs.dll";
#elif __APPLE__
  file_name = "libhdfs.dylib";
#else
  file_name = "libhdfs.so";
#endif

  // Common paths
  std::vector<fs::path> search_paths = {fs::path(""), fs::path(".")};

  // Path from environment variable
  const char* hadoop_home = std::getenv("HADOOP_HOME");
  if (hadoop_home != nullptr) {
    auto path = fs::path(hadoop_home) / "lib/native";
    search_paths.push_back(path);
  }

  const char* libhdfs_dir = std::getenv("ARROW_LIBHDFS_DIR");
  if (libhdfs_dir != nullptr) {
    search_paths.push_back(fs::path(libhdfs_dir));
  }

  // All paths with file name
  for (auto& path : search_paths) {
    libhdfs_potential_paths.push_back(path / file_name);
  }

  return libhdfs_potential_paths;
}

static std::vector<fs::path> get_potential_libhdfs3_paths() {
  std::vector<fs::path> potential_paths;
  std::string file_name;

// OS-specific file name
#ifdef _WIN32
  file_name = "hdfs3.dll";
#elif __APPLE__
  file_name = "libhdfs3.dylib";
#else
  file_name = "libhdfs3.so";
#endif

  // Common paths
  std::vector<fs::path> search_paths = {fs::path(""), fs::path(".")};

  const char* libhdfs3_dir = std::getenv("ARROW_LIBHDFS3_DIR");
  if (libhdfs3_dir != nullptr) {
    search_paths.push_back(fs::path(libhdfs3_dir));
  }

  // All paths with file name
  for (auto& path : search_paths) {
    potential_paths.push_back(path / file_name);
  }

  return potential_paths;
}

static std::vector<fs::path> get_potential_libjvm_paths() {
  std::vector<fs::path> libjvm_potential_paths;

  std::vector<fs::path> search_prefixes;
  std::vector<fs::path> search_suffixes;
  std::string file_name;

// From heuristics
#ifdef __WIN32
  search_prefixes = {""};
  search_suffixes = {"/jre/bin/server", "/bin/server"};
  file_name = "jvm.dll";
#elif __APPLE__
  search_prefixes = {""};
  search_suffixes = {"", "/jre/lib/server", "/lib/server"};
  file_name = "libjvm.dylib";

// SFrame uses /usr/libexec/java_home to find JAVA_HOME; for now we are
// expecting users to set an environment variable
#else
  search_prefixes = {
      "/usr/lib/jvm/default-java",                // ubuntu / debian distros
      "/usr/lib/jvm/java",                        // rhel6
      "/usr/lib/jvm",                             // centos6
      "/usr/lib64/jvm",                           // opensuse 13
      "/usr/local/lib/jvm/default-java",          // alt ubuntu / debian distros
      "/usr/local/lib/jvm/java",                  // alt rhel6
      "/usr/local/lib/jvm",                       // alt centos6
      "/usr/local/lib64/jvm",                     // alt opensuse 13
      "/usr/local/lib/jvm/java-7-openjdk-amd64",  // alt ubuntu / debian distros
      "/usr/lib/jvm/java-7-openjdk-amd64",        // alt ubuntu / debian distros
      "/usr/local/lib/jvm/java-6-openjdk-amd64",  // alt ubuntu / debian distros
      "/usr/lib/jvm/java-6-openjdk-amd64",        // alt ubuntu / debian distros
      "/usr/lib/jvm/java-7-oracle",               // alt ubuntu
      "/usr/lib/jvm/java-8-oracle",               // alt ubuntu
      "/usr/lib/jvm/java-6-oracle",               // alt ubuntu
      "/usr/local/lib/jvm/java-7-oracle",         // alt ubuntu
      "/usr/local/lib/jvm/java-8-oracle",         // alt ubuntu
      "/usr/local/lib/jvm/java-6-oracle",         // alt ubuntu
      "/usr/lib/jvm/default",                     // alt centos
      "/usr/java/latest",                         // alt centos
  };
  search_suffixes = {"", "/jre/lib/amd64/server", "/lib/amd64/server"};
  file_name = "libjvm.so";
#endif
  // From direct environment variable
  char* env_value = NULL;
  if ((env_value = getenv("JAVA_HOME")) != NULL) {
    search_prefixes.insert(search_prefixes.begin(), env_value);
  }

  // Generate cross product between search_prefixes, search_suffixes, and file_name
  for (auto& prefix : search_prefixes) {
    for (auto& suffix : search_suffixes) {
      auto path = (fs::path(prefix) / fs::path(suffix) / fs::path(file_name));
      libjvm_potential_paths.push_back(path);
    }
  }

  return libjvm_potential_paths;
}

#ifndef _WIN32
static arrow::Status try_dlopen(std::vector<fs::path> potential_paths, const char* name,
                                void*& out_handle) {
  std::vector<std::string> error_messages;

  for (auto& i : potential_paths) {
    i.make_preferred();
    out_handle = dlopen(i.native().c_str(), RTLD_NOW | RTLD_LOCAL);

    if (out_handle != NULL) {
      // std::cout << "Loaded " << i << std::endl;
      break;
    } else {
      const char* err_msg = dlerror();
      if (err_msg != NULL) {
        error_messages.push_back(std::string(err_msg));
      } else {
        error_messages.push_back(std::string(" returned NULL"));
      }
    }
  }

  if (out_handle == NULL) {
    return arrow::Status::IOError("Unable to load ", name);
  }

  return arrow::Status::OK();
}

#else
static arrow::Status try_dlopen(std::vector<fs::path> potential_paths, const char* name,
                                HINSTANCE& out_handle) {
  std::vector<std::string> error_messages;

  for (auto& i : potential_paths) {
    i.make_preferred();
    out_handle = LoadLibrary(i.string().c_str());

    if (out_handle != NULL) {
      break;
    } else {
      // error_messages.push_back(get_last_err_str(GetLastError()));
    }
  }

  if (out_handle == NULL) {
    return arrow::Status::IOError("Unable to load ", name);
  }

  return arrow::Status::OK();
}
#endif  // _WIN32

static inline void* GetLibrarySymbol(void* handle, const char* symbol) {
  if (handle == NULL) return NULL;
#ifndef _WIN32
  return dlsym(handle, symbol);
#else

  void* ret = reinterpret_cast<void*>(
      GetProcAddress(reinterpret_cast<HINSTANCE>(handle), symbol));
  if (ret == NULL) {
    // logstream(LOG_INFO) << "GetProcAddress error: "
    //                     << get_last_err_str(GetLastError()) << std::endl;
  }
  return ret;
#endif
}

#define GET_SYMBOL_REQUIRED(SHIM, SYMBOL_NAME)                         \
  do {                                                                 \
    if (!SHIM->SYMBOL_NAME) {                                          \
      *reinterpret_cast<void**>(&SHIM->SYMBOL_NAME) =                  \
          GetLibrarySymbol(SHIM->handle, "" #SYMBOL_NAME);             \
    }                                                                  \
    if (!SHIM->SYMBOL_NAME)                                            \
      return Status::IOError("Getting symbol " #SYMBOL_NAME "failed"); \
  } while (0)

#define GET_SYMBOL(SHIM, SYMBOL_NAME)                    \
  if (!SHIM->SYMBOL_NAME) {                              \
    *reinterpret_cast<void**>(&SHIM->SYMBOL_NAME) =      \
        GetLibrarySymbol(SHIM->handle, "" #SYMBOL_NAME); \
  }

namespace arrow {
namespace io {
namespace internal {

static LibHdfsShim libhdfs_shim;
static LibHdfsShim libhdfs3_shim;

hdfsBuilder* LibHdfsShim::NewBuilder(void) { return this->hdfsNewBuilder(); }

void LibHdfsShim::BuilderSetNameNode(hdfsBuilder* bld, const char* nn) {
  this->hdfsBuilderSetNameNode(bld, nn);
}

void LibHdfsShim::BuilderSetNameNodePort(hdfsBuilder* bld, tPort port) {
  this->hdfsBuilderSetNameNodePort(bld, port);
}

void LibHdfsShim::BuilderSetUserName(hdfsBuilder* bld, const char* userName) {
  this->hdfsBuilderSetUserName(bld, userName);
}

void LibHdfsShim::BuilderSetKerbTicketCachePath(hdfsBuilder* bld,
                                                const char* kerbTicketCachePath) {
  this->hdfsBuilderSetKerbTicketCachePath(bld, kerbTicketCachePath);
}

void LibHdfsShim::BuilderSetForceNewInstance(hdfsBuilder* bld) {
  this->hdfsBuilderSetForceNewInstance(bld);
}

hdfsFS LibHdfsShim::BuilderConnect(hdfsBuilder* bld) {
  return this->hdfsBuilderConnect(bld);
}

int LibHdfsShim::BuilderConfSetStr(hdfsBuilder* bld, const char* key, const char* val) {
  return this->hdfsBuilderConfSetStr(bld, key, val);
}

int LibHdfsShim::Disconnect(hdfsFS fs) { return this->hdfsDisconnect(fs); }

hdfsFile LibHdfsShim::OpenFile(hdfsFS fs, const char* path, int flags, int bufferSize,
                               short replication, tSize blocksize) {  // NOLINT
  return this->hdfsOpenFile(fs, path, flags, bufferSize, replication, blocksize);
}

int LibHdfsShim::CloseFile(hdfsFS fs, hdfsFile file) {
  return this->hdfsCloseFile(fs, file);
}

int LibHdfsShim::Exists(hdfsFS fs, const char* path) {
  return this->hdfsExists(fs, path);
}

int LibHdfsShim::Seek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
  return this->hdfsSeek(fs, file, desiredPos);
}

tOffset LibHdfsShim::Tell(hdfsFS fs, hdfsFile file) { return this->hdfsTell(fs, file); }

tSize LibHdfsShim::Read(hdfsFS fs, hdfsFile file, void* buffer, tSize length) {
  return this->hdfsRead(fs, file, buffer, length);
}

bool LibHdfsShim::HasPread() {
  GET_SYMBOL(this, hdfsPread);
  return this->hdfsPread != nullptr;
}

tSize LibHdfsShim::Pread(hdfsFS fs, hdfsFile file, tOffset position, void* buffer,
                         tSize length) {
  GET_SYMBOL(this, hdfsPread);
  DCHECK(this->hdfsPread);
  return this->hdfsPread(fs, file, position, buffer, length);
}

tSize LibHdfsShim::Write(hdfsFS fs, hdfsFile file, const void* buffer, tSize length) {
  return this->hdfsWrite(fs, file, buffer, length);
}

int LibHdfsShim::Flush(hdfsFS fs, hdfsFile file) { return this->hdfsFlush(fs, file); }

int LibHdfsShim::Available(hdfsFS fs, hdfsFile file) {
  GET_SYMBOL(this, hdfsAvailable);
  if (this->hdfsAvailable)
    return this->hdfsAvailable(fs, file);
  else
    return 0;
}

int LibHdfsShim::Copy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  GET_SYMBOL(this, hdfsCopy);
  if (this->hdfsCopy)
    return this->hdfsCopy(srcFS, src, dstFS, dst);
  else
    return 0;
}

int LibHdfsShim::Move(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  GET_SYMBOL(this, hdfsMove);
  if (this->hdfsMove)
    return this->hdfsMove(srcFS, src, dstFS, dst);
  else
    return 0;
}

int LibHdfsShim::Delete(hdfsFS fs, const char* path, int recursive) {
  return this->hdfsDelete(fs, path, recursive);
}

int LibHdfsShim::Rename(hdfsFS fs, const char* oldPath, const char* newPath) {
  GET_SYMBOL(this, hdfsRename);
  if (this->hdfsRename)
    return this->hdfsRename(fs, oldPath, newPath);
  else
    return 0;
}

char* LibHdfsShim::GetWorkingDirectory(hdfsFS fs, char* buffer, size_t bufferSize) {
  GET_SYMBOL(this, hdfsGetWorkingDirectory);
  if (this->hdfsGetWorkingDirectory) {
    return this->hdfsGetWorkingDirectory(fs, buffer, bufferSize);
  } else {
    return NULL;
  }
}

int LibHdfsShim::SetWorkingDirectory(hdfsFS fs, const char* path) {
  GET_SYMBOL(this, hdfsSetWorkingDirectory);
  if (this->hdfsSetWorkingDirectory) {
    return this->hdfsSetWorkingDirectory(fs, path);
  } else {
    return 0;
  }
}

int LibHdfsShim::MakeDirectory(hdfsFS fs, const char* path) {
  return this->hdfsCreateDirectory(fs, path);
}

int LibHdfsShim::SetReplication(hdfsFS fs, const char* path, int16_t replication) {
  GET_SYMBOL(this, hdfsSetReplication);
  if (this->hdfsSetReplication) {
    return this->hdfsSetReplication(fs, path, replication);
  } else {
    return 0;
  }
}

hdfsFileInfo* LibHdfsShim::ListDirectory(hdfsFS fs, const char* path, int* numEntries) {
  return this->hdfsListDirectory(fs, path, numEntries);
}

hdfsFileInfo* LibHdfsShim::GetPathInfo(hdfsFS fs, const char* path) {
  return this->hdfsGetPathInfo(fs, path);
}

void LibHdfsShim::FreeFileInfo(hdfsFileInfo* hdfsFileInfo, int numEntries) {
  this->hdfsFreeFileInfo(hdfsFileInfo, numEntries);
}

char*** LibHdfsShim::GetHosts(hdfsFS fs, const char* path, tOffset start,
                              tOffset length) {
  GET_SYMBOL(this, hdfsGetHosts);
  if (this->hdfsGetHosts) {
    return this->hdfsGetHosts(fs, path, start, length);
  } else {
    return NULL;
  }
}

void LibHdfsShim::FreeHosts(char*** blockHosts) {
  GET_SYMBOL(this, hdfsFreeHosts);
  if (this->hdfsFreeHosts) {
    this->hdfsFreeHosts(blockHosts);
  }
}

tOffset LibHdfsShim::GetDefaultBlockSize(hdfsFS fs) {
  GET_SYMBOL(this, hdfsGetDefaultBlockSize);
  if (this->hdfsGetDefaultBlockSize) {
    return this->hdfsGetDefaultBlockSize(fs);
  } else {
    return 0;
  }
}

tOffset LibHdfsShim::GetCapacity(hdfsFS fs) { return this->hdfsGetCapacity(fs); }

tOffset LibHdfsShim::GetUsed(hdfsFS fs) { return this->hdfsGetUsed(fs); }

int LibHdfsShim::Chown(hdfsFS fs, const char* path, const char* owner,
                       const char* group) {
  return this->hdfsChown(fs, path, owner, group);
}

int LibHdfsShim::Chmod(hdfsFS fs, const char* path, short mode) {  // NOLINT
  return this->hdfsChmod(fs, path, mode);
}

int LibHdfsShim::Utime(hdfsFS fs, const char* path, tTime mtime, tTime atime) {
  GET_SYMBOL(this, hdfsUtime);
  if (this->hdfsUtime) {
    return this->hdfsUtime(fs, path, mtime, atime);
  } else {
    return 0;
  }
}

Status LibHdfsShim::GetRequiredSymbols() {
  GET_SYMBOL_REQUIRED(this, hdfsNewBuilder);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderSetNameNode);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderSetNameNodePort);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderSetUserName);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderSetKerbTicketCachePath);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderSetForceNewInstance);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderConfSetStr);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderConnect);
  GET_SYMBOL_REQUIRED(this, hdfsCreateDirectory);
  GET_SYMBOL_REQUIRED(this, hdfsDelete);
  GET_SYMBOL_REQUIRED(this, hdfsDisconnect);
  GET_SYMBOL_REQUIRED(this, hdfsExists);
  GET_SYMBOL_REQUIRED(this, hdfsFreeFileInfo);
  GET_SYMBOL_REQUIRED(this, hdfsGetCapacity);
  GET_SYMBOL_REQUIRED(this, hdfsGetUsed);
  GET_SYMBOL_REQUIRED(this, hdfsGetPathInfo);
  GET_SYMBOL_REQUIRED(this, hdfsListDirectory);
  GET_SYMBOL_REQUIRED(this, hdfsChown);
  GET_SYMBOL_REQUIRED(this, hdfsChmod);

  // File methods
  GET_SYMBOL_REQUIRED(this, hdfsCloseFile);
  GET_SYMBOL_REQUIRED(this, hdfsFlush);
  GET_SYMBOL_REQUIRED(this, hdfsOpenFile);
  GET_SYMBOL_REQUIRED(this, hdfsRead);
  GET_SYMBOL_REQUIRED(this, hdfsSeek);
  GET_SYMBOL_REQUIRED(this, hdfsTell);
  GET_SYMBOL_REQUIRED(this, hdfsWrite);

  return Status::OK();
}

Status ConnectLibHdfs(LibHdfsShim** driver) {
  static std::mutex lock;
  std::lock_guard<std::mutex> guard(lock);

  LibHdfsShim* shim = &libhdfs_shim;

  static bool shim_attempted = false;
  if (!shim_attempted) {
    shim_attempted = true;

    shim->Initialize();

    std::vector<fs::path> libjvm_potential_paths = get_potential_libjvm_paths();
    RETURN_NOT_OK(try_dlopen(libjvm_potential_paths, "libjvm", libjvm_handle));

    std::vector<fs::path> libhdfs_potential_paths = get_potential_libhdfs_paths();
    RETURN_NOT_OK(try_dlopen(libhdfs_potential_paths, "libhdfs", shim->handle));
  } else if (shim->handle == nullptr) {
    return Status::IOError("Prior attempt to load libhdfs failed");
  }

  *driver = shim;
  return shim->GetRequiredSymbols();
}

Status ConnectLibHdfs3(LibHdfsShim** driver) {
  static std::mutex lock;
  std::lock_guard<std::mutex> guard(lock);

  LibHdfsShim* shim = &libhdfs3_shim;

  static bool shim_attempted = false;
  if (!shim_attempted) {
    shim_attempted = true;

    shim->Initialize();

    std::vector<fs::path> libhdfs3_potential_paths = get_potential_libhdfs3_paths();
    RETURN_NOT_OK(try_dlopen(libhdfs3_potential_paths, "libhdfs3", shim->handle));
  } else if (shim->handle == nullptr) {
    return Status::IOError("Prior attempt to load libhdfs3 failed");
  }

  *driver = shim;
  return shim->GetRequiredSymbols();
}

}  // namespace internal
}  // namespace io
}  // namespace arrow
