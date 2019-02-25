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

#ifndef ARROW_IO_HDFS_INTERNAL
#define ARROW_IO_HDFS_INTERNAL

#include <cstddef>
#include <cstdint>

#include <hdfs.h>

#include "arrow/util/visibility.h"
#include "arrow/util/windows_compatibility.h"  // IWYU pragma: keep

using std::size_t;

struct hdfsBuilder;

namespace arrow {

class Status;

namespace io {
namespace internal {

// NOTE(wesm): cpplint does not like use of short and other imprecise C types
struct LibHdfsShim {
#ifndef _WIN32
  void* handle;
#else
  HINSTANCE handle;
#endif

  hdfsBuilder* (*hdfsNewBuilder)(void);
  void (*hdfsBuilderSetNameNode)(hdfsBuilder* bld, const char* nn);
  void (*hdfsBuilderSetNameNodePort)(hdfsBuilder* bld, tPort port);
  void (*hdfsBuilderSetUserName)(hdfsBuilder* bld, const char* userName);
  void (*hdfsBuilderSetKerbTicketCachePath)(hdfsBuilder* bld,
                                            const char* kerbTicketCachePath);
  void (*hdfsBuilderSetForceNewInstance)(hdfsBuilder* bld);
  hdfsFS (*hdfsBuilderConnect)(hdfsBuilder* bld);
  int (*hdfsBuilderConfSetStr)(hdfsBuilder* bld, const char* key, const char* val);

  int (*hdfsDisconnect)(hdfsFS fs);

  hdfsFile (*hdfsOpenFile)(hdfsFS fs, const char* path, int flags, int bufferSize,
                           short replication, tSize blocksize);  // NOLINT

  int (*hdfsCloseFile)(hdfsFS fs, hdfsFile file);
  int (*hdfsExists)(hdfsFS fs, const char* path);
  int (*hdfsSeek)(hdfsFS fs, hdfsFile file, tOffset desiredPos);
  tOffset (*hdfsTell)(hdfsFS fs, hdfsFile file);
  tSize (*hdfsRead)(hdfsFS fs, hdfsFile file, void* buffer, tSize length);
  tSize (*hdfsPread)(hdfsFS fs, hdfsFile file, tOffset position, void* buffer,
                     tSize length);
  tSize (*hdfsWrite)(hdfsFS fs, hdfsFile file, const void* buffer, tSize length);
  int (*hdfsFlush)(hdfsFS fs, hdfsFile file);
  int (*hdfsAvailable)(hdfsFS fs, hdfsFile file);
  int (*hdfsCopy)(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);
  int (*hdfsMove)(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);
  int (*hdfsDelete)(hdfsFS fs, const char* path, int recursive);
  int (*hdfsRename)(hdfsFS fs, const char* oldPath, const char* newPath);
  char* (*hdfsGetWorkingDirectory)(hdfsFS fs, char* buffer, size_t bufferSize);
  int (*hdfsSetWorkingDirectory)(hdfsFS fs, const char* path);
  int (*hdfsCreateDirectory)(hdfsFS fs, const char* path);
  int (*hdfsSetReplication)(hdfsFS fs, const char* path, int16_t replication);
  hdfsFileInfo* (*hdfsListDirectory)(hdfsFS fs, const char* path, int* numEntries);
  hdfsFileInfo* (*hdfsGetPathInfo)(hdfsFS fs, const char* path);
  void (*hdfsFreeFileInfo)(hdfsFileInfo* hdfsFileInfo, int numEntries);
  char*** (*hdfsGetHosts)(hdfsFS fs, const char* path, tOffset start, tOffset length);
  void (*hdfsFreeHosts)(char*** blockHosts);
  tOffset (*hdfsGetDefaultBlockSize)(hdfsFS fs);
  tOffset (*hdfsGetCapacity)(hdfsFS fs);
  tOffset (*hdfsGetUsed)(hdfsFS fs);
  int (*hdfsChown)(hdfsFS fs, const char* path, const char* owner, const char* group);
  int (*hdfsChmod)(hdfsFS fs, const char* path, short mode);  // NOLINT
  int (*hdfsUtime)(hdfsFS fs, const char* path, tTime mtime, tTime atime);

  void Initialize() {
    this->handle = nullptr;
    this->hdfsNewBuilder = nullptr;
    this->hdfsBuilderSetNameNode = nullptr;
    this->hdfsBuilderSetNameNodePort = nullptr;
    this->hdfsBuilderSetUserName = nullptr;
    this->hdfsBuilderSetKerbTicketCachePath = nullptr;
    this->hdfsBuilderSetForceNewInstance = nullptr;
    this->hdfsBuilderConfSetStr = nullptr;
    this->hdfsBuilderConnect = nullptr;
    this->hdfsDisconnect = nullptr;
    this->hdfsOpenFile = nullptr;
    this->hdfsCloseFile = nullptr;
    this->hdfsExists = nullptr;
    this->hdfsSeek = nullptr;
    this->hdfsTell = nullptr;
    this->hdfsRead = nullptr;
    this->hdfsPread = nullptr;
    this->hdfsWrite = nullptr;
    this->hdfsFlush = nullptr;
    this->hdfsAvailable = nullptr;
    this->hdfsCopy = nullptr;
    this->hdfsMove = nullptr;
    this->hdfsDelete = nullptr;
    this->hdfsRename = nullptr;
    this->hdfsGetWorkingDirectory = nullptr;
    this->hdfsSetWorkingDirectory = nullptr;
    this->hdfsCreateDirectory = nullptr;
    this->hdfsSetReplication = nullptr;
    this->hdfsListDirectory = nullptr;
    this->hdfsGetPathInfo = nullptr;
    this->hdfsFreeFileInfo = nullptr;
    this->hdfsGetHosts = nullptr;
    this->hdfsFreeHosts = nullptr;
    this->hdfsGetDefaultBlockSize = nullptr;
    this->hdfsGetCapacity = nullptr;
    this->hdfsGetUsed = nullptr;
    this->hdfsChown = nullptr;
    this->hdfsChmod = nullptr;
    this->hdfsUtime = nullptr;
  }

  hdfsBuilder* NewBuilder(void);

  void BuilderSetNameNode(hdfsBuilder* bld, const char* nn);

  void BuilderSetNameNodePort(hdfsBuilder* bld, tPort port);

  void BuilderSetUserName(hdfsBuilder* bld, const char* userName);

  void BuilderSetKerbTicketCachePath(hdfsBuilder* bld, const char* kerbTicketCachePath);

  void BuilderSetForceNewInstance(hdfsBuilder* bld);

  int BuilderConfSetStr(hdfsBuilder* bld, const char* key, const char* val);

  hdfsFS BuilderConnect(hdfsBuilder* bld);

  int Disconnect(hdfsFS fs);

  hdfsFile OpenFile(hdfsFS fs, const char* path, int flags, int bufferSize,
                    short replication, tSize blocksize);  // NOLINT

  int CloseFile(hdfsFS fs, hdfsFile file);

  int Exists(hdfsFS fs, const char* path);

  int Seek(hdfsFS fs, hdfsFile file, tOffset desiredPos);

  tOffset Tell(hdfsFS fs, hdfsFile file);

  tSize Read(hdfsFS fs, hdfsFile file, void* buffer, tSize length);

  bool HasPread();

  tSize Pread(hdfsFS fs, hdfsFile file, tOffset position, void* buffer, tSize length);

  tSize Write(hdfsFS fs, hdfsFile file, const void* buffer, tSize length);

  int Flush(hdfsFS fs, hdfsFile file);

  int Available(hdfsFS fs, hdfsFile file);

  int Copy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);

  int Move(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);

  int Delete(hdfsFS fs, const char* path, int recursive);

  int Rename(hdfsFS fs, const char* oldPath, const char* newPath);

  char* GetWorkingDirectory(hdfsFS fs, char* buffer, size_t bufferSize);

  int SetWorkingDirectory(hdfsFS fs, const char* path);

  int MakeDirectory(hdfsFS fs, const char* path);

  int SetReplication(hdfsFS fs, const char* path, int16_t replication);

  hdfsFileInfo* ListDirectory(hdfsFS fs, const char* path, int* numEntries);

  hdfsFileInfo* GetPathInfo(hdfsFS fs, const char* path);

  void FreeFileInfo(hdfsFileInfo* hdfsFileInfo, int numEntries);

  char*** GetHosts(hdfsFS fs, const char* path, tOffset start, tOffset length);

  void FreeHosts(char*** blockHosts);

  tOffset GetDefaultBlockSize(hdfsFS fs);
  tOffset GetCapacity(hdfsFS fs);

  tOffset GetUsed(hdfsFS fs);

  int Chown(hdfsFS fs, const char* path, const char* owner, const char* group);

  int Chmod(hdfsFS fs, const char* path, short mode);  // NOLINT

  int Utime(hdfsFS fs, const char* path, tTime mtime, tTime atime);

  Status GetRequiredSymbols();
};

// TODO(wesm): Remove these exports when we are linking statically
Status ARROW_EXPORT ConnectLibHdfs(LibHdfsShim** driver);
Status ARROW_EXPORT ConnectLibHdfs3(LibHdfsShim** driver);

}  // namespace internal
}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_HDFS_INTERNAL
