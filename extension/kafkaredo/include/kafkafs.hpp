#pragma once
#include "duckdb/common/file_system.hpp"
#include <string>
#include <librdkafka/rdkafka.h>
 
namespace duckdb {
  class KafkaFileHandle : public FileHandle {
  public:
    KafkaFileHandle(FileSystem &fs, string path, string topic, uint8_t flags);
    ~KafkaFileHandle() override;
    void Close() override {
    }
    //private:
    rd_kafka_conf_t *conf;
    rd_kafka_t *rk;
  };
  class KafkaFileSystem : public FileSystem {
    string bootstrap_server;
    string topic;
	bool is_writer;
  public:
    string GetName() const override {
      return "KafkaFileSystem";
    }
    bool FileExists(const string& filename) override{return true;}
    void Truncate(FileHandle &handle, int64_t new_size) override;
    bool CanHandleFile(const string &fpath) override;
    duckdb::unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = DEFAULT_LOCK,
					    FileCompressionType compression = DEFAULT_COMPRESSION,
					    FileOpener *opener = nullptr) final;
    // FS methods
    void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
    int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
    void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
    int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
    void FileSync(FileHandle &handle) override;
    int64_t GetFileSize(FileHandle &handle) override;
  };
} // namespace duckdb
