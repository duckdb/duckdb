#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "httpfs-extension.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include "s3fs.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	S3FileSystem::Verify(); // run some tests to see if all the hashes work out
	auto &fs = instance.GetFileSystem();

	fs.RegisterSubSystem(make_unique<HTTPFileSystem>());
	fs.RegisterSubSystem(make_unique<S3FileSystem>(*instance.GetStorageManager().buffer_manager));

	auto &config = DBConfig::GetConfig(instance);

	// Global S3 config
	config.AddExtensionOption("s3_region", "S3 Region", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_access_key_id", "S3 Access Key ID", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_secret_access_key", "S3 Access Key", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_session_token", "S3 Session Token", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_endpoint", "S3 Endpoint (default s3.amazonaws.com)", LogicalType::VARCHAR);

	// S3 Uploader config
	config.AddExtensionOption("s3_uploader_max_filesize", "S3 Uploader max filesize (default 52GB)", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_uploader_max_parts_per_file", "S3 Uploader max parts per file (default 10000)", LogicalType::UBIGINT);
	config.AddExtensionOption("s3_uploader_timeout", "S3 Uploader part upload timeout (default 30000ms)", LogicalType::UBIGINT);
	config.AddExtensionOption("s3_uploader_thread_limit", "S3 Uploader global thread limit (default 100)", LogicalType::UBIGINT);
}

void HTTPFsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string HTTPFsExtension::Name() {
	return "httpfs";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void httpfs_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *httpfs_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
