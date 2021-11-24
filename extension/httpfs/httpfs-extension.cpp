#include "duckdb.hpp"
#include "httpfs-extension.hpp"

#include "s3fs.hpp"
namespace duckdb {

void HTTPFsExtension::Load(DuckDB &db) {
	S3FileSystem::Verify(); // run some tests to see if all the hashes work out
	auto &fs = db.instance->GetFileSystem();
	fs.RegisterSubSystem(make_unique<HTTPFileSystem>());
	fs.RegisterSubSystem(make_unique<S3FileSystem>());

	auto &config = DBConfig::GetConfig(*db.instance);
	config.AddExtensionOption("s3_region", "S3 Region", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_access_key_id", "S3 Access Key ID", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_secret_access_key", "S3 Access Key", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_session_token", "S3 Session Token", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_endpoint", "S3 Endpoint (default s3.amazonaws.com)", LogicalType::VARCHAR);
}

} // namespace duckdb
