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
	config.extension_parameters["s3_region"] = LogicalType::VARCHAR;
	config.extension_parameters["s3_access_key_id"] = LogicalType::VARCHAR;
	config.extension_parameters["s3_secret_access_key"] = LogicalType::VARCHAR;
	config.extension_parameters["s3_session_token"] = LogicalType::VARCHAR;
	config.extension_parameters["s3_endpoint"] = LogicalType::VARCHAR;
}

} // namespace duckdb
