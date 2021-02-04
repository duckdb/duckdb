#include "duckdb.hpp"
#include "httpfs-extension.hpp"

#include "s3fs.hpp"
namespace duckdb {

void HTTPFsExtension::Load(DuckDB &db) {
	S3FileSystem::Verify(); // run some tests to see if all the hashes work out

	db.instance->GetFileSystem().RegisterProtocolHandler("s3://", make_unique<S3FileSystem>());
}

} // namespace duckdb
