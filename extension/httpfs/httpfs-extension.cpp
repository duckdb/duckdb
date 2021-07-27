#include "duckdb.hpp"
#include "httpfs-extension.hpp"

#include "s3fs.hpp"
namespace duckdb {

void HTTPFsExtension::Load(DuckDB &db) {
	S3FileSystem::Verify(); // run some tests to see if all the hashes work out
	auto &fs = db.instance->GetFileSystem();
	fs.RegisterSubSystem(make_unique<HTTPFileSystem>());
	fs.RegisterSubSystem(make_unique<S3FileSystem>(*db.instance));
}

} // namespace duckdb
