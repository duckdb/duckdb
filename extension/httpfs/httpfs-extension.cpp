#include "duckdb.hpp"
#include "httpfs-extension.hpp"

#include "s3fs.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	S3FileSystem::Verify(); // run some tests to see if all the hashes work out
	auto &fs = instance.GetFileSystem();
	fs.RegisterSubSystem(make_unique<HTTPFileSystem>());
	fs.RegisterSubSystem(make_unique<S3FileSystem>());
}

void HTTPFsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string HTTPFsExtension::Name() {
	return "httpfs";
}

} // namespace duckdb

extern "C" {

void httpfs_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

const char *httpfs_version() {
	return duckdb::DuckDB::LibraryVersion();
}

}
