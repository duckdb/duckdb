#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

unique_ptr<FileSystem> FileSystem::CreateLocal() {
	return make_unique<LocalFileSystem>();
}

} // namespace duckdb
