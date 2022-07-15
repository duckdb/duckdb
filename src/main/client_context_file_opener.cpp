#include "duckdb/main/client_context_file_opener.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

bool ClientContextFileOpener::TryGetCurrentSetting(const string &key, Value &result) {
	return context.TryGetCurrentSetting(key, result);
}

} // namespace duckdb
