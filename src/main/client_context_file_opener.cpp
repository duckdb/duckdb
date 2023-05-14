#include "duckdb/main/client_context_file_opener.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

bool ClientContextFileOpener::TryGetCurrentSetting(const string &key, Value &result) {
	return context.TryGetCurrentSetting(key, result);
}

ClientContext *FileOpener::TryGetClientContext(FileOpener *opener) {
	if (!opener) {
		return nullptr;
	}
	return opener->TryGetClientContext();
}

bool FileOpener::TryGetCurrentSetting(FileOpener *opener, const string &key, Value &result) {
	if (!opener) {
		return false;
	}
	return opener->TryGetCurrentSetting(key, result);
}

} // namespace duckdb
