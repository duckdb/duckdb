#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include <string>
#include <unordered_set>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
	std::string input(reinterpret_cast<const char *>(data), size);
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	std::unordered_set<std::string> internal_error_messages = {"Unoptimized Result differs from original result!",
	                                                           "INTERNAL"};
	con.Query("PRAGMA enable_verification");
	try {
		auto result = con.Query(input);
		if (!result->success) {
			for (auto &internal_error : internal_error_messages) {
				if (duckdb::StringUtil::Contains(result->error, internal_error)) {
					return 1;
				}
			}
		}
	} catch (std::exception &e) {
	}
	return 0;
}
