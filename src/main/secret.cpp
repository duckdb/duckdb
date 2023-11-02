#include "duckdb/main/secret.hpp"

namespace duckdb {

int RegisteredSecret::LongestMatch(const string &path) {
	int longest_match = -1;
	for (const auto &prefix : prefix_paths) {
		if (prefix == "*") {
			longest_match = 0;
			continue;
		}
		if (StringUtil::StartsWith(path, prefix)) {
			longest_match = MaxValue<int>(prefix.length(), longest_match);
		};
	}
	return longest_match;
}

} // namespace duckdb
