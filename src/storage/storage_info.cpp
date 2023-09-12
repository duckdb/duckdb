#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

const uint64_t VERSION_NUMBER = 60;

struct StorageVersionInfo {
	const char *version_name;
	idx_t storage_version;
};

static StorageVersionInfo storage_version_info[] = {{"v0.8.0 or v0.8.1", 51},
                                                    {"v0.7.0 or v0.7.1", 43},
                                                    {"v0.6.0 or v0.6.1", 39},
                                                    {"v0.5.0 or v0.5.1", 38},
                                                    {"v0.3.3, v0.3.4 or v0.4.0", 33},
                                                    {"v0.3.2", 31},
                                                    {"v0.3.1", 27},
                                                    {"v0.3.0", 25},
                                                    {"v0.2.9", 21},
                                                    {"v0.2.8", 18},
                                                    {"v0.2.7", 17},
                                                    {"v0.2.6", 15},
                                                    {"v0.2.5", 13},
                                                    {"v0.2.4", 11},
                                                    {"v0.2.3", 6},
                                                    {"v0.2.2", 4},
                                                    {"v0.2.1 and prior", 1},
                                                    {nullptr, 0}};

const char *GetDuckDBVersion(idx_t version_number) {
	for (idx_t i = 0; storage_version_info[i].version_name; i++) {
		if (version_number == storage_version_info[i].storage_version) {
			return storage_version_info[i].version_name;
		}
	}
	return nullptr;
}

} // namespace duckdb
