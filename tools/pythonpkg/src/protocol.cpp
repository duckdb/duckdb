#include "duckdb_python/protocol.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

bool Protocol::ListLike(py::handle &element) {
	static const char *required_attributes[] = {"__getitem__", "__len__"};
	const idx_t len = sizeof(required_attributes) / sizeof(*required_attributes);
	bool success = true;
	for (idx_t i = 0; i < len; i++) {
		auto attribute = required_attributes[i];
		success = success && py::hasattr(element, attribute);
	}

	return success;
}

} // namespace duckdb
