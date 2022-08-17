#include "duckdb_python/protocol.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

bool Protocol::ListLike(py::handle &element) {
	vector<string_t> required_attributes = {"__getitem__", "__len__"};
	bool success = true;
	for (auto &attribute : required_attributes) {
		success = success && py::hasattr(element, attribute);
	}

	return success;
}

} // namespace duckdb
