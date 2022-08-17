//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/protocol.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb_python/pybind_wrapper.hpp"

namespace duckdb {

class Protocol {
	Protocol() = delete;

public:
	static bool ListLike(py::handle &element);
};

} // namespace duckdb
