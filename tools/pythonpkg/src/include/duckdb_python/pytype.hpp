#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class DuckDBPyType : public std::enable_shared_from_this<DuckDBPyType> {
public:
	explicit DuckDBPyType(LogicalType type);

public:
	static void Initialize(py::handle &m);

public:
	string ToString() const;
	const LogicalType &Type() const;

private:
private:
	LogicalType type;
};

} // namespace duckdb
