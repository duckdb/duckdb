//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pystatement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb.hpp"

namespace duckdb {

struct DuckDBPyStatement {
public:
	explicit DuckDBPyStatement(unique_ptr<SQLStatement> statement);

public:
	//! Create a copy of the wrapped statement
	unique_ptr<SQLStatement> GetStatement();
	string Query() const;
	py::set NamedParameters() const;
	StatementType Type() const;
	py::list ExpectedResultType() const;

public:
	static void Initialize(py::handle &m);

private:
	unique_ptr<SQLStatement> statement;
};

} // namespace duckdb
