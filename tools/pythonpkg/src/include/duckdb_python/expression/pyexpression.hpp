//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyrelation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb_python/python_conversion.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {

struct DuckDBPyExpression {
public:
	explicit DuckDBPyExpression(unique_ptr<ParsedExpression> expr);

public:
	static void Initialize(py::handle &m);

	string Type() const;

	string ToString() const;
	void Print() const;

public:
	const ParsedExpression &GetExpression() const;

private:
private:
	unique_ptr<ParsedExpression> expression;
};

struct PythonValue : py::object {
public:
	PythonValue(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object) { // NOLINT
		auto &import_cache = *DuckDBPyConnection::ImportCache();
		return py::isinstance(object, import_cache.pyduckdb().value());
	}
};

struct PyConstantExpression : public DuckDBPyExpression {
public:
	explicit PyConstantExpression(const PythonValue &value)
	    : DuckDBPyExpression(make_uniq<ConstantExpression>(TransformPythonValue(value))) {
	}
};

} // namespace duckdb
