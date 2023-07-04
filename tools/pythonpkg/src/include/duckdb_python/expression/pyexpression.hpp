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

namespace duckdb {

struct DuckDBPyExpression {
public:
	explicit DuckDBPyExpression(unique_ptr<ParsedExpression> expr) : expression(std::move(expr)) {
	}

public:
	static void Initialize(py::handle &m);

	string ToSQL();

	py::str Type();

	string ToString();
	void Print();

private:
private:
	unique_ptr<ParsedExpression> expression;
};

struct PyConstantExpression : public DuckDBPyExpression {
public:
	explicit PyConstantExpression(Value value) : DuckDBPyExpression(make_uniq<ConstantExpression>(std::move(value))) {
	}
};

} // namespace duckdb
