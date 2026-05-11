//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/column_qualifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"

namespace duckdb {

class ColumnQualifier {
public:
	explicit ColumnQualifier(Binder &binder);

	//! Returns the STRUCT_EXTRACT operator expression
	unique_ptr<ParsedExpression> CreateStructExtract(unique_ptr<ParsedExpression> base, const string &field_name);
	//! Returns a STRUCT_PACK function expression
	unique_ptr<ParsedExpression> CreateStructPack(ColumnRefExpression &col_ref);

private:
	Binder &binder;
};

} // namespace duckdb
