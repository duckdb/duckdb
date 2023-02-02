//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/common_table_expression_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

class SelectStatement;

struct CommonTableExpressionInfo {
	vector<string> aliases;
	unique_ptr<SelectStatement> query;

	void FormatSerialize(FormatSerializer &serializer) const {
		serializer.WriteProperty("aliases", aliases);
		serializer.WriteProperty("query", query);
	}
};

} // namespace duckdb
