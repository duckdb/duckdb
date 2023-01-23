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
public:
	vector<string> aliases;
	unique_ptr<SelectStatement> query;

public:
	bool Equals(const CommonTableExpressionInfo &other) const {
		if (aliases != other.aliases) {
			return false;
		}
		if (!query && !other.query) {
			return true;
		}
		if (!query || !other.query) {
			return false;
		}
		return query->Equals(other.query);
	}
};

} // namespace duckdb
