//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/common_table_expression_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class SelectStatement;

struct CommonTableExpressionInfo {
	CommonTableExpressionInfo();
	~CommonTableExpressionInfo();

public:
	vector<string> aliases;
	unique_ptr<SelectStatement> query;

public:
	bool Equals(const CommonTableExpressionInfo &other) const;
};

} // namespace duckdb
