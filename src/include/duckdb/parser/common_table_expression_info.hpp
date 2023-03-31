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

	void FormatSerialize(FormatSerializer &serializer) const;
	static unique_ptr<CommonTableExpressionInfo> FormatDeserialize(FormatDeserializer &deserializer);
	unique_ptr<CommonTableExpressionInfo> Copy();
};

} // namespace duckdb
