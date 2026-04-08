//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/extension_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class ExtensionStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::EXTENSION_STATEMENT;

public:
	ExtensionStatement(ParserExtension extension, unique_ptr<ParserExtensionParseData> parse_data);

	//! The ParserExtension this statement was generated from
	ParserExtension extension;
	//! The parse data for this specific statement
	unique_ptr<ParserExtensionParseData> parse_data;

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
