//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/create_sequence_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class CreateSequenceStatement : public SQLStatement {
public:
	CreateSequenceStatement() : SQLStatement(StatementType::CREATE_SEQUENCE), info(make_unique<CreateSequenceInfo>()){};

	unique_ptr<CreateSequenceInfo> info;
};

} // namespace duckdb
