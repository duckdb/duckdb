//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/transaction_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/sql_statement.hpp"

#include <vector>

namespace duckdb {

enum class TransactionType : uint8_t { INVALID, BEGIN_TRANSACTION, COMMIT, ROLLBACK };

class TransactionStatement : public SQLStatement {
public:
	TransactionStatement(TransactionType type) : SQLStatement(StatementType::TRANSACTION), type(type){};

	TransactionType type;
};
} // namespace duckdb
