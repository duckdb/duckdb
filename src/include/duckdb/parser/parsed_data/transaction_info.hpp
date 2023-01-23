//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/transaction_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

enum class TransactionType : uint8_t { INVALID, BEGIN_TRANSACTION, COMMIT, ROLLBACK };

struct TransactionInfo : public ParseInfo {
public:
	explicit TransactionInfo(TransactionType type) : type(type) {
	}

	//! The type of transaction statement
	TransactionType type;

public:
	bool Equals(const TransactionInfo &other) const {
		return type == other.type;
	}
};

} // namespace duckdb
