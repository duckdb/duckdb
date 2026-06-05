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

enum class TransactionModifierType : uint8_t {
	TRANSACTION_DEFAULT_MODIFIER,
	TRANSACTION_READ_ONLY,
	TRANSACTION_READ_WRITE
};

enum class TransactionInvalidationPolicy : uint8_t { STANDARD_POLICY, ALL_ERRORS_INVALIDATE_TRANSACTION };

struct TransactionInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::TRANSACTION_INFO;

public:
	explicit TransactionInfo(
	    TransactionType type,
	    TransactionInvalidationPolicy invalidation_policy = TransactionInvalidationPolicy::STANDARD_POLICY,
	    bool auto_rollback = false);

	//! The type of transaction statement
	TransactionType type;
	//! Whether or not a transaction can make modifications to the database
	TransactionModifierType modifier;
	//! Which types of exceptions invalidate the database
	TransactionInvalidationPolicy invalidation_policy;
	//! If transaction fails, automatically do a ROLLBACK;
	bool auto_rollback;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);

	string ToString() const;
	unique_ptr<TransactionInfo> Copy() const;

private:
	TransactionInfo();
};

} // namespace duckdb
