#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

TransactionInfo::TransactionInfo() : ParseInfo(TYPE) {
}

TransactionInfo::TransactionInfo(TransactionType type)
    : ParseInfo(TYPE), type(type), modifier(TransactionModifierType::TRANSACTION_DEFAULT_MODIFIER) {
}

string TransactionInfo::ToString() const {
	string result = "";
	switch (type) {
	case TransactionType::BEGIN_TRANSACTION:
		result += "BEGIN";
		break;
	case TransactionType::COMMIT:
		result += "COMMIT";
		break;
	case TransactionType::ROLLBACK:
		result += "ROLLBACK";
		break;
	default: {
		throw InternalException("ToString for TransactionStatement with type: %s not implemented",
		                        EnumUtil::ToString(type));
	}
	}
	switch (modifier) {
	case TransactionModifierType::TRANSACTION_DEFAULT_MODIFIER:
		break;
	case TransactionModifierType::TRANSACTION_READ_ONLY:
		result += " READ ONLY";
		break;
	case TransactionModifierType::TRANSACTION_READ_WRITE:
		result += " READ WRITE";
		break;
	default:
		throw InternalException("ToString for TransactionStatement with modifier type: %s not implemented",
		                        EnumUtil::ToString(modifier));
	}
	result += ";";
	return result;
}

unique_ptr<TransactionInfo> TransactionInfo::Copy() const {
	auto result = make_uniq<TransactionInfo>(type);
	result->modifier = modifier;
	return result;
}

} // namespace duckdb
