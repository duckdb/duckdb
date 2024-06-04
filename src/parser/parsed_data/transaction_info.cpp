#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

TransactionInfo::TransactionInfo() : ParseInfo(TYPE) {
}

TransactionInfo::TransactionInfo(TransactionType type) : ParseInfo(TYPE), type(type) {
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
	result += ";";
	return result;
}

} // namespace duckdb
