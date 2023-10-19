#include "duckdb/parser/parsed_data/transaction_info.hpp"

namespace duckdb {

TransactionInfo::TransactionInfo() : ParseInfo(TYPE) {
}

TransactionInfo::TransactionInfo(TransactionType type) : ParseInfo(TYPE), type(type) {
}

} // namespace duckdb
