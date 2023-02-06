#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

TransactionManager::TransactionManager(AttachedDatabase &db) : db(db) {
}

TransactionManager::~TransactionManager() {
}

} // namespace duckdb
