#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<TransactionStatement> Transformer::TransformTransaction(duckdb_libpgquery::PGTransactionStmt &stmt) {
	switch (stmt.kind) {
	case duckdb_libpgquery::PG_TRANS_STMT_BEGIN:
	case duckdb_libpgquery::PG_TRANS_STMT_START:
		return make_uniq<TransactionStatement>(TransactionType::BEGIN_TRANSACTION);
	case duckdb_libpgquery::PG_TRANS_STMT_COMMIT:
		return make_uniq<TransactionStatement>(TransactionType::COMMIT);
	case duckdb_libpgquery::PG_TRANS_STMT_ROLLBACK:
		return make_uniq<TransactionStatement>(TransactionType::ROLLBACK);
	default:
		throw NotImplementedException("Transaction type %d not implemented yet", stmt.kind);
	}
}

} // namespace duckdb
