#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TransactionStatement> Transformer::TransformTransaction(postgres::PGNode *node) {
	auto stmt = reinterpret_cast<postgres::PGTransactionStmt *>(node);
	assert(stmt);
	switch (stmt->kind) {
	case postgres::PG_TRANS_STMT_BEGIN:
	case postgres::PG_TRANS_STMT_START:
		return make_unique<TransactionStatement>(TransactionType::BEGIN_TRANSACTION);
	case postgres::PG_TRANS_STMT_COMMIT:
		return make_unique<TransactionStatement>(TransactionType::COMMIT);
	case postgres::PG_TRANS_STMT_ROLLBACK:
		return make_unique<TransactionStatement>(TransactionType::ROLLBACK);
	default:
		throw NotImplementedException("Transaction type %d not implemented yet", stmt->kind);
	}
}
