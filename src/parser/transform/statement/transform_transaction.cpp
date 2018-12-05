#include "parser/statement/transaction_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<TransactionStatement> Transformer::TransformTransaction(Node *node) {
	TransactionStmt *stmt = reinterpret_cast<TransactionStmt *>(node);
	assert(stmt);
	switch (stmt->kind) {
	case TRANS_STMT_BEGIN:
	case TRANS_STMT_START:
		return make_unique<TransactionStatement>(TransactionType::BEGIN_TRANSACTION);
	case TRANS_STMT_COMMIT:
		return make_unique<TransactionStatement>(TransactionType::COMMIT);
	case TRANS_STMT_ROLLBACK:
		return make_unique<TransactionStatement>(TransactionType::ROLLBACK);
	default:
		throw NotImplementedException("Transaction type %d not implemented yet", stmt->kind);
	}
}
