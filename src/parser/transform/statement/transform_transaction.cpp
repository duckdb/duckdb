#include <memory>
#include <utility>

#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

TransactionType TransformTransactionType(duckdb_libpgquery::PGTransactionStmtKind kind) {
	switch (kind) {
	case duckdb_libpgquery::PG_TRANS_STMT_BEGIN:
	case duckdb_libpgquery::PG_TRANS_STMT_START:
		return TransactionType::BEGIN_TRANSACTION;
	case duckdb_libpgquery::PG_TRANS_STMT_COMMIT:
		return TransactionType::COMMIT;
	case duckdb_libpgquery::PG_TRANS_STMT_ROLLBACK:
		return TransactionType::ROLLBACK;
	default:
		throw NotImplementedException("Transaction type %d not implemented yet", kind);
	}
}

TransactionModifierType TransformTransactionModifier(duckdb_libpgquery::PGTransactionStmtType type) {
	switch (type) {
	case duckdb_libpgquery::PG_TRANS_TYPE_DEFAULT:
		return TransactionModifierType::TRANSACTION_DEFAULT_MODIFIER;
	case duckdb_libpgquery::PG_TRANS_TYPE_READ_ONLY:
		return TransactionModifierType::TRANSACTION_READ_ONLY;
	case duckdb_libpgquery::PG_TRANS_TYPE_READ_WRITE:
		return TransactionModifierType::TRANSACTION_READ_WRITE;
	default:
		throw NotImplementedException("Transaction modifier %d not implemented yet", type);
	}
}

unique_ptr<TransactionStatement> Transformer::TransformTransaction(duckdb_libpgquery::PGTransactionStmt &stmt) {
	//	stmt.transaction_type
	auto type = TransformTransactionType(stmt.kind);
	auto info = make_uniq<TransactionInfo>(type);
	info->modifier = TransformTransactionModifier(stmt.transaction_type);
	return make_uniq<TransactionStatement>(std::move(info));
}

} // namespace duckdb
