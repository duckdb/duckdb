#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/transformer.hpp"

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

TransactionIsolationLevel TransformIsolationLevel(const char *str) {
	if (strcmp(str, "read uncommitted") == 0) {
		return TransactionIsolationLevel::READ_UNCOMMITTED;
	} else if (strcmp(str, "read committed") == 0) {
		return TransactionIsolationLevel::READ_COMMITTED;
	} else if (strcmp(str, "repeatable read") == 0) {
		return TransactionIsolationLevel::REPEATABLE_READ;
	} else if (strcmp(str, "serializable") == 0) {
		return TransactionIsolationLevel::SERIALIZABLE;
	}
	throw NotImplementedException("Isolation level \"%s\" is not supported", str);
}

unique_ptr<TransactionStatement> Transformer::TransformTransaction(duckdb_libpgquery::PGTransactionStmt &stmt) {
	auto type = TransformTransactionType(stmt.kind);
	auto info = make_uniq<TransactionInfo>(type);
	info->modifier = TransformTransactionModifier(stmt.transaction_type);

	if (stmt.options) {
		for (auto cell = stmt.options->head; cell; cell = cell->next) {
			auto def = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
			string opt_name(def->defname);
			if (opt_name == "transaction_isolation") {
				auto val = PGPointerCast<duckdb_libpgquery::PGAConst>(def->arg);
				info->isolation_level = TransformIsolationLevel(val->val.val.str);
			} else if (opt_name == "transaction_read_only") {
				auto val = PGPointerCast<duckdb_libpgquery::PGAConst>(def->arg);
				info->modifier = TransformTransactionModifier(
				    val->val.val.ival ? duckdb_libpgquery::PG_TRANS_TYPE_READ_ONLY
				                     : duckdb_libpgquery::PG_TRANS_TYPE_READ_WRITE);
			} else {
				D_ASSERT(opt_name == "transaction_deferrable");
				throw NotImplementedException("DEFERRABLE transactions are not supported");
			}
		}
	}

	return make_uniq<TransactionStatement>(std::move(info));
}

} // namespace duckdb
