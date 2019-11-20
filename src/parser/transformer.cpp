#include "duckdb/parser/transformer.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/statement/list.hpp"

using namespace duckdb;
using namespace std;

bool Transformer::TransformParseTree(postgres::PGList *tree, vector<unique_ptr<SQLStatement>> &statements) {
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		auto stmt = TransformStatement((postgres::PGNode *)entry->data.ptr_value);
		if (!stmt) {
			statements.clear();
			return false;
		}
		statements.push_back(move(stmt));
	}
	return true;
}

unique_ptr<SQLStatement> Transformer::TransformStatement(postgres::PGNode *stmt) {
	switch (stmt->type) {
	case postgres::T_PGRawStmt:
		return TransformStatement(((postgres::PGRawStmt *)stmt)->stmt);
	case postgres::T_PGSelectStmt:
		return TransformSelect(stmt);
	case postgres::T_PGCreateStmt:
		return TransformCreateTable(stmt);
	case postgres::T_PGCreateSchemaStmt:
		return TransformCreateSchema(stmt);
	case postgres::T_PGViewStmt:
		return TransformCreateView(stmt);
	case postgres::T_PGCreateSeqStmt:
		return TransformCreateSequence(stmt);
	case postgres::T_PGDropStmt:
		return TransformDrop(stmt);
	case postgres::T_PGInsertStmt:
		return TransformInsert(stmt);
	case postgres::T_PGCopyStmt:
		return TransformCopy(stmt);
	case postgres::T_PGTransactionStmt:
		return TransformTransaction(stmt);
	case postgres::T_PGDeleteStmt:
		return TransformDelete(stmt);
	case postgres::T_PGUpdateStmt:
		return TransformUpdate(stmt);
	case postgres::T_PGIndexStmt:
		return TransformCreateIndex(stmt);
	case postgres::T_PGAlterTableStmt:
		return TransformAlter(stmt);
	case postgres::T_PGRenameStmt:
		return TransformRename(stmt);
	case postgres::T_PGPrepareStmt:
		return TransformPrepare(stmt);
	case postgres::T_PGExecuteStmt:
		return TransformExecute(stmt);
	case postgres::T_PGDeallocateStmt:
		return TransformDeallocate(stmt);
	case postgres::T_PGCreateTableAsStmt:
		return TransformCreateTableAs(stmt);
	case postgres::T_PGExplainStmt: {
		postgres::PGExplainStmt *explain_stmt = reinterpret_cast<postgres::PGExplainStmt *>(stmt);
		return make_unique<ExplainStatement>(TransformStatement(explain_stmt->query));
	}
	case postgres::T_PGVacuumStmt: { // Ignore VACUUM/ANALYZE for now
		return nullptr;
	}
	default:
		throw NotImplementedException(NodetypeToString(stmt->type));
	}
	return nullptr;
}
