#include "duckdb/parser/transformer.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/statement/list.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"

using namespace duckdb;
using namespace std;

bool Transformer::TransformParseTree(PGList *tree, vector<unique_ptr<SQLStatement>> &statements) {
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		auto stmt = TransformStatement((PGNode *)entry->data.ptr_value);
		if (!stmt) {
			statements.clear();
			return false;
		}
		statements.push_back(move(stmt));
	}
	return true;
}

unique_ptr<SQLStatement> Transformer::TransformStatement(PGNode *stmt) {
	switch (stmt->type) {
	case T_PGRawStmt: {
		auto raw_stmt = (PGRawStmt *)stmt;
		auto result = TransformStatement(raw_stmt->stmt);
		if (result) {
			result->stmt_location = raw_stmt->stmt_location;
			result->stmt_length = raw_stmt->stmt_len;
		}
		return result;
	}
	case T_PGSelectStmt:
		return TransformSelect(stmt);
	case T_PGCreateStmt:
		return TransformCreateTable(stmt);
	case T_PGCreateSchemaStmt:
		return TransformCreateSchema(stmt);
	case T_PGViewStmt:
		return TransformCreateView(stmt);
	case T_PGCreateSeqStmt:
		return TransformCreateSequence(stmt);
	case T_PGDropStmt:
		return TransformDrop(stmt);
	case T_PGInsertStmt:
		return TransformInsert(stmt);
	case T_PGCopyStmt:
		return TransformCopy(stmt);
	case T_PGTransactionStmt:
		return TransformTransaction(stmt);
	case T_PGDeleteStmt:
		return TransformDelete(stmt);
	case T_PGUpdateStmt:
		return TransformUpdate(stmt);
	case T_PGIndexStmt:
		return TransformCreateIndex(stmt);
	case T_PGAlterTableStmt:
		return TransformAlter(stmt);
	case T_PGRenameStmt:
		return TransformRename(stmt);
	case T_PGPrepareStmt:
		return TransformPrepare(stmt);
	case T_PGExecuteStmt:
		return TransformExecute(stmt);
	case T_PGDeallocateStmt:
		return TransformDeallocate(stmt);
	case T_PGCreateTableAsStmt:
		return TransformCreateTableAs(stmt);
	case T_PGPragmaStmt:
		return TransformPragma(stmt);
	case T_PGExplainStmt:
		return TransformExplain(stmt);
	case T_PGVacuumStmt:
		return TransformVacuum(stmt);
	case T_PGVariableShowStmt:
		return TransformShow(stmt);
	default:
		throw NotImplementedException(NodetypeToString(stmt->type));
	}
	return nullptr;
}
