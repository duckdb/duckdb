#include "parser/transformer.hpp"

#include "parser/expression/list.hpp"
#include "parser/statement/list.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

bool Transformer::TransformParseTree(List *tree, vector<unique_ptr<SQLStatement>> &statements) {
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		auto stmt = TransformStatement((Node *)entry->data.ptr_value);
		if (!stmt) {
			statements.clear();
			return false;
		}
		statements.push_back(move(stmt));
	}
	return true;
}

unique_ptr<SQLStatement> Transformer::TransformStatement(Node *stmt) {
	switch (stmt->type) {
	case T_RawStmt:
		return TransformStatement(((RawStmt *)stmt)->stmt);
	case T_SelectStmt:
		return TransformSelect(stmt);
	case T_CreateStmt:
		return TransformCreateTable(stmt);
	case T_CreateSchemaStmt:
		return TransformCreateSchema(stmt);
	case T_ViewStmt:
		return TransformCreateView(stmt);
	case T_CreateSeqStmt:
		return TransformCreateSequence(stmt);
	case T_DropStmt:
		return TransformDrop(stmt);
	case T_InsertStmt:
		return TransformInsert(stmt);
	case T_CopyStmt:
		return TransformCopy(stmt);
	case T_TransactionStmt:
		return TransformTransaction(stmt);
	case T_DeleteStmt:
		return TransformDelete(stmt);
	case T_UpdateStmt:
		return TransformUpdate(stmt);
	case T_IndexStmt:
		return TransformCreateIndex(stmt);
	case T_AlterTableStmt:
		return TransformAlter(stmt);
	case T_RenameStmt:
		return TransformRename(stmt);
	case T_PrepareStmt:
		return TransformPrepare(stmt);
	case T_ExecuteStmt:
		return TransformExecute(stmt);
	case T_DeallocateStmt:
		return TransformDeallocate(stmt);
	case T_CreateTableAsStmt:
		return TransformCreateTableAs(stmt);
	case T_ExplainStmt: {
		ExplainStmt *explain_stmt = reinterpret_cast<ExplainStmt *>(stmt);
		return make_unique<ExplainStatement>(TransformStatement(explain_stmt->query));
	}
	default:
		throw NotImplementedException(NodetypeToString(stmt->type));
	}
	return nullptr;
}
