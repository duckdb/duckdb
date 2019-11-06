#include "duckdb/parser/transformer.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/statement/list.hpp"

using namespace duckdb;
using namespace std;

bool Transformer::TransformParseTree(postgres::List *tree, vector<unique_ptr<SQLStatement>> &statements) {
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		auto stmt = TransformStatement((postgres::Node *)entry->data.ptr_value);
		if (!stmt) {
			statements.clear();
			return false;
		}
		statements.push_back(move(stmt));
	}
	return true;
}

unique_ptr<SQLStatement> Transformer::TransformStatement(postgres::Node *stmt) {
	switch (stmt->type) {
	case postgres::T_RawStmt:
		return TransformStatement(((postgres::RawStmt *)stmt)->stmt);
	case postgres::T_SelectStmt:
		return TransformSelect(stmt);
	case postgres::T_CreateStmt:
		return TransformCreateTable(stmt);
	case postgres::T_CreateSchemaStmt:
		return TransformCreateSchema(stmt);
	case postgres::T_ViewStmt:
		return TransformCreateView(stmt);
	case postgres::T_CreateSeqStmt:
		return TransformCreateSequence(stmt);
	case postgres::T_DropStmt:
		return TransformDrop(stmt);
	case postgres::T_InsertStmt:
		return TransformInsert(stmt);
	case postgres::T_CopyStmt:
		return TransformCopy(stmt);
	case postgres::T_TransactionStmt:
		return TransformTransaction(stmt);
	case postgres::T_DeleteStmt:
		return TransformDelete(stmt);
	case postgres::T_UpdateStmt:
		return TransformUpdate(stmt);
	case postgres::T_IndexStmt:
		return TransformCreateIndex(stmt);
	case postgres::T_AlterTableStmt:
		return TransformAlter(stmt);
	case postgres::T_RenameStmt:
		return TransformRename(stmt);
	case postgres::T_PrepareStmt:
		return TransformPrepare(stmt);
	case postgres::T_ExecuteStmt:
		return TransformExecute(stmt);
	case postgres::T_DeallocateStmt:
		return TransformDeallocate(stmt);
	case postgres::T_CreateTableAsStmt:
		return TransformCreateTableAs(stmt);
	case postgres::T_ExplainStmt: {
		postgres::ExplainStmt *explain_stmt = reinterpret_cast<postgres::ExplainStmt *>(stmt);
		return make_unique<ExplainStatement>(TransformStatement(explain_stmt->query));
	}
	case postgres::T_VacuumStmt: { // Ignore VACUUM/ANALYZE for now
		return nullptr;
	}
	default:
		throw NotImplementedException(NodetypeToString(stmt->type));
	}
	return nullptr;
}
