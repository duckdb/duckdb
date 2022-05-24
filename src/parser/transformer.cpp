#include "duckdb/parser/transformer.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/statement/list.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"

namespace duckdb {

StackChecker::StackChecker(Transformer &transformer_p, idx_t stack_usage_p)
    : transformer(transformer_p), stack_usage(stack_usage_p) {
	transformer.stack_depth += stack_usage;
}

StackChecker::~StackChecker() {
	transformer.stack_depth -= stack_usage;
}

StackChecker::StackChecker(StackChecker &&other) noexcept
    : transformer(other.transformer), stack_usage(other.stack_usage) {
	other.stack_usage = 0;
}

Transformer::Transformer(idx_t max_expression_depth_p)
    : parent(nullptr), max_expression_depth(max_expression_depth_p), stack_depth(DConstants::INVALID_INDEX) {
}

Transformer::Transformer(Transformer *parent)
    : parent(parent), max_expression_depth(parent->max_expression_depth), stack_depth(DConstants::INVALID_INDEX) {
}

bool Transformer::TransformParseTree(duckdb_libpgquery::PGList *tree, vector<unique_ptr<SQLStatement>> &statements) {
	InitializeStackCheck();
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		SetParamCount(0);
		auto stmt = TransformStatement((duckdb_libpgquery::PGNode *)entry->data.ptr_value);
		D_ASSERT(stmt);
		stmt->n_param = ParamCount();
		statements.push_back(move(stmt));
	}
	return true;
}

void Transformer::InitializeStackCheck() {
	stack_depth = 0;
}

StackChecker Transformer::StackCheck(idx_t extra_stack) {
	auto node = this;
	while (node->parent) {
		node = node->parent;
	}
	D_ASSERT(node->stack_depth != DConstants::INVALID_INDEX);
	if (node->stack_depth + extra_stack >= max_expression_depth) {
		throw ParserException("Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
		                      "increase the maximum expression depth.",
		                      max_expression_depth);
	}
	return StackChecker(*node, extra_stack);
}

unique_ptr<SQLStatement> Transformer::TransformStatement(duckdb_libpgquery::PGNode *stmt) {
	auto result = TransformStatementInternal(stmt);
	result->n_param = ParamCount();
	return result;
}

unique_ptr<SQLStatement> Transformer::TransformStatementInternal(duckdb_libpgquery::PGNode *stmt) {
	switch (stmt->type) {
	case duckdb_libpgquery::T_PGRawStmt: {
		auto raw_stmt = (duckdb_libpgquery::PGRawStmt *)stmt;
		auto result = TransformStatement(raw_stmt->stmt);
		if (result) {
			result->stmt_location = raw_stmt->stmt_location;
			result->stmt_length = raw_stmt->stmt_len;
		}
		return result;
	}
	case duckdb_libpgquery::T_PGSelectStmt:
		return TransformSelect(stmt);
	case duckdb_libpgquery::T_PGCreateStmt:
		return TransformCreateTable(stmt);
	case duckdb_libpgquery::T_PGCreateSchemaStmt:
		return TransformCreateSchema(stmt);
	case duckdb_libpgquery::T_PGViewStmt:
		return TransformCreateView(stmt);
	case duckdb_libpgquery::T_PGCreateSeqStmt:
		return TransformCreateSequence(stmt);
	case duckdb_libpgquery::T_PGCreateFunctionStmt:
		return TransformCreateFunction(stmt);
	case duckdb_libpgquery::T_PGCreateMatViewStmt:
		return TransformCreateMatView(stmt);
	case duckdb_libpgquery::T_PGDropStmt:
		return TransformDrop(stmt);
	case duckdb_libpgquery::T_PGInsertStmt:
		return TransformInsert(stmt);
	case duckdb_libpgquery::T_PGCopyStmt:
		return TransformCopy(stmt);
	case duckdb_libpgquery::T_PGTransactionStmt:
		return TransformTransaction(stmt);
	case duckdb_libpgquery::T_PGDeleteStmt:
		return TransformDelete(stmt);
	case duckdb_libpgquery::T_PGUpdateStmt:
		return TransformUpdate(stmt);
	case duckdb_libpgquery::T_PGIndexStmt:
		return TransformCreateIndex(stmt);
	case duckdb_libpgquery::T_PGAlterTableStmt:
		return TransformAlter(stmt);
	case duckdb_libpgquery::T_PGRenameStmt:
		return TransformRename(stmt);
	case duckdb_libpgquery::T_PGPrepareStmt:
		return TransformPrepare(stmt);
	case duckdb_libpgquery::T_PGExecuteStmt:
		return TransformExecute(stmt);
	case duckdb_libpgquery::T_PGDeallocateStmt:
		return TransformDeallocate(stmt);
	case duckdb_libpgquery::T_PGCreateTableAsStmt:
		return TransformCreateTableAs(stmt);
	case duckdb_libpgquery::T_PGPragmaStmt:
		return TransformPragma(stmt);
	case duckdb_libpgquery::T_PGExportStmt:
		return TransformExport(stmt);
	case duckdb_libpgquery::T_PGImportStmt:
		return TransformImport(stmt);
	case duckdb_libpgquery::T_PGExplainStmt:
		return TransformExplain(stmt);
	case duckdb_libpgquery::T_PGVacuumStmt:
		return TransformVacuum(stmt);
	case duckdb_libpgquery::T_PGVariableShowStmt:
		return TransformShow(stmt);
	case duckdb_libpgquery::T_PGVariableShowSelectStmt:
		return TransformShowSelect(stmt);
	case duckdb_libpgquery::T_PGCallStmt:
		return TransformCall(stmt);
	case duckdb_libpgquery::T_PGVariableSetStmt:
		return TransformSet(stmt);
	case duckdb_libpgquery::T_PGCheckPointStmt:
		return TransformCheckpoint(stmt);
	case duckdb_libpgquery::T_PGLoadStmt:
		return TransformLoad(stmt);
	case duckdb_libpgquery::T_PGCreateEnumStmt:
		return TransformCreateEnum(stmt);
	case duckdb_libpgquery::T_PGAlterSeqStmt:
		return TransformAlterSequence(stmt);
	default:
		throw NotImplementedException(NodetypeToString(stmt->type));
	}
	return nullptr;
}

} // namespace duckdb
