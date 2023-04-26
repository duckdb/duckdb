#include "duckdb/parser/transformer.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/statement/list.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/parser_options.hpp"

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

Transformer::Transformer(ParserOptions &options)
    : parent(nullptr), options(options), stack_depth(DConstants::INVALID_INDEX) {
}

Transformer::Transformer(Transformer &parent)
    : parent(&parent), options(parent.options), stack_depth(DConstants::INVALID_INDEX) {
}

Transformer::~Transformer() {
}

void Transformer::Clear() {
	SetParamCount(0);
	pivot_entries.clear();
}

bool Transformer::TransformParseTree(duckdb_libpgquery::PGList *tree, vector<unique_ptr<SQLStatement>> &statements) {
	InitializeStackCheck();
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		Clear();
		auto n = (duckdb_libpgquery::PGNode *)entry->data.ptr_value;
		auto stmt = TransformStatement(n);
		D_ASSERT(stmt);
		if (HasPivotEntries()) {
			stmt = CreatePivotStatement(std::move(stmt));
		}
		stmt->n_param = ParamCount();
		statements.push_back(std::move(stmt));
	}
	return true;
}

void Transformer::InitializeStackCheck() {
	stack_depth = 0;
}

StackChecker Transformer::StackCheck(idx_t extra_stack) {
	auto &root = RootTransformer();
	D_ASSERT(root.stack_depth != DConstants::INVALID_INDEX);
	if (root.stack_depth + extra_stack >= options.max_expression_depth) {
		throw ParserException("Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
		                      "increase the maximum expression depth.",
		                      options.max_expression_depth);
	}
	return StackChecker(root, extra_stack);
}

unique_ptr<SQLStatement> Transformer::TransformStatement(duckdb_libpgquery::PGNode *stmt) {
	auto result = TransformStatementInternal(stmt);
	result->n_param = ParamCount();
	if (!named_param_map.empty()) {
		// Avoid overriding a previous move with nothing
		result->named_param_map = std::move(named_param_map);
	}
	return result;
}

Transformer &Transformer::RootTransformer() {
	reference<Transformer> node = *this;
	while (node.get().parent) {
		node = *node.get().parent;
	}
	return node.get();
}

const Transformer &Transformer::RootTransformer() const {
	reference<const Transformer> node = *this;
	while (node.get().parent) {
		node = *node.get().parent;
	}
	return node.get();
}

idx_t Transformer::ParamCount() const {
	auto &root = RootTransformer();
	return root.prepared_statement_parameter_index;
}

void Transformer::SetParamCount(idx_t new_count) {
	auto &root = RootTransformer();
	root.prepared_statement_parameter_index = new_count;
}

static void ParamTypeCheck(PreparedParamType last_type, PreparedParamType new_type) {
	// Mixing positional/auto-increment and named parameters is not supported
	if (last_type == PreparedParamType::INVALID) {
		return;
	}
	if (last_type == PreparedParamType::NAMED) {
		if (new_type != PreparedParamType::NAMED) {
			throw NotImplementedException("Mixing named and positional parameters is not supported yet");
		}
	}
	if (last_type != PreparedParamType::NAMED) {
		if (new_type == PreparedParamType::NAMED) {
			throw NotImplementedException("Mixing named and positional parameters is not supported yet");
		}
	}
}

void Transformer::SetParam(const string &identifier, idx_t index, PreparedParamType type) {
	auto &root = RootTransformer();
	ParamTypeCheck(root.last_param_type, type);
	root.last_param_type = type;
	D_ASSERT(!root.named_param_map.count(identifier));
	root.named_param_map[identifier] = index;
}

bool Transformer::GetParam(const string &identifier, idx_t &index, PreparedParamType type) {
	auto &root = RootTransformer();
	ParamTypeCheck(root.last_param_type, type);
	auto entry = root.named_param_map.find(identifier);
	if (entry == root.named_param_map.end()) {
		return false;
	}
	index = entry->second;
	return true;
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
	case duckdb_libpgquery::T_PGCreateTypeStmt:
		return TransformCreateType(stmt);
	case duckdb_libpgquery::T_PGAlterSeqStmt:
		return TransformAlterSequence(stmt);
	case duckdb_libpgquery::T_PGAttachStmt:
		return TransformAttach(stmt);
	case duckdb_libpgquery::T_PGDetachStmt:
		return TransformDetach(stmt);
	case duckdb_libpgquery::T_PGUseStmt:
		return TransformUse(stmt);
	case duckdb_libpgquery::T_PGCreateDatabaseStmt:
		return TransformCreateDatabase(stmt);
	default:
		throw NotImplementedException(NodetypeToString(stmt->type));
	}
}

} // namespace duckdb
