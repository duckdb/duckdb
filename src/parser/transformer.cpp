#include "duckdb/parser/transformer.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/statement/list.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/parser_options.hpp"

namespace duckdb {

Transformer::Transformer(ParserOptions &options)
    : parent(nullptr), options(options), stack_depth(DConstants::INVALID_INDEX) {
}

Transformer::Transformer(Transformer &parent)
    : parent(&parent), options(parent.options), stack_depth(DConstants::INVALID_INDEX) {
}

Transformer::~Transformer() {
}

void Transformer::Clear() {
	ClearParameters();
	pivot_entries.clear();
}

bool Transformer::TransformParseTree(duckdb_libpgquery::PGList *tree, vector<unique_ptr<SQLStatement>> &statements) {
	InitializeStackCheck();
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		Clear();
		auto n = PGPointerCast<duckdb_libpgquery::PGNode>(entry->data.ptr_value);
		auto stmt = TransformStatement(*n);
		D_ASSERT(stmt);
		if (HasPivotEntries()) {
			stmt = CreatePivotStatement(std::move(stmt));
		}
		statements.push_back(std::move(stmt));
	}
	return true;
}

void Transformer::InitializeStackCheck() {
	stack_depth = 0;
}

StackChecker<Transformer> Transformer::StackCheck(idx_t extra_stack) {
	auto &root = RootTransformer();
	D_ASSERT(root.stack_depth != DConstants::INVALID_INDEX);
	if (root.stack_depth + extra_stack >= options.max_expression_depth) {
		throw ParserException("Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
		                      "increase the maximum expression depth.",
		                      options.max_expression_depth);
	}
	return StackChecker<Transformer>(root, extra_stack);
}

unique_ptr<SQLStatement> Transformer::TransformStatement(duckdb_libpgquery::PGNode &stmt) {
	auto result = TransformStatementInternal(stmt);
	if (!named_param_map.empty()) {
		// Avoid overriding a previous move with nothing
		result->named_param_map = named_param_map;
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

void Transformer::ClearParameters() {
	auto &root = RootTransformer();
	root.prepared_statement_parameter_index = 0;
	root.named_param_map.clear();
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

unique_ptr<SQLStatement> Transformer::TransformStatementInternal(duckdb_libpgquery::PGNode &stmt) {
	switch (stmt.type) {
	case duckdb_libpgquery::T_PGRawStmt: {
		auto &raw_stmt = PGCast<duckdb_libpgquery::PGRawStmt>(stmt);
		auto result = TransformStatement(*raw_stmt.stmt);
		if (result) {
			result->stmt_location = NumericCast<idx_t>(raw_stmt.stmt_location);
			result->stmt_length = NumericCast<idx_t>(raw_stmt.stmt_len);
		}
		return result;
	}
	case duckdb_libpgquery::T_PGSelectStmt:
		return TransformSelectStmt(PGCast<duckdb_libpgquery::PGSelectStmt>(stmt));
	case duckdb_libpgquery::T_PGCreateStmt:
		return TransformCreateTable(PGCast<duckdb_libpgquery::PGCreateStmt>(stmt));
	case duckdb_libpgquery::T_PGCreateSchemaStmt:
		return TransformCreateSchema(PGCast<duckdb_libpgquery::PGCreateSchemaStmt>(stmt));
	case duckdb_libpgquery::T_PGViewStmt:
		return TransformCreateView(PGCast<duckdb_libpgquery::PGViewStmt>(stmt));
	case duckdb_libpgquery::T_PGCreateSeqStmt:
		return TransformCreateSequence(PGCast<duckdb_libpgquery::PGCreateSeqStmt>(stmt));
	case duckdb_libpgquery::T_PGCreateFunctionStmt:
		return TransformCreateFunction(PGCast<duckdb_libpgquery::PGCreateFunctionStmt>(stmt));
	case duckdb_libpgquery::T_PGDropStmt:
		return TransformDrop(PGCast<duckdb_libpgquery::PGDropStmt>(stmt));
	case duckdb_libpgquery::T_PGInsertStmt:
		return TransformInsert(PGCast<duckdb_libpgquery::PGInsertStmt>(stmt));
	case duckdb_libpgquery::T_PGCopyStmt:
		return TransformCopy(PGCast<duckdb_libpgquery::PGCopyStmt>(stmt));
	case duckdb_libpgquery::T_PGTransactionStmt:
		return TransformTransaction(PGCast<duckdb_libpgquery::PGTransactionStmt>(stmt));
	case duckdb_libpgquery::T_PGDeleteStmt:
		return TransformDelete(PGCast<duckdb_libpgquery::PGDeleteStmt>(stmt));
	case duckdb_libpgquery::T_PGUpdateStmt:
		return TransformUpdate(PGCast<duckdb_libpgquery::PGUpdateStmt>(stmt));
	case duckdb_libpgquery::T_PGUpdateExtensionsStmt:
		return TransformUpdateExtensions(PGCast<duckdb_libpgquery::PGUpdateExtensionsStmt>(stmt));
	case duckdb_libpgquery::T_PGIndexStmt:
		return TransformCreateIndex(PGCast<duckdb_libpgquery::PGIndexStmt>(stmt));
	case duckdb_libpgquery::T_PGAlterTableStmt:
		return TransformAlter(PGCast<duckdb_libpgquery::PGAlterTableStmt>(stmt));
	case duckdb_libpgquery::T_PGRenameStmt:
		return TransformRename(PGCast<duckdb_libpgquery::PGRenameStmt>(stmt));
	case duckdb_libpgquery::T_PGPrepareStmt:
		return TransformPrepare(PGCast<duckdb_libpgquery::PGPrepareStmt>(stmt));
	case duckdb_libpgquery::T_PGExecuteStmt:
		return TransformExecute(PGCast<duckdb_libpgquery::PGExecuteStmt>(stmt));
	case duckdb_libpgquery::T_PGDeallocateStmt:
		return TransformDeallocate(PGCast<duckdb_libpgquery::PGDeallocateStmt>(stmt));
	case duckdb_libpgquery::T_PGCreateTableAsStmt:
		return TransformCreateTableAs(PGCast<duckdb_libpgquery::PGCreateTableAsStmt>(stmt));
	case duckdb_libpgquery::T_PGPragmaStmt:
		return TransformPragma(PGCast<duckdb_libpgquery::PGPragmaStmt>(stmt));
	case duckdb_libpgquery::T_PGExportStmt:
		return TransformExport(PGCast<duckdb_libpgquery::PGExportStmt>(stmt));
	case duckdb_libpgquery::T_PGImportStmt:
		return TransformImport(PGCast<duckdb_libpgquery::PGImportStmt>(stmt));
	case duckdb_libpgquery::T_PGExplainStmt:
		return TransformExplain(PGCast<duckdb_libpgquery::PGExplainStmt>(stmt));
	case duckdb_libpgquery::T_PGVacuumStmt:
		return TransformVacuum(PGCast<duckdb_libpgquery::PGVacuumStmt>(stmt));
	case duckdb_libpgquery::T_PGVariableShowStmt:
		return TransformShowStmt(PGCast<duckdb_libpgquery::PGVariableShowStmt>(stmt));
	case duckdb_libpgquery::T_PGVariableShowSelectStmt:
		return TransformShowSelectStmt(PGCast<duckdb_libpgquery::PGVariableShowSelectStmt>(stmt));
	case duckdb_libpgquery::T_PGCallStmt:
		return TransformCall(PGCast<duckdb_libpgquery::PGCallStmt>(stmt));
	case duckdb_libpgquery::T_PGVariableSetStmt:
		return TransformSet(PGCast<duckdb_libpgquery::PGVariableSetStmt>(stmt));
	case duckdb_libpgquery::T_PGCheckPointStmt:
		return TransformCheckpoint(PGCast<duckdb_libpgquery::PGCheckPointStmt>(stmt));
	case duckdb_libpgquery::T_PGLoadStmt:
		return TransformLoad(PGCast<duckdb_libpgquery::PGLoadStmt>(stmt));
	case duckdb_libpgquery::T_PGCreateTypeStmt:
		return TransformCreateType(PGCast<duckdb_libpgquery::PGCreateTypeStmt>(stmt));
	case duckdb_libpgquery::T_PGAlterSeqStmt:
		return TransformAlterSequence(PGCast<duckdb_libpgquery::PGAlterSeqStmt>(stmt));
	case duckdb_libpgquery::T_PGAttachStmt:
		return TransformAttach(PGCast<duckdb_libpgquery::PGAttachStmt>(stmt));
	case duckdb_libpgquery::T_PGDetachStmt:
		return TransformDetach(PGCast<duckdb_libpgquery::PGDetachStmt>(stmt));
	case duckdb_libpgquery::T_PGUseStmt:
		return TransformUse(PGCast<duckdb_libpgquery::PGUseStmt>(stmt));
	case duckdb_libpgquery::T_PGCopyDatabaseStmt:
		return TransformCopyDatabase(PGCast<duckdb_libpgquery::PGCopyDatabaseStmt>(stmt));
	case duckdb_libpgquery::T_PGCreateSecretStmt:
		return TransformSecret(PGCast<duckdb_libpgquery::PGCreateSecretStmt>(stmt));
	case duckdb_libpgquery::T_PGDropSecretStmt:
		return TransformDropSecret(PGCast<duckdb_libpgquery::PGDropSecretStmt>(stmt));
	case duckdb_libpgquery::T_PGCommentOnStmt:
		return TransformCommentOn(PGCast<duckdb_libpgquery::PGCommentOnStmt>(stmt));
	default:
		throw NotImplementedException(NodetypeToString(stmt.type));
	}
}

unique_ptr<QueryNode> Transformer::TransformMaterializedCTE(unique_ptr<QueryNode> root) {
	// Extract materialized CTEs from cte_map
	vector<unique_ptr<CTENode>> materialized_ctes;

	for (auto &cte : root->cte_map.map) {
		auto &cte_entry = cte.second;
		if (cte_entry->materialized == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
			auto mat_cte = make_uniq<CTENode>();
			mat_cte->ctename = cte.first;
			mat_cte->query = cte_entry->query->node->Copy();
			mat_cte->aliases = cte_entry->aliases;
			materialized_ctes.push_back(std::move(mat_cte));
		}
	}

	while (!materialized_ctes.empty()) {
		unique_ptr<CTENode> node_result;
		node_result = std::move(materialized_ctes.back());
		node_result->cte_map = root->cte_map.Copy();
		node_result->child = std::move(root);
		root = std::move(node_result);
		materialized_ctes.pop_back();
	}

	return root;
}

void Transformer::SetQueryLocation(ParsedExpression &expr, int query_location) {
	if (query_location < 0) {
		return;
	}
	expr.query_location = optional_idx(idx_t(query_location));
}

void Transformer::SetQueryLocation(TableRef &ref, int query_location) {
	if (query_location < 0) {
		return;
	}
	ref.query_location = optional_idx(idx_t(query_location));
}

} // namespace duckdb
