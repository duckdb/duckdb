// within_index: PoC extension that INTERCEPTS the core WITHIN vector range-join.
//
// Registers:
//  (1) a table function `within_index_scan(nested_query)` — represents an index-backed
//      radius scan. During init_global it EXECUTES the embedded radius join (stand-in for a
//      real HNSW / compressed-index lookup) and caches the rows; the scan emits them, so the
//      rewritten plan is SEMANTICALLY identical to the un-intercepted query.
//  (2) an OptimizerExtension (pre-optimization hook) that walks the logical plan and, for
//      each LOGICAL_WITHIN_JOIN, replaces it with a LogicalGet over within_index_scan,
//      remapping the join's left/right column bindings into the single scan index with the
//      core's ColumnBindingReplacer.
//
// Why the pre-optimization hook: the core join-order optimizer runs first and, on its
// round-trip, normalizes the tagged node back to a base ANY_JOIN (the WITHIN_JOIN tag does
// not survive the full optimizer). At the pre-optimization point the node still carries
// LOGICAL_WITHIN_JOIN, so the index extension can claim the vector join before the core
// optimizer reorders it — the natural interception point for a vector/spatial index.
//
// Proof (poc_verify.sql + poc_contrast.sql):
//  * `JOIN r WITHIN BY array_distance(l.v, r.v) 0.5` -> EXPLAIN shows the join REPLACED by
//    `Table Function within_index_scan(...)` (the interception), and the query returns the
//    radius-filtered rows.
//  * plain `JOIN r ON array_distance(l.v, r.v) <= 0.5` (no WITHIN BY) -> NOT intercepted,
//    still `Blockwise Nl Join` (the core never invents the tag on its own).
//  * the intercepted result == the plain-join result (same radius filter).
#include <sstream>
#include <functional>

#include "within_index_extension.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Table function: within_index_scan(nested_query)
//===----------------------------------------------------------------------===//
struct WithinIndexBindData : public TableFunctionData {
	string nested_query;
};

struct WithinIndexGlobalState : public GlobalTableFunctionState {
	unique_ptr<QueryResult> result;
	bool done = false;
	idx_t total_rows = 0;
};

struct WithinIndexLocalState : public LocalTableFunctionState {};

static unique_ptr<FunctionData> WithinIndexBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<WithinIndexBindData>();
	if (input.inputs.size() >= 1 && !input.inputs[0].IsNull()) {
		result->nested_query = input.inputs[0].ToString();
	}
	// The schema is carried on the LogicalGet built by the optimizer; bind() returns it.
	// (No external input needed — the optimizer supplies ret_types/ret_names directly.)
	(void)context;
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> WithinIndexInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<WithinIndexGlobalState>();
	auto &bind = input.bind_data->Cast<WithinIndexBindData>();
	if (bind.nested_query.empty()) {
		state->result = nullptr;
		state->done = true;
		return std::move(state);
	}
	// Execute the "index radius scan" (stand-in for a real ANN / radius-index lookup).
	Connection con(*context.db);
	state->result = con.Query(bind.nested_query);
	if (state->result->HasError()) {
		state->result = nullptr;
	}
	state->total_rows = state->result ? state->result->RowCount() : 0;
	state->done = true;
	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> WithinIndexInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state) {
	return make_uniq<WithinIndexLocalState>();
}

static OperatorResultType WithinIndexScan(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
                                          DataChunk &output) {
	auto &gstate = data.global_state->Cast<WithinIndexGlobalState>();
	if (!gstate.result) {
		return OperatorResultType::FINISHED;
	}
	auto chunk = gstate.result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return OperatorResultType::FINISHED;
	}
	for (idx_t c = 0; c < output.ColumnCount() && c < chunk->ColumnCount(); c++) {
		chunk->data[c].Flatten();
		output.data[c].Reference(chunk->data[c]);
	}
	output.SetChildCardinality(chunk->size());
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

static TableFunction MakeWithinIndexScan() {
	auto fn = make_uniq<TableFunction>(Identifier("within_index_scan"), vector<LogicalType>{LogicalType::VARCHAR},
	                                  nullptr, WithinIndexBind, WithinIndexInitGlobal, WithinIndexInitLocal);
	fn->in_out_function = WithinIndexScan;
	fn->parallelism = TableFunctionParallelism::FORCE_SINGLE_THREADED;
	fn->return_type = TableFunctionReturnType::SET_RETURNING_FUNCTION;
	return *fn;
}

//===----------------------------------------------------------------------===//
// Helpers: read the join's vector columns + radius from the bound condition
//===----------------------------------------------------------------------===//
static string ColName(const Expression &e) {
	if (e.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return e.Cast<BoundColumnRefExpression>().GetName().GetIdentifierName();
	}
	return e.ToString();
}

// condition = array_distance(left_expr, right_expr) <= const
static bool ExtractWithin(const Expression &cond, string &vec_left, string &vec_right, double &eps) {
	if (cond.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &fn = cond.Cast<BoundFunctionExpression>();
	if (fn.GetChildren().size() != 2) {
		return false;
	}
	auto &lhs = *fn.GetChildren()[0]; // array_distance(a, b)
	if (lhs.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION ||
	    lhs.Cast<BoundFunctionExpression>().GetChildren().size() != 2) {
		return false;
	}
	vec_left = ColName(*lhs.Cast<BoundFunctionExpression>().GetChildren()[0]);
	vec_right = ColName(*lhs.Cast<BoundFunctionExpression>().GetChildren()[1]);
	auto &rhs = *fn.GetChildren()[1];
	// The RHS may be CAST(const AS DOUBLE) — which binds to a single-argument operator
	// function (__cast) — or a bare constant. Unwrap one cast level if present.
	Expression *rhs_ptr = &rhs;
	if (rhs.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &rf = rhs.Cast<BoundFunctionExpression>();
		if (rf.IsOperator() && rf.GetChildren().size() == 1) {
			rhs_ptr = rf.GetChildren()[0].get();
		}
	}
	if (rhs_ptr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		eps = rhs_ptr->Cast<BoundConstantExpression>().GetValue().GetValue<double>();
	} else {
		eps = 0.0;
	}
	return true;
}

static string FindBaseTableName(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable();
		if (table) {
			return table->name.GetIdentifierName();
		}
		return string();
	}
	for (auto &child : op.children) {
		if (child) {
			auto name = FindBaseTableName(*child);
			if (!name.empty()) {
				return name;
			}
		}
	}
	return string();
}

static string BaseName(const string &qualified) {
	auto pos = qualified.find('.');
	return pos == string::npos ? qualified : qualified.substr(pos + 1);
}

//===----------------------------------------------------------------------===//
// Optimizer extension: rewrite LOGICAL_WITHIN_JOIN -> within_index_scan LogicalGet
//===----------------------------------------------------------------------===//
static void WithinIndexOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	// Locate the first LOGICAL_WITHIN_JOIN (pre-order DFS).
	struct Frame {
		LogicalOperator *parent;
		LogicalOperator *op;
	};
	vector<Frame> stack;
	stack.push_back({nullptr, plan.get()});
	LogicalOperator *target = nullptr;
	LogicalOperator *target_parent = nullptr;
	while (!stack.empty()) {
		auto f = stack.back();
		stack.pop_back();
		if (f.op->type == LogicalOperatorType::LOGICAL_WITHIN_JOIN) {
			target = f.op;
			target_parent = f.parent;
			break;
		}
		for (auto &child : f.op->children) {
			if (child) {
				stack.push_back({f.op, child.get()});
			}
		}
	}
	if (!target) {
		return; // no WITHIN join in this plan -> nothing to intercept
	}
	auto &join = *target;
	auto &aj = join.Cast<LogicalAnyJoin>();
	auto &left = *aj.children[0];
	auto &right = *aj.children[1];

	string vec_left, vec_right;
	double eps = 0.0;
	ExtractWithin(*aj.condition, vec_left, vec_right, eps);

	string ltable = FindBaseTableName(left);
	string rtable = FindBaseTableName(right);
	optional_ptr<TableCatalogEntry> ltable_entry = nullptr;
	optional_ptr<TableCatalogEntry> rtable_entry = nullptr;
	if (left.type == LogicalOperatorType::LOGICAL_GET) {
		ltable_entry = left.Cast<LogicalGet>().GetTable();
	}
	if (right.type == LogicalOperatorType::LOGICAL_GET) {
		rtable_entry = right.Cast<LogicalGet>().GetTable();
	}
	if (ltable.empty() || rtable.empty() || !ltable_entry || !rtable_entry) {
		return; // cannot name the base tables (e.g. a subquery child) -> leave the join as-is
	}

	// The join's own `types` is empty at pre-optimization; resolve the real schema from the
	// child table scans, in the CHILD GET's projection (column_ids) order, so that scan
	// position i == child column binding i (catalog order != planner projection order).
	auto &lget = left.Cast<LogicalGet>();
	auto &rget = right.Cast<LogicalGet>();
	auto &lcols = ltable_entry->GetColumns();
	auto &rcols = rtable_entry->GetColumns();
	auto l_ids = lget.GetColumnIds();
	auto r_ids = rget.GetColumnIds();
	idx_t L = l_ids.size();
	idx_t R = r_ids.size();
	vector<LogicalType> ret_types;
	vector<Identifier> ret_names;
	for (idx_t i = 0; i < L; i++) {
		auto ci = l_ids[i].GetPrimaryIndex();
		ret_types.push_back(lcols.GetColumn(LogicalIndex(ci)).Type());
		ret_names.push_back(lcols.GetColumn(LogicalIndex(ci)).Name());
	}
	for (idx_t i = 0; i < R; i++) {
		auto ci = r_ids[i].GetPrimaryIndex();
		ret_types.push_back(rcols.GetColumn(LogicalIndex(ci)).Type());
		ret_names.push_back(rcols.GetColumn(LogicalIndex(ci)).Name());
	}

	// Build the nested "index scan" SQL — the same radius join, as an opaque index lookup.
	// The SELECT list is in the child Get's column_ids order (== the scan's declared output
	// order == the order the parent reads), so columns align positionally.
	std::ostringstream ss;
	ss << "SELECT ";
	bool first = true;
	for (idx_t i = 0; i < L; i++) {
		if (!first) {
			ss << ", ";
		}
		first = false;
		ss << ltable << "." << lcols.GetColumn(LogicalIndex(l_ids[i].GetPrimaryIndex())).Name().GetIdentifierName();
	}
	for (idx_t i = 0; i < R; i++) {
		if (!first) {
			ss << ", ";
		}
		first = false;
		ss << rtable << "." << rcols.GetColumn(LogicalIndex(r_ids[i].GetPrimaryIndex())).Name().GetIdentifierName();
	}
	ss << " FROM " << ltable << " JOIN " << rtable << " ON array_distance(" << ltable << "." << BaseName(vec_left)
	   << ", " << rtable << "." << BaseName(vec_right) << ") <= " << eps;
	string nested = ss.str();

	// Bind data holding the nested query.
	auto bind_data = make_uniq<WithinIndexBindData>();
	bind_data->nested_query = nested;

	// GetTableIndex() is empty at pre-optimization; compute a fresh, collision-free table
	// index by walking every column binding in the plan.
	idx_t new_tidx = 0;
	std::function<void(LogicalOperator &)> walk_tidx = [&](LogicalOperator &op) {
		for (auto &cb : op.GetColumnBindings()) {
			new_tidx = MaxValue(new_tidx, (idx_t)cb.table_index.index + 1);
		}
		for (auto &c : op.children) {
			if (c) {
				walk_tidx(*c);
			}
		}
	};
	walk_tidx(*plan);

	TableFunction fn = MakeWithinIndexScan();
	BoundTableFunction bfn(fn);
	auto new_get = make_uniq<LogicalGet>(TableIndex(new_tidx), std::move(bfn), std::move(bind_data), std::move(ret_types),
	                                     std::move(ret_names));
	for (idx_t i = 0; i < L + R; i++) {
		new_get->AddColumnId((column_t)i);
	}

	// Remap the parent's column references: the join's left/right children's column bindings
	// -> the new scan's projection positions.
	auto left_b = left.GetColumnBindings();
	auto right_b = right.GetColumnBindings();
	vector<ColumnBinding> old_bindings, new_bindings;
	for (idx_t i = 0; i < left_b.size(); i++) {
		old_bindings.push_back(left_b[i]);
		new_bindings.emplace_back(ColumnBinding(TableIndex(new_tidx), ProjectionIndex(i)));
	}
	for (idx_t i = 0; i < right_b.size(); i++) {
		old_bindings.push_back(right_b[i]);
		new_bindings.emplace_back(ColumnBinding(TableIndex(new_tidx), ProjectionIndex(L + i)));
	}

	// Swap the join for the scan in the parent (or at the root).
	if (target_parent) {
		for (auto &c : target_parent->children) {
			if (c.get() == &join) {
				c = std::move(new_get);
				break;
			}
		}
	} else {
		plan = std::move(new_get);
	}

	// Remap every column reference above the join to the new scan index.
	ColumnBindingReplacer replacer;
	replacer.AddReplacements(old_bindings, new_bindings);
	replacer.VisitOperator(*plan);
}

//===----------------------------------------------------------------------===//
// Extension entry
//===----------------------------------------------------------------------===//
static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(MakeWithinIndexScan());

	auto &config = loader.GetDatabaseInstance().config;
	OptimizerExtension ext;
	ext.pre_optimize_function = WithinIndexOptimize;
	OptimizerExtension::Register(config, std::move(ext));
}

void WithinIndexExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string WithinIndexExtension::Name() {
	return "within_index";
}
std::string WithinIndexExtension::Version() const {
	return "0.1.0";
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(within_index, loader) {
	duckdb::LoadInternal(loader);
}
}
