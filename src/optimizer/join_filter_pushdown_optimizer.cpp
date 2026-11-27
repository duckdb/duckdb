#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"

#include "duckdb/common/reference_map.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

struct FilterTarget {
	FilterTarget(LogicalOperator &op, vector<JoinFilterPushdownColumn> columns) : op(op), columns(std::move(columns)) {
	}

	LogicalOperator &op;
	vector<JoinFilterPushdownColumn> columns;
};

JoinFilterPushdownOptimizer::JoinFilterPushdownOptimizer(Optimizer &optimizer) : optimizer(optimizer) {
}

static bool IsJoinFilterPushdownIntegralType(const LogicalType &type) {
	return type.IsIntegral() && GetTypeIdSize(type.InternalType()) <= GetTypeIdSize(PhysicalType::INT64);
}

static bool IsJoinFilterPushdownIntegralCast(const LogicalType &src, const LogicalType &tgt) {
	return IsJoinFilterPushdownIntegralType(src) && IsJoinFilterPushdownIntegralType(tgt);
}

static bool IsJoinFilterPushdownVariantIntegralCast(const LogicalType &src, const LogicalType &tgt) {
	if (src.id() == LogicalTypeId::VARIANT) {
		return IsJoinFilterPushdownIntegralType(tgt);
	}
	if (tgt.id() == LogicalTypeId::VARIANT) {
		return IsJoinFilterPushdownIntegralType(src);
	}
	return false;
}

struct JoinFilterExpressionPath {
	ColumnBinding binding;
	JoinFilterPushdownMode mode = JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION;
	unique_ptr<Expression> input_expression;
	bool has_struct_extract = false;
};

static void CopyJoinFilterExpression(const Expression &expr, JoinFilterExpressionPath &path) {
	auto result = expr.Copy();
	result->Cast<BoundFunctionExpression>().GetChildrenMutable()[0] = std::move(path.input_expression);
	path.input_expression = std::move(result);
}

static bool PushdownJoinFilterExpressionInternal(const Expression &expr, JoinFilterExpressionPath &path,
                                                 bool allow_struct = false) {
	const auto &return_type = expr.GetReturnType();
	if (return_type.IsNested() && return_type.id() != LogicalTypeId::VARIANT &&
	    !(allow_struct && StructType::IsStruct(return_type))) {
		return false;
	}
	if (return_type.id() == LogicalTypeId::INTERVAL || expr.IsVolatile()) {
		return false;
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		if (colref.Depth() != 0) {
			return false;
		}
		path.binding = colref.Binding();
		path.input_expression = make_uniq<BoundReferenceExpression>(return_type, idx_t(0));
		return true;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (!BoundCastExpression::IsCast(expr)) {
			idx_t child_idx;
			if (TryGetStructExtractChildIndex(function, child_idx) && !function.GetChildren().empty() &&
			    StructType::IsStruct(function.GetChildren()[0]->GetReturnType())) {
				if (!PushdownJoinFilterExpressionInternal(*function.GetChildren()[0], path, true)) {
					return false;
				}
				path.has_struct_extract = true;
			} else {
				if (function.Function().GetName() != "variant_normalize" || function.GetChildren().size() != 1 ||
				    !PushdownJoinFilterExpressionInternal(*function.GetChildren()[0], path)) {
					return false;
				}
			}
		} else {
			const auto &source = BoundCastExpression::Child(function).GetReturnType();
			const auto &target = function.GetReturnType();
			if ((!IsJoinFilterPushdownIntegralCast(source, target) &&
			     !IsJoinFilterPushdownVariantIntegralCast(source, target)) ||
			    !PushdownJoinFilterExpressionInternal(BoundCastExpression::Child(function), path)) {
				return false;
			}
			if (target.id() == LogicalTypeId::VARIANT) {
				path.mode = JoinFilterPushdownMode::STORAGE_ONLY;
			}
		}
		CopyJoinFilterExpression(expr, path);
		return true;
	}
	default:
		return false;
	}
}

static bool RuntimeFilterInputMatchesType(const Expression &expr, const LogicalType &type) {
	bool found_reference = false;
	bool matches = true;
	ExpressionIterator::VisitExpressionClass(expr, ExpressionClass::BOUND_REF, [&](const Expression &child) {
		found_reference = true;
		auto &ref = child.Cast<BoundReferenceExpression>();
		matches &= ref.Index() == 0 && ref.GetReturnType() == type;
	});
	return found_reference && matches;
}

static bool RuntimeFilterInputHasStructExtract(const Expression &expr) {
	bool result = false;
	ExpressionIterator::VisitExpressionClass(expr, ExpressionClass::BOUND_FUNCTION, [&](const Expression &child) {
		idx_t child_idx;
		result |= TryGetStructExtractChildIndex(child.Cast<BoundFunctionExpression>(), child_idx);
	});
	return result;
}

bool JoinFilterPushdownUtil::PushdownJoinFilterExpression(const Expression &expr, JoinFilterPushdownColumn &filter) {
	JoinFilterExpressionPath path;
	if (!PushdownJoinFilterExpressionInternal(expr, path, filter.input_expression != nullptr)) {
		return false;
	}
	if (filter.input_expression && !RuntimeFilterInputMatchesType(*filter.input_expression, expr.GetReturnType())) {
		return false;
	}
	const auto storage_only =
	    path.mode == JoinFilterPushdownMode::STORAGE_ONLY || filter.mode == JoinFilterPushdownMode::STORAGE_ONLY;
	if (storage_only && (path.has_struct_extract ||
	                     (filter.input_expression && RuntimeFilterInputHasStructExtract(*filter.input_expression)))) {
		return false;
	}
	filter.probe_column_index = path.binding;
	if (storage_only) {
		filter.mode = JoinFilterPushdownMode::STORAGE_ONLY;
		filter.input_expression.reset();
		return true;
	}
	if (filter.input_expression) {
		auto combined = filter.input_expression->Copy();
		ExpressionFilter::ReplaceExpressionRecursive(combined, *path.input_expression);
		filter.input_expression = std::move(combined);
	} else {
		filter.input_expression = std::move(path.input_expression);
	}
	return true;
}

bool JoinFilterPushdownUtil::JoinTypeIsSupported(JoinType join_type) {
	switch (join_type) {
	case JoinType::MARK:
	case JoinType::SINGLE:
	case JoinType::LEFT:
	case JoinType::OUTER:
	case JoinType::ANTI:
	case JoinType::RIGHT_ANTI:
		// cannot generate join filters for these join types
		// mark/single - cannot change cardinality of probe side
		// left/outer always need to include every row from probe side
		// anti - the probe rows without a match are the result
		// right_anti - join filters are generated (see CanGenerateJoinFilters), but the build side optimizer, which
		// asks this function as well, does not take them into account
		return false;
	default:
		return true;
	}
}

struct FilterColumnHash {
	hash_t operator()(const JoinFilterPushdownColumn &column) const {
		auto result = CombineHash(Hash(column.join_filter_idx), ColumnBindingHashFunction()(column.probe_column_index));
		result = CombineHash(result, column.storage_type.Hash());
		result = CombineHash(result, Hash(uint8_t(column.mode)));
		if (column.input_expression) {
			result = CombineHash(result, column.input_expression->Hash());
		}
		return result;
	}
};

struct FilterColumnEquality {
	bool operator()(const JoinFilterPushdownColumn &a, const JoinFilterPushdownColumn &b) const {
		if (a.join_filter_idx != b.join_filter_idx || a.probe_column_index != b.probe_column_index ||
		    a.storage_type != b.storage_type || a.mode != b.mode ||
		    bool(a.input_expression) != bool(b.input_expression)) {
			return false;
		}
		return !a.input_expression || a.input_expression->Equals(*b.input_expression);
	}
};

using filter_column_set_t = unordered_set<JoinFilterPushdownColumn, FilterColumnHash, FilterColumnEquality>;
using filter_equality_graph_t = column_binding_map_t<vector<ColumnBinding>>;

enum class FilterTargetMode : uint8_t { SCANS, SCANS_AND_CTE_REFERENCES };

class FilterTargetFinder {
public:
	explicit FilterTargetFinder(FilterTargetMode mode,
	                            EqualityPropagation equality_propagation = EqualityPropagation::DISABLED,
	                            bool cross_secure_views = false)
	    : mode(mode), equality_propagation(equality_propagation), cross_secure_views(cross_secure_views) {
	}

	void Find(LogicalOperator &op, const vector<JoinFilterPushdownColumn> &columns, vector<FilterTarget> &targets) {
		FindTargets(op, FilterColumns(columns), targets);
	}

private:
	//! Borrowed for synchronous traversal; remapped columns start without an expansion region.
	struct FilterColumns {
		explicit FilterColumns(const vector<JoinFilterPushdownColumn> &columns,
		                       optional_ptr<const filter_equality_graph_t> expanded_for = nullptr)
		    : columns(columns), expanded_for(expanded_for) {
		}

		const vector<JoinFilterPushdownColumn> &columns;
		const optional_ptr<const filter_equality_graph_t> expanded_for;
	};

	void FindTargets(LogicalOperator &op, const FilterColumns &state, vector<FilterTarget> &targets);
	void FindJoinFilterTargets(LogicalOperator &op, const FilterColumns &state, vector<FilterTarget> &targets);
	void FindSetOperationFilterTargets(LogicalSetOperation &setop, const vector<JoinFilterPushdownColumn> &columns,
	                                   vector<FilterTarget> &targets);
	void FindWindowFilterTargets(LogicalWindow &window, const vector<JoinFilterPushdownColumn> &columns,
	                             vector<FilterTarget> &targets);
	static void FindScanFilterTargets(LogicalGet &get, const vector<JoinFilterPushdownColumn> &columns,
	                                  vector<FilterTarget> &targets);
	static void PushdownProjectionColumns(LogicalProjection &proj, const vector<JoinFilterPushdownColumn> &columns,
	                                      vector<JoinFilterPushdownColumn> &projected_columns);
	static void PushdownAggregateColumns(LogicalAggregate &aggr, const vector<JoinFilterPushdownColumn> &columns,
	                                     vector<JoinFilterPushdownColumn> &group_columns);
	static vector<JoinFilterPushdownColumn>
	ExpandEquivalentFilterColumns(const filter_equality_graph_t &equalities,
	                              const vector<JoinFilterPushdownColumn> &columns);
	static void AddFilterEquality(filter_equality_graph_t &equalities, ColumnBinding lhs, ColumnBinding rhs);
	static void CollectJoinFilterEqualities(const LogicalComparisonJoin &join, filter_equality_graph_t &equalities);
	static bool OperatorHasVolatileExpressions(const LogicalOperator &op);
	static bool FilterTargetPreservesLeftInput(JoinType type);

	const filter_equality_graph_t &GetEqualities(const LogicalOperator &op);
	void CollectEqualities(const LogicalOperator &op, const shared_ptr<filter_equality_graph_t> &graph);
	bool CanSkipSubtree(const LogicalOperator &op);
	bool CanSkipSubtreeInternal(const LogicalOperator &op);

private:
	const FilterTargetMode mode;
	const EqualityPropagation equality_propagation;
	const bool cross_secure_views;
	reference_map_t<const LogicalOperator, bool> skippable_subtrees;
	reference_map_t<const LogicalOperator, shared_ptr<filter_equality_graph_t>> equality_graphs;
};

void FilterTargetFinder::PushdownProjectionColumns(LogicalProjection &proj,
                                                   const vector<JoinFilterPushdownColumn> &columns,
                                                   vector<JoinFilterPushdownColumn> &projected_columns) {
	for (auto &filter : columns) {
		if (filter.probe_column_index.table_index != proj.table_index) {
			continue;
		}
		auto candidate = filter.Copy();
		auto &expr = proj.GetExpression(candidate.probe_column_index);
		if (!JoinFilterPushdownUtil::PushdownJoinFilterExpression(expr, candidate)) {
			continue;
		}
		projected_columns.push_back(std::move(candidate));
	}
}

void FilterTargetFinder::PushdownAggregateColumns(LogicalAggregate &aggr,
                                                  const vector<JoinFilterPushdownColumn> &columns,
                                                  vector<JoinFilterPushdownColumn> &group_columns) {
	for (auto &filter : columns) {
		if (filter.probe_column_index.table_index != aggr.group_index) {
			continue;
		}
		const auto group_index = filter.probe_column_index.column_index;
		bool present_in_all = true;
		for (auto &grouping_set : aggr.grouping_sets) {
			present_in_all &= grouping_set.count(group_index) != 0;
		}
		if (!present_in_all) {
			continue;
		}
		auto candidate = filter.Copy();
		auto &expr = aggr.GetGroupExpression(group_index);
		if (!JoinFilterPushdownUtil::PushdownJoinFilterExpression(expr, candidate)) {
			continue;
		}
		group_columns.push_back(std::move(candidate));
	}
}

void FilterTargetFinder::AddFilterEquality(filter_equality_graph_t &equalities, ColumnBinding lhs, ColumnBinding rhs) {
	equalities[lhs].push_back(rhs);
	equalities[rhs].push_back(lhs);
}

void FilterTargetFinder::CollectJoinFilterEqualities(const LogicalComparisonJoin &join,
                                                     filter_equality_graph_t &equalities) {
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL ||
		    condition.GetLHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    condition.GetRHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		auto &lhs = condition.GetLHS().Cast<BoundColumnRefExpression>();
		auto &rhs = condition.GetRHS().Cast<BoundColumnRefExpression>();
		if (lhs.Depth() == 0 && rhs.Depth() == 0 && lhs.GetReturnType() == rhs.GetReturnType()) {
			AddFilterEquality(equalities, lhs.Binding(), rhs.Binding());
		}
	}
}

void FilterTargetFinder::FindScanFilterTargets(LogicalGet &get, const vector<JoinFilterPushdownColumn> &columns,
                                               vector<FilterTarget> &targets) {
	if (!get.function.filter_pushdown || get.ordinality_idx.IsValid() || get.extra_info.sample_options) {
		return;
	}
	const auto bindings = get.GetColumnBindings();
	vector<JoinFilterPushdownColumn> accepted_columns;
	filter_column_set_t seen;
	for (auto &input_filter : columns) {
		if (input_filter.probe_column_index.table_index != get.table_index) {
			continue;
		}
		if (get.function.supports_pushdown_type) {
			auto column_id = get.GetColumnIndex(input_filter.probe_column_index).GetPrimaryIndex();
			if (!get.function.supports_pushdown_type(*get.bind_data, column_id)) {
				continue;
			}
		}
		auto filter = input_filter.Copy();
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (filter.probe_column_index.column_index == bindings[i].column_index) {
				filter.storage_type = get.GetColumnType(get.GetColumnIndex(bindings[i]));
				break;
			}
		}
		D_ASSERT(filter.storage_type != LogicalType::INVALID);
		if (filter.input_expression && !RuntimeFilterInputMatchesType(*filter.input_expression, filter.storage_type)) {
			continue;
		}
		const auto reconstructed_type = RuntimeFilterCastUtil::GetRuntimeFilterInputType(filter, filter.storage_type);
		if (filter.storage_type.id() == LogicalTypeId::VARIANT &&
		    filter.mode == JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION &&
		    reconstructed_type.id() == LogicalTypeId::VARIANT) {
			continue;
		}
		auto inserted = seen.insert(std::move(filter));
		if (inserted.second) {
			accepted_columns.push_back(inserted.first->Copy());
		}
	}
	if (!accepted_columns.empty()) {
		targets.emplace_back(get, std::move(accepted_columns));
	}
}

bool FilterTargetFinder::OperatorHasVolatileExpressions(const LogicalOperator &op) {
	bool result = false;
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](const unique_ptr<Expression> *expr) { result |= (*expr)->IsVolatile(); });
	return result;
}

void FilterTargetFinder::FindSetOperationFilterTargets(LogicalSetOperation &setop,
                                                       const vector<JoinFilterPushdownColumn> &columns,
                                                       vector<FilterTarget> &targets) {
	for (auto &child : setop.children) {
		vector<JoinFilterPushdownColumn> child_columns;
		const auto child_bindings = child->GetColumnBindings();
		for (auto &filter : columns) {
			if (filter.probe_column_index.table_index != setop.table_index) {
				continue;
			}
			auto candidate = filter.Copy();
			candidate.probe_column_index = child_bindings[filter.probe_column_index.column_index];
			child_columns.push_back(std::move(candidate));
		}
		FindTargets(*child, FilterColumns(child_columns), targets);
		if (setop.type == LogicalOperatorType::LOGICAL_EXCEPT) {
			break;
		}
	}
}

bool FilterTargetFinder::FilterTargetPreservesLeftInput(JoinType type) {
	switch (type) {
	case JoinType::INNER:
	case JoinType::LEFT:
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
	case JoinType::SINGLE:
		return true;
	default:
		// Filtering a NULL-generating input can create new unmatched rows.
		return false;
	}
}

const filter_equality_graph_t &FilterTargetFinder::GetEqualities(const LogicalOperator &op) {
	auto entry = equality_graphs.find(op);
	if (entry != equality_graphs.end()) {
		return *entry->second;
	}
	auto graph = make_shared_ptr<filter_equality_graph_t>();
	CollectEqualities(op, graph);
	return *graph;
}

bool FilterTargetFinder::CanSkipSubtree(const LogicalOperator &op) {
	auto entry = skippable_subtrees.find(op);
	if (entry != skippable_subtrees.end()) {
		return entry->second;
	}
	const auto result = CanSkipSubtreeInternal(op);
	skippable_subtrees.emplace(op, result);
	return result;
}

void FilterTargetFinder::CollectEqualities(const LogicalOperator &op,
                                           const shared_ptr<filter_equality_graph_t> &graph) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::INNER) {
			return;
		}
		CollectJoinFilterEqualities(join, *graph);
		break;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			auto &expr = *projection.expressions[i];
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			auto &column = expr.Cast<BoundColumnRefExpression>();
			if (column.Depth() == 0) {
				AddFilterEquality(*graph, ColumnBinding(projection.table_index, ProjectionIndex(i)), column.Binding());
			}
		}
		break;
	}
	default:
		return;
	}
	equality_graphs.emplace(op, graph);
	for (auto &child : op.children) {
		CollectEqualities(*child, graph);
	}
}

bool FilterTargetFinder::CanSkipSubtreeInternal(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_UPDATE:
	case LogicalOperatorType::LOGICAL_DELETE:
	case LogicalOperatorType::LOGICAL_MERGE_INTO:
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
	case LogicalOperatorType::LOGICAL_CTE_REF:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		return false;
	default:
		break;
	}
	if (OperatorHasVolatileExpressions(op)) {
		return false;
	}
	for (auto &child : op.children) {
		if (!CanSkipSubtree(*child)) {
			return false;
		}
	}
	return true;
}

vector<JoinFilterPushdownColumn>
FilterTargetFinder::ExpandEquivalentFilterColumns(const filter_equality_graph_t &equalities,
                                                  const vector<JoinFilterPushdownColumn> &columns) {
	D_ASSERT(!equalities.empty());
	vector<JoinFilterPushdownColumn> result;
	filter_column_set_t seen;
	for (auto &column : columns) {
		if (seen.insert(column.Copy()).second) {
			result.push_back(column.Copy());
		}
	}
	for (idx_t i = 0; i < result.size(); i++) {
		auto entry = equalities.find(result[i].probe_column_index);
		if (entry == equalities.end()) {
			continue;
		}
		for (auto &binding : entry->second) {
			auto inferred = result[i].Copy();
			inferred.probe_column_index = binding;
			auto inserted = seen.insert(std::move(inferred));
			if (inserted.second) {
				result.push_back(inserted.first->Copy());
			}
		}
	}
	return result;
}

void FilterTargetFinder::FindJoinFilterTargets(LogicalOperator &op, const FilterColumns &state,
                                               vector<FilterTarget> &targets) {
	if (OperatorHasVolatileExpressions(op)) {
		return;
	}
	const auto inner = op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT ||
	                   op.Cast<LogicalComparisonJoin>().join_type == JoinType::INNER;
	if (!inner && !FilterTargetPreservesLeftInput(op.Cast<LogicalComparisonJoin>().join_type)) {
		return;
	}
	optional_ptr<const filter_equality_graph_t> equalities;
	if (inner && equality_propagation == EqualityPropagation::ENABLED) {
		equalities = GetEqualities(op);
	}
	const auto needs_expansion = equalities && state.expanded_for.get() != equalities.get() && !equalities->empty();
	vector<JoinFilterPushdownColumn> expanded_columns;
	if (needs_expansion) {
		expanded_columns = ExpandEquivalentFilterColumns(*equalities, state.columns);
	}
	// Remapping creates fresh state; unchanged columns share their expansion within the same equality region.
	const FilterColumns child_state(needs_expansion ? expanded_columns : state.columns, equalities);
	FindTargets(*op.children[0], child_state, targets);
	if (!inner) {
		return;
	}
	vector<FilterTarget> right_targets;
	FindTargets(*op.children[1], child_state, right_targets);
	// An empty build can skip the probe input, including expressions outside the filtered branch.
	if (!right_targets.empty() && CanSkipSubtree(*op.children[0])) {
		for (auto &target : right_targets) {
			targets.push_back(std::move(target));
		}
	}
}

void FilterTargetFinder::FindWindowFilterTargets(LogicalWindow &window, const vector<JoinFilterPushdownColumn> &columns,
                                                 vector<FilterTarget> &targets) {
	vector<JoinFilterPushdownColumn> partition_columns;
	for (auto &filter : columns) {
		bool shared_partition = filter.probe_column_index.table_index != window.window_index;
		for (auto &expr : window.expressions) {
			bool found = false;
			for (auto &partition : expr->Cast<BoundWindowExpression>().Partitions()) {
				if (partition->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
					continue;
				}
				auto &column = partition->Cast<BoundColumnRefExpression>();
				found |= column.Depth() == 0 && column.Binding() == filter.probe_column_index;
			}
			shared_partition &= found;
		}
		if (shared_partition) {
			partition_columns.push_back(filter.Copy());
		}
	}
	FindTargets(*window.children[0], FilterColumns(partition_columns), targets);
}

void FilterTargetFinder::FindTargets(LogicalOperator &op, const FilterColumns &state, vector<FilterTarget> &targets) {
	const auto &columns = state.columns;
	if (columns.empty() || OperatorHasVolatileExpressions(op)) {
		return;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		FindScanFilterTargets(op.Cast<LogicalGet>(), columns, targets);
		break;
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		if (mode != FilterTargetMode::SCANS_AND_CTE_REFERENCES) {
			break;
		}
		auto &ref = op.Cast<LogicalCTERef>();
		if (ref.is_recurring || ref.correlated_columns != 0) {
			break;
		}
		vector<JoinFilterPushdownColumn> accepted;
		for (auto &input_column : columns) {
			if (input_column.probe_column_index.table_index != ref.table_index ||
			    !input_column.probe_column_index.column_index.IsValid() ||
			    input_column.probe_column_index.column_index.GetIndex() >= ref.chunk_types.size()) {
				continue;
			}
			auto column = input_column.Copy();
			column.storage_type = ref.chunk_types[column.probe_column_index.column_index.GetIndex()];
			if (column.storage_type.id() == LogicalTypeId::VARIANT &&
			    RuntimeFilterCastUtil::GetRuntimeFilterInputType(column, column.storage_type).id() ==
			        LogicalTypeId::VARIANT &&
			    column.mode == JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION) {
				continue;
			}
			accepted.push_back(std::move(column));
		}
		if (!accepted.empty()) {
			targets.emplace_back(ref, std::move(accepted));
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		vector<JoinFilterPushdownColumn> projected_columns;
		PushdownProjectionColumns(op.Cast<LogicalProjection>(), columns, projected_columns);
		FindTargets(*op.children[0], FilterColumns(projected_columns), targets);
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		vector<JoinFilterPushdownColumn> group_columns;
		PushdownAggregateColumns(op.Cast<LogicalAggregate>(), columns, group_columns);
		FindTargets(*op.children[0], FilterColumns(group_columns), targets);
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &join = op.Cast<LogicalAnyJoin>();
		// Only remove left rows where that cannot introduce null-extended rows or change volatile predicates.
		if (join.join_type != JoinType::INNER || join.condition->IsVolatile()) {
			break;
		}
		FindTargets(*join.children[0], state, targets);
		break;
	}
	case LogicalOperatorType::LOGICAL_UNNEST: {
		vector<JoinFilterPushdownColumn> input_columns;
		for (auto &filter : columns) {
			if (filter.probe_column_index.table_index != op.Cast<LogicalUnnest>().unnest_index) {
				input_columns.push_back(filter.Copy());
			}
		}
		FindTargets(*op.children[0], FilterColumns(input_columns), targets);
		break;
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNION:
		FindSetOperationFilterTargets(op.Cast<LogicalSetOperation>(), columns, targets);
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		FindJoinFilterTargets(op, state, targets);
		break;
	case LogicalOperatorType::LOGICAL_WINDOW:
		FindWindowFilterTargets(op.Cast<LogicalWindow>(), columns, targets);
		break;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		if (CanSkipSubtree(*op.children[0])) {
			FindTargets(*op.children[1], state, targets);
		}
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_FILTER:
		FindTargets(*op.children[0], state, targets);
		break;
	case LogicalOperatorType::LOGICAL_SECURE_VIEW:
		if (!cross_secure_views) {
			break;
		}
		// a join filter enters the view as a pushed WHERE filter would, and needs no barrier: it cannot fail, as a cast
		// that can fail is a TRY_CAST that keeps the rows it cannot convert for the join
		// (PreserveRuntimeFilterCastErrors)
		FindTargets(*op.children[0], state, targets);
		break;
	default:
		break;
	}
}

void JoinFilterPushdownOptimizer::GetPushdownFilterTargets(LogicalOperator &op,
                                                           const vector<JoinFilterPushdownColumn> &columns,
                                                           vector<PushdownFilterTarget> &targets,
                                                           EqualityPropagation equality_propagation,
                                                           bool cross_secure_views) {
	FilterTargetFinder finder(FilterTargetMode::SCANS, equality_propagation, cross_secure_views);
	vector<FilterTarget> result;
	finder.Find(op, columns, result);
	for (auto &target : result) {
		targets.emplace_back(target.op.Cast<LogicalGet>(), std::move(target.columns));
	}
}

void JoinFilterPushdownOptimizer::GetCTEFilterTargets(
    LogicalOperator &op, const vector<JoinFilterPushdownColumn> &columns,
    reference_map_t<LogicalCTERef, vector<JoinFilterPushdownColumn>> &targets) {
	FilterTargetFinder finder(FilterTargetMode::SCANS_AND_CTE_REFERENCES);
	vector<FilterTarget> result;
	finder.Find(op, columns, result);
	for (auto &target : result) {
		if (target.op.type != LogicalOperatorType::LOGICAL_CTE_REF) {
			continue;
		}
		auto &cte_columns = targets[target.op.Cast<LogicalCTERef>()];
		for (auto &column : target.columns) {
			cte_columns.push_back(std::move(column));
		}
	}
}

bool JoinFilterPushdownOptimizer::IsFiltering(const unique_ptr<LogicalOperator> &op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op->Cast<LogicalGet>();
		return get.table_filters.HasFilters();
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		return true;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		return true;
	}
	default:
		for (const unique_ptr<LogicalOperator> &child : op->children) {
			if (IsFiltering(child)) {
				return true;
			}
		}
		return false;
	}
}

static void SimplifyJoinFilterCastChain(ClientContext &context, JoinFilterPushdownColumn &column,
                                        ExpressionType comparison) {
	if (comparison != ExpressionType::COMPARE_EQUAL || column.mode != JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION ||
	    !IsJoinFilterPushdownIntegralType(column.storage_type) || !column.input_expression) {
		return;
	}
	auto &cast_functions = CastFunctionSet::Get(context);
	optional_ptr<const Expression> current = column.input_expression.get();
	idx_t cast_count = 0;
	while (BoundCastExpression::IsCast(*current)) {
		auto &cast = current->Cast<BoundFunctionExpression>();
		const auto &source_type = BoundCastExpression::Child(cast).GetReturnType();
		const auto &target_type = cast.GetReturnType();
		if (!IsJoinFilterPushdownIntegralType(source_type) || !IsJoinFilterPushdownIntegralType(target_type) ||
		    !RuntimeFilterCastUtil::IsBuiltinCast(cast) ||
		    cast_functions.CanOverrideDefaultCast(source_type, target_type)) {
			return;
		}
		cast_count++;
		current = BoundCastExpression::Child(cast);
	}
	if (cast_count < 2 || current->GetExpressionClass() != ExpressionClass::BOUND_REF ||
	    cast_functions.CanOverrideDefaultCast(column.storage_type, column.input_expression->GetReturnType())) {
		return;
	}
	// Successful integer casts preserve the value; extra filter matches are harmless.
	auto target_type = column.input_expression->GetReturnType();
	column.input_expression = make_uniq<BoundReferenceExpression>(column.storage_type, idx_t(0));
	if (target_type != column.storage_type) {
		column.input_expression =
		    BoundCastExpression::AddCastToType(context, std::move(column.input_expression), target_type, true);
	}
}

bool JoinFilterPushdownOptimizer::CanGenerateJoinFilters(const LogicalComparisonJoin &join) {
	// a RIGHT ANTI join emits the build rows without a match: its probe rows only mark build rows as matched.
	// a probe row whose key is not in the build side cannot mark anything, so it can be filtered out like for
	// a RIGHT SEMI join
	if (join.join_type != JoinType::RIGHT_ANTI && !JoinFilterPushdownUtil::JoinTypeIsSupported(join.join_type)) {
		return false;
	}
	for (auto &condition : join.conditions) {
		auto &expression = condition.IsComparison() ? condition.GetLHS() : condition.GetJoinExpression();
		if (expression.IsVolatile()) {
			return false;
		}
	}
	return true;
}

static bool CanAdaptScanFilters(const LogicalGet &get) {
	return get.children.empty() && !get.has_pushed_projection && !get.ordinality_idx.IsValid() &&
	       !get.table_filters.HasMultiColumnFilters();
}

static bool IsScanKeyConstraint(const LogicalGet &get, ProjectionIndex column_index, const TableFilter &filter) {
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return false;
	}
	auto &expression = filter.Cast<ExpressionFilter>();
	if (!expression.column_indexes.empty()) {
		return false;
	}
	auto &column = get.GetColumnIds()[column_index];
	if (column.IsVirtualColumn() || column.HasChildren() || column.IsPushdownExtract()) {
		return false;
	}
	auto &type = get.GetColumnType(column);
	if (!type.EqualValuesAreIdentical()) {
		return false;
	}
	BoundReferenceExpression key(type, idx_t(0));
	return FilterCombiner::CanMatchExpression(*expression.expr) &&
	       FilterCombiner::IsKeyConstraint(*expression.expr, {key});
}

static void MakeRedundantProbeFiltersAdaptive(LogicalComparisonJoin &join) {
	if (join.join_type != JoinType::INNER || join.children[0]->type != LogicalOperatorType::LOGICAL_GET ||
	    join.children[1]->type != LogicalOperatorType::LOGICAL_GET || !join.expressions.empty()) {
		return;
	}
	auto &probe = join.children[0]->Cast<LogicalGet>();
	auto &build = join.children[1]->Cast<LogicalGet>();
	if (!CanAdaptScanFilters(probe) || !CanAdaptScanFilters(build) || !probe.table_filters.HasFilters() ||
	    !build.table_filters.HasFilters()) {
		return;
	}
	// Computed conditions may have been simplified using the probe filter's statistics.
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL ||
		    condition.GetLHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    condition.GetRHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    condition.GetLHS().GetReturnType() != condition.GetRHS().GetReturnType()) {
			return;
		}
	}
	// Pausing a probe filter must not expose unsafe scan expressions to additional rows.
	for (auto &entry : probe.table_filters) {
		if (!IsScanKeyConstraint(probe, entry.GetIndex(), entry.Filter())) {
			return;
		}
	}
	for (auto &condition : join.conditions) {
		auto &probe_key = condition.GetLHS().Cast<BoundColumnRefExpression>();
		auto &build_key = condition.GetRHS().Cast<BoundColumnRefExpression>();
		if (probe_key.Depth() != 0 || build_key.Depth() != 0 || probe_key.Binding().table_index != probe.table_index ||
		    build_key.Binding().table_index != build.table_index) {
			continue;
		}
		auto probe_column = probe_key.Binding().column_index;
		auto build_column = build_key.Binding().column_index;
		auto probe_filter = probe.table_filters.TryGetFilterByColumnIndex(probe_column);
		auto build_filter = build.table_filters.TryGetFilterByColumnIndex(build_column);
		if (!probe_filter || !build_filter || !IsScanKeyConstraint(build, build_column, *build_filter)) {
			continue;
		}
		auto &probe_expression = probe_filter->Cast<ExpressionFilter>();
		auto &build_expression = build_filter->Cast<ExpressionFilter>();
		if (!probe_expression.Equals(build_expression)) {
			continue;
		}
		// The build predicate and equality still enforce this constraint on every output row.
		probe.table_filters.SetFilterByColumnIndex(
		    probe_column,
		    ExpressionFilter::CreateSelectivityOptionalFilter(probe_expression.expr->Copy(), probe_key.GetReturnType(),
		                                                      SelectivityOptionalFilterType::MIN_MAX));
	}
}

void JoinFilterPushdownOptimizer::GenerateJoinFilters(LogicalComparisonJoin &join) {
	if (!CanGenerateJoinFilters(join)) {
		return;
	}
	MakeRedundantProbeFiltersAdaptive(join);
	// re-order conditions here - otherwise this will happen later on and invalidate the indexes we generate
	PhysicalComparisonJoin::ReorderConditions(join.conditions);
	auto pushdown_info = make_uniq<JoinFilterPushdownInfo>();

	vector<JoinFilterPushdownColumn> pushdown_columns;
	for (idx_t cond_idx = 0; cond_idx < join.conditions.size(); cond_idx++) {
		auto &cond = join.conditions[cond_idx];
		if (!cond.IsComparison()) {
			continue;
		}
		switch (cond.GetComparisonType()) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			break;
		default:
			continue;
		}

		JoinFilterPushdownColumn pushdown_col;
		if (!JoinFilterPushdownUtil::PushdownJoinFilterExpression(cond.GetLHS(), pushdown_col)) {
			continue;
		}

		pushdown_col.join_filter_idx = pushdown_info->join_condition.size();
		pushdown_columns.push_back(std::move(pushdown_col));
		pushdown_info->join_condition.push_back(cond_idx);
	}
	if (pushdown_columns.empty()) {
		// could not generate any filters - bail-out
		return;
	}
	// recurse the query tree to find the LogicalGets in which we can push the filter info
	vector<PushdownFilterTarget> pushdown_filter_targets;
	GetPushdownFilterTargets(*join.children[0], pushdown_columns, pushdown_filter_targets, EqualityPropagation::ENABLED,
	                         true);
	for (auto &target : pushdown_filter_targets) {
		vector<JoinFilterPushdownColumn> get_columns;
		for (auto &column : target.columns) {
			const auto comparison =
			    join.conditions[pushdown_info->join_condition[column.join_filter_idx]].GetComparisonType();
			if (comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM &&
			    column.mode != JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION) {
				continue;
			}
			SimplifyJoinFilterCastChain(optimizer.GetContext(), column, comparison);
			get_columns.push_back(std::move(column));
		}
		if (get_columns.empty()) {
			continue;
		}
		auto &get = target.get;
		// pushdown info can be applied to this LogicalGet - push the dynamic table filter set
		if (!get.dynamic_filters) {
			get.dynamic_filters = make_shared_ptr<DynamicTableFilterSet>();
		}

		JoinFilterPushdownFilter get_filter;
		get_filter.dynamic_filters = get.dynamic_filters;
		get_filter.columns = std::move(get_columns);
		if (get.has_estimated_cardinality) {
			get_filter.estimated_scan_cardinality = get.estimated_cardinality;
		}
		pushdown_info->probe_info.push_back(std::move(get_filter));
	}

	auto cte_entry = cte_filters.find(join);
	if (cte_entry != cte_filters.end()) {
		for (auto &filter : cte_entry->second) {
			for (auto &column : filter.columns) {
				auto condition_idx = column.join_filter_idx;
				auto entry = std::find(pushdown_info->join_condition.begin(), pushdown_info->join_condition.end(),
				                       condition_idx);
				D_ASSERT(entry != pushdown_info->join_condition.end());
				column.join_filter_idx = NumericCast<idx_t>(entry - pushdown_info->join_condition.begin());
				SimplifyJoinFilterCastChain(optimizer.GetContext(), column, ExpressionType::COMPARE_EQUAL);
			}
			pushdown_info->probe_info.push_back(std::move(filter));
		}
	}

	if (pushdown_info->probe_info.empty()) {
		// no table sources found in which we can push down filters
		return;
	}

	// set up the min/max aggregates for each of the filters
	auto &context = optimizer.GetContext();
	for (auto &join_condition : pushdown_info->join_condition) {
		for (const auto &aggr_name : {MinFun::Name, MaxFun::Name}) {
			FunctionBinder function_binder(context);
			vector<unique_ptr<Expression>> aggr_children;
			aggr_children.push_back(join.conditions[join_condition].GetRHS().Copy());
			// the one-argument overload of the min/max set is the plain aggregate, min(x, n) takes two
			auto aggr = GetBuiltinAggregateFunction(context, aggr_name, {aggr_children[0]->GetReturnType()});
			auto aggr_expr = function_binder.BindAggregateFunction(aggr, std::move(aggr_children), nullptr,
			                                                       AggregateType::NON_DISTINCT);
			if (aggr_expr->GetChildren().size() != 1) {
				// min/max with collation - not supported
				return;
			}
			pushdown_info->min_max_aggregates.push_back(std::move(aggr_expr));
		}
	}
	const idx_t child_idx = join.children[1]->type == LogicalOperatorType::LOGICAL_DELIM_GET ? 0 : 1;
	pushdown_info->build_side_has_filter = IsFiltering(join.children[child_idx]);

	// set up the filter pushdown in the join itself
	join.filter_pushdown = std::move(pushdown_info);
}

void JoinFilterPushdownOptimizer::VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		// Runtime filters disable build-side reuse across recursive iterations.
		VisitOperator(*op.children[0]);
		return;
	}
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		// comparison join - try to generate join filters (if possible)
		GenerateJoinFilters(op.Cast<LogicalComparisonJoin>());
	}
	LogicalOperatorVisitor::VisitOperator(op);
}

} // namespace duckdb
