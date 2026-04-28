#include "duckdb/optimizer/scalar_aggregate_fusion.hpp"

#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

struct BindingReplacement {
	ColumnBinding old_binding;
	ColumnBinding new_binding;
	LogicalType new_type;
	bool replace_type = false;
};

struct BranchInfo {
	LogicalAggregate *aggregate = nullptr;
	LogicalFilter *filter = nullptr;
	vector<LogicalGet *> source_gets;
	vector<BindingReplacement> replacements_to_primary;
	vector<unique_ptr<Expression>> canonical_predicates;
	vector<bool> common_predicates;
};

static bool IsPassthroughProjection(const LogicalProjection &projection);

static bool IsCrossProduct(const LogicalOperator &op) {
	return op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT;
}

static void ReplaceExpressionBindings(unique_ptr<Expression> &expr, const vector<BindingReplacement> &replacements) {
	if (replacements.empty()) {
		return;
	}
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
		    for (idx_t iteration = 0; iteration < replacements.size(); iteration++) {
			    bool replaced = false;
			    for (auto &replacement : replacements) {
				    if (colref.binding == replacement.old_binding) {
					    if (colref.binding == replacement.new_binding) {
						    return;
					    }
					    colref.binding = replacement.new_binding;
					    if (replacement.replace_type) {
						    colref.return_type = replacement.new_type;
					    }
					    replaced = true;
					    break;
				    }
			    }
			    if (!replaced) {
				    return;
			    }
		    }
	    });
}

static void ReplaceOperatorExpressionBindings(LogicalOperator &op, const vector<BindingReplacement> &replacements) {
	if (replacements.empty()) {
		return;
	}
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *expr) { ReplaceExpressionBindings(*expr, replacements); });
}

static unique_ptr<Expression> MakeConjunction(ExpressionType type, vector<unique_ptr<Expression>> expressions) {
	D_ASSERT(type == ExpressionType::CONJUNCTION_AND || type == ExpressionType::CONJUNCTION_OR);
	if (expressions.empty()) {
		return nullptr;
	}
	if (expressions.size() == 1) {
		return std::move(expressions[0]);
	}
	auto result = make_uniq<BoundConjunctionExpression>(type);
	result->children = std::move(expressions);
	return std::move(result);
}

static void PushUniqueExpression(vector<unique_ptr<Expression>> &expressions, unique_ptr<Expression> candidate) {
	for (auto &expr : expressions) {
		if (Expression::Equals(*expr, *candidate)) {
			return;
		}
	}
	expressions.push_back(std::move(candidate));
}

static bool FlattenCrossProduct(unique_ptr<LogicalOperator> &op,
                                vector<reference<unique_ptr<LogicalOperator>>> &leaves) {
	if (IsCrossProduct(*op)) {
		if (op->children.size() != 2) {
			return false;
		}
		return FlattenCrossProduct(op->children[0], leaves) && FlattenCrossProduct(op->children[1], leaves);
	}
	leaves.push_back(op);
	return true;
}

static bool ExtractSources(LogicalOperator &op, BranchInfo &branch) {
	if (IsCrossProduct(op)) {
		if (op.children.size() != 2) {
			return false;
		}
		return ExtractSources(*op.children[0], branch) && ExtractSources(*op.children[1], branch);
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		if (!IsPassthroughProjection(projection)) {
			return false;
		}
		auto projection_bindings = projection.GetColumnBindings();
		auto child_bindings = projection.children[0]->GetColumnBindings();
		D_ASSERT(projection_bindings.size() == child_bindings.size());
		for (idx_t binding_idx = 0; binding_idx < projection_bindings.size(); binding_idx++) {
			if (projection_bindings[binding_idx] == child_bindings[binding_idx]) {
				continue;
			}
			auto &expression = projection.expressions[binding_idx];
			branch.replacements_to_primary.push_back(
			    {projection_bindings[binding_idx], child_bindings[binding_idx], expression->return_type, true});
		}
		return ExtractSources(*projection.children[0], branch);
	}
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = op.Cast<LogicalGet>();
	if (!get.children.empty() || get.table_filters.HasFilters() || get.dynamic_filters) {
		return false;
	}
	if (get.ordinality_idx.IsValid() || get.row_group_order_options) {
		return false;
	}
	if (!get.projection_ids.empty() || !get.projected_input.empty()) {
		return false;
	}
	branch.source_gets.push_back(&get);
	return true;
}

static unique_ptr<LogicalOperator> StripPassthroughProjections(unique_ptr<LogicalOperator> op) {
	if (IsCrossProduct(*op)) {
		for (auto &child : op->children) {
			child = StripPassthroughProjections(std::move(child));
		}
		op->ResolveOperatorTypes();
		return op;
	}
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op->Cast<LogicalProjection>();
		if (IsPassthroughProjection(projection)) {
			return StripPassthroughProjections(std::move(projection.children[0]));
		}
	}
	return op;
}

static ProjectionIndex FindOrAddColumn(LogicalGet &get, const ColumnIndex &column_index) {
	auto &column_ids = get.GetMutableColumnIds();
	for (idx_t column_idx = 0; column_idx < column_ids.size(); column_idx++) {
		if (column_ids[column_idx] == column_index) {
			return ProjectionIndex(column_idx);
		}
	}
	auto result = ProjectionIndex(column_ids.size());
	column_ids.push_back(column_index);
	return result;
}

static bool SupportsSourceFusion(const LogicalGet &get) {
	// Repeated native table scans can be faster than filtered aggregates once data
	// is hot. Keep this pass scoped to serializable table functions for now.
	if (get.GetTable() || !get.SupportSerialization()) {
		return false;
	}
	return StringUtil::CIEquals(get.function.name, "read_parquet") ||
	       StringUtil::CIEquals(get.function.name, "parquet_scan");
}

static bool GetSourceSignature(LogicalGet &get, string &signature) {
	if (!SupportsSourceFusion(get)) {
		return false;
	}

	auto table_index = get.table_index;
	auto table_filters = std::move(get.table_filters);
	auto column_ids = std::move(get.GetMutableColumnIds());
	auto projection_ids = std::move(get.projection_ids);

	get.table_index = TableIndex(0);
	get.table_filters = TableFilterSet();
	get.GetMutableColumnIds().clear();
	for (idx_t col_idx = 0; col_idx < get.names.size(); col_idx++) {
		get.GetMutableColumnIds().push_back(ColumnIndex(col_idx));
	}
	for (const auto &entry : get.virtual_columns) {
		get.GetMutableColumnIds().push_back(ColumnIndex(entry.first));
	}
	get.projection_ids.clear();

	MemoryStream stream(DEFAULT_BLOCK_ALLOC_SIZE);
	BinarySerializer serializer(stream);
	const auto offset = stream.GetPosition();
	serializer.Begin();
	bool success = true;
	try {
		get.Serialize(serializer);
	} catch (std::exception &) {
		success = false;
	}
	serializer.End();
	if (success) {
		signature =
		    string(char_ptr_cast(stream.GetData() + offset), NumericCast<uint32_t>(stream.GetPosition() - offset));
	}

	get.table_index = table_index;
	get.table_filters = std::move(table_filters);
	get.GetMutableColumnIds() = std::move(column_ids);
	get.projection_ids = std::move(projection_ids);
	return success;
}

static bool SameGetSource(LogicalGet &left, LogicalGet &right) {
	string left_signature;
	string right_signature;
	if (!GetSourceSignature(left, left_signature) || !GetSourceSignature(right, right_signature)) {
		return false;
	}
	return left_signature == right_signature;
}

static bool IsSupportedAggregate(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		return false;
	}
	auto &aggregate = expr.Cast<BoundAggregateExpression>();
	if (aggregate.filter || aggregate.order_bys || aggregate.IsVolatile()) {
		return false;
	}
	for (auto &child : aggregate.children) {
		if (child->IsVolatile() || child->CanThrow()) {
			return false;
		}
	}

	auto &name = aggregate.function.name;
	if (name == "count_star") {
		return aggregate.children.empty() && !aggregate.IsDistinct();
	}
	if (name == "count") {
		return aggregate.children.size() <= 1 && !aggregate.IsDistinct();
	}
	if (aggregate.IsDistinct()) {
		return false;
	}
	if (name == "sum" || name == "avg" || name == "min" || name == "max") {
		return aggregate.children.size() == 1;
	}
	return false;
}

static bool IsPassthroughProjection(const LogicalProjection &projection) {
	if (projection.children.size() != 1) {
		return false;
	}
	auto child_bindings = projection.children[0]->GetColumnBindings();
	if (projection.expressions.size() != child_bindings.size()) {
		return false;
	}
	for (idx_t expr_idx = 0; expr_idx < projection.expressions.size(); expr_idx++) {
		auto &expr = *projection.expressions[expr_idx];
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		if (colref.depth != 0 || colref.binding != child_bindings[expr_idx]) {
			return false;
		}
	}
	return true;
}

static bool ExtractBranch(reference<unique_ptr<LogicalOperator>> branch_ref, BranchInfo &branch) {
	auto *branch_op = &*branch_ref.get();
	if (branch_op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = branch_op->Cast<LogicalProjection>();
		if (!IsPassthroughProjection(projection)) {
			return false;
		}
		branch_op = &*projection.children[0];
	}
	if (branch_op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || branch_op->children.size() != 1) {
		return false;
	}
	auto &aggregate = branch_op->Cast<LogicalAggregate>();
	if (!aggregate.groups.empty() || !aggregate.grouping_sets.empty() || !aggregate.grouping_functions.empty()) {
		return false;
	}
	if (aggregate.expressions.empty()) {
		return false;
	}
	for (auto &expr : aggregate.expressions) {
		if (!IsSupportedAggregate(*expr)) {
			return false;
		}
	}
	auto &aggregate_child = *aggregate.children[0];
	if (aggregate_child.type != LogicalOperatorType::LOGICAL_FILTER || aggregate_child.children.size() != 1) {
		return false;
	}
	auto &filter = aggregate_child.Cast<LogicalFilter>();
	if (filter.HasProjectionMap() || filter.expressions.empty()) {
		return false;
	}
	for (auto &expr : filter.expressions) {
		if (expr->IsVolatile() || expr->CanThrow()) {
			return false;
		}
	}
	branch.aggregate = &aggregate;
	branch.filter = &filter;
	branch.common_predicates.resize(filter.expressions.size(), false);
	return ExtractSources(*filter.children[0], branch);
}

static bool BuildPrimaryBindingMap(BranchInfo &primary, BranchInfo &branch) {
	if (primary.source_gets.size() != branch.source_gets.size()) {
		return false;
	}
	for (idx_t get_idx = 0; get_idx < primary.source_gets.size(); get_idx++) {
		auto &primary_get = *primary.source_gets[get_idx];
		auto &branch_get = *branch.source_gets[get_idx];
		if (!SameGetSource(primary_get, branch_get)) {
			return false;
		}
		auto branch_bindings = branch_get.GetColumnBindings();
		for (idx_t binding_idx = 0; binding_idx < branch_bindings.size(); binding_idx++) {
			auto &old_binding = branch_bindings[binding_idx];
			auto &branch_column = branch_get.GetColumnIndex(old_binding);
			auto new_projection_index = FindOrAddColumn(primary_get, branch_column);
			ColumnBinding new_binding(primary_get.table_index, new_projection_index);
			if (old_binding == new_binding) {
				continue;
			}
			auto type = primary_get.GetColumnType(branch_column);
			branch.replacements_to_primary.push_back({old_binding, new_binding, type, true});
		}
	}
	return true;
}

static unique_ptr<LogicalOperator> BuildCrossProduct(vector<unique_ptr<LogicalOperator>> leaves) {
	D_ASSERT(!leaves.empty());
	auto result = std::move(leaves[0]);
	for (idx_t leaf_idx = 1; leaf_idx < leaves.size(); leaf_idx++) {
		result = LogicalCrossProduct::Create(std::move(result), std::move(leaves[leaf_idx]));
	}
	return result;
}

static unique_ptr<Expression> CanonicalCopy(const Expression &expr, const vector<BindingReplacement> &replacements) {
	auto result = expr.Copy();
	ReplaceExpressionBindings(result, replacements);
	return result;
}

static bool PreparePredicates(vector<BranchInfo> &branches) {
	for (auto &branch : branches) {
		for (auto &predicate : branch.filter->expressions) {
			branch.canonical_predicates.push_back(CanonicalCopy(*predicate, branch.replacements_to_primary));
		}
	}
	auto &primary = branches[0];
	for (idx_t predicate_idx = 0; predicate_idx < primary.canonical_predicates.size(); predicate_idx++) {
		auto &candidate = *primary.canonical_predicates[predicate_idx];
		bool common = true;
		for (idx_t branch_idx = 1; branch_idx < branches.size(); branch_idx++) {
			bool found = false;
			for (auto &other_predicate : branches[branch_idx].canonical_predicates) {
				if (Expression::Equals(candidate, *other_predicate)) {
					found = true;
					break;
				}
			}
			if (!found) {
				common = false;
				break;
			}
		}
		if (!common) {
			continue;
		}
		for (auto &branch : branches) {
			for (idx_t other_idx = 0; other_idx < branch.canonical_predicates.size(); other_idx++) {
				if (Expression::Equals(candidate, *branch.canonical_predicates[other_idx])) {
					branch.common_predicates[other_idx] = true;
					break;
				}
			}
		}
	}
	return true;
}

static vector<unique_ptr<Expression>> GetBranchPredicates(const BranchInfo &branch) {
	vector<unique_ptr<Expression>> result;
	for (idx_t predicate_idx = 0; predicate_idx < branch.canonical_predicates.size(); predicate_idx++) {
		if (!branch.common_predicates[predicate_idx]) {
			result.push_back(branch.canonical_predicates[predicate_idx]->Copy());
		}
	}
	return result;
}

static unique_ptr<LogicalOperator> CreateFusedAggregate(Optimizer &optimizer, vector<BranchInfo> &branches) {
	if (!PreparePredicates(branches)) {
		return nullptr;
	}

	vector<unique_ptr<Expression>> common_predicates;
	for (idx_t predicate_idx = 0; predicate_idx < branches[0].canonical_predicates.size(); predicate_idx++) {
		if (branches[0].common_predicates[predicate_idx]) {
			common_predicates.push_back(branches[0].canonical_predicates[predicate_idx]->Copy());
		}
	}
	if (branches[0].source_gets.size() > 1 && common_predicates.empty()) {
		return nullptr;
	}

	vector<unique_ptr<Expression>> branch_or_terms;
	vector<vector<unique_ptr<Expression>>> branch_predicates;
	bool add_or_guard = true;
	branch_predicates.reserve(branches.size());
	for (auto &branch : branches) {
		auto predicates = GetBranchPredicates(branch);
		if (predicates.empty()) {
			// This branch has no branch-local predicate. A filtered aggregate would
			// be unfiltered and an OR guard would be redundant.
			add_or_guard = false;
		}
		if (add_or_guard) {
			vector<unique_ptr<Expression>> or_term_children;
			for (auto &predicate : predicates) {
				or_term_children.push_back(predicate->Copy());
			}
			PushUniqueExpression(branch_or_terms,
			                     MakeConjunction(ExpressionType::CONJUNCTION_AND, std::move(or_term_children)));
		}
		branch_predicates.push_back(std::move(predicates));
	}
	if (add_or_guard && !branch_or_terms.empty()) {
		common_predicates.push_back(MakeConjunction(ExpressionType::CONJUNCTION_OR, std::move(branch_or_terms)));
	}

	vector<unique_ptr<Expression>> fused_aggregates;
	for (idx_t branch_idx = 0; branch_idx < branches.size(); branch_idx++) {
		auto &branch = branches[branch_idx];
		for (auto &aggregate_expr : branch.aggregate->expressions) {
			auto aggregate_copy = CanonicalCopy(*aggregate_expr, branch.replacements_to_primary);
			auto &bound_aggregate = aggregate_copy->Cast<BoundAggregateExpression>();
			vector<unique_ptr<Expression>> filter_terms;
			for (auto &predicate : branch_predicates[branch_idx]) {
				filter_terms.push_back(predicate->Copy());
			}
			bound_aggregate.filter = MakeConjunction(ExpressionType::CONJUNCTION_AND, std::move(filter_terms));
			fused_aggregates.push_back(std::move(aggregate_copy));
		}
	}

	auto fused_aggregate = make_uniq<LogicalAggregate>(
	    optimizer.binder.GenerateTableIndex(), optimizer.binder.GenerateTableIndex(), std::move(fused_aggregates));
	fused_aggregate->SetEstimatedCardinality(1);
	auto fused_child = StripPassthroughProjections(std::move(branches[0].filter->children[0]));
	if (!common_predicates.empty()) {
		auto fused_filter = make_uniq<LogicalFilter>();
		fused_filter->expressions = std::move(common_predicates);
		fused_filter->children.push_back(std::move(fused_child));
		fused_child = std::move(fused_filter);
	}
	fused_aggregate->children.push_back(std::move(fused_child));
	fused_aggregate->ResolveOperatorTypes();
	return std::move(fused_aggregate);
}

static unique_ptr<LogicalOperator> TryFuseCrossProduct(Optimizer &optimizer, unique_ptr<LogicalOperator> op) {
	if (!IsCrossProduct(*op)) {
		return op;
	}

	vector<reference<unique_ptr<LogicalOperator>>> leaves;
	if (!FlattenCrossProduct(op, leaves) || leaves.size() < 2) {
		return op;
	}

	for (idx_t start = 0; start + 1 < leaves.size(); start++) {
		vector<BranchInfo> branches;
		branches.emplace_back();
		if (!ExtractBranch(leaves[start], branches[0])) {
			continue;
		}

		idx_t end = start + 1;
		for (; end < leaves.size(); end++) {
			BranchInfo branch;
			if (!ExtractBranch(leaves[end], branch)) {
				break;
			}
			if (!BuildPrimaryBindingMap(branches[0], branch)) {
				break;
			}
			branches.push_back(std::move(branch));
		}
		if (branches.size() < 2) {
			continue;
		}

		auto fused_aggregate = CreateFusedAggregate(optimizer, branches);
		if (!fused_aggregate) {
			continue;
		}

		vector<unique_ptr<LogicalOperator>> new_leaves;
		new_leaves.reserve(leaves.size() - branches.size() + 1);
		for (idx_t leaf_idx = 0; leaf_idx < leaves.size(); leaf_idx++) {
			if (leaf_idx == start) {
				new_leaves.push_back(std::move(fused_aggregate));
				leaf_idx = end - 1;
				continue;
			}
			new_leaves.push_back(std::move(leaves[leaf_idx].get()));
		}
		return BuildCrossProduct(std::move(new_leaves));
	}
	return op;
}

static unique_ptr<LogicalOperator> OptimizeRecursive(Optimizer &optimizer, unique_ptr<LogicalOperator> op) {
	if (IsCrossProduct(*op)) {
		auto original = op.get();
		op = TryFuseCrossProduct(optimizer, std::move(op));
		if (op.get() != original) {
			return OptimizeRecursive(optimizer, std::move(op));
		}
	}

	vector<BindingReplacement> replacements;
	for (auto &child : op->children) {
		auto old_bindings = child->GetColumnBindings();
		auto old_types = child->types;
		child = OptimizeRecursive(optimizer, std::move(child));
		auto new_bindings = child->GetColumnBindings();
		if (old_bindings == new_bindings) {
			continue;
		}
		if (old_bindings.size() != new_bindings.size()) {
			throw InternalException("Scalar aggregate fusion changed a child binding count unexpectedly");
		}
		for (idx_t binding_idx = 0; binding_idx < old_bindings.size(); binding_idx++) {
			auto replacement_type = binding_idx < old_types.size() ? old_types[binding_idx] : LogicalType();
			replacements.push_back({old_bindings[binding_idx], new_bindings[binding_idx], replacement_type, false});
		}
	}
	ReplaceOperatorExpressionBindings(*op, replacements);
	return TryFuseCrossProduct(optimizer, std::move(op));
}

} // namespace

ScalarAggregateFusion::ScalarAggregateFusion(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

unique_ptr<LogicalOperator> ScalarAggregateFusion::Optimize(unique_ptr<LogicalOperator> op) {
	return OptimizeRecursive(optimizer, std::move(op));
}

} // namespace duckdb
