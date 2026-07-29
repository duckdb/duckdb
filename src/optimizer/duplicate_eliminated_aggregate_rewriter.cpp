//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_aggregate_rewriter.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_aggregate_rewriter.hpp"

#include "duckdb/optimizer/duplicate_eliminated_domain_candidate.hpp"
#include "duckdb/optimizer/late_materialization_helper.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>

namespace duckdb {

static bool GetDomainBinding(const Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &column = expr.Cast<BoundColumnRefExpression>();
	if (column.Depth() != 0) {
		return false;
	}
	binding = column.Binding();
	return true;
}

static bool IsDomainJoinEquivalenceComparison(const JoinCondition &condition) {
	if (!condition.IsComparison()) {
		return false;
	}
	auto comparison = condition.GetComparisonType();
	return comparison == ExpressionType::COMPARE_EQUAL || comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

struct TrackedBinding {
	TrackedBinding(ColumnBinding binding_p, LogicalType type_p) : binding(binding_p), type(std::move(type_p)) {
	}

	ColumnBinding binding;
	LogicalType type;
};

struct AggregateDomainPreservation {
	AggregateDomainPreservation(unique_ptr<LogicalOperator> &join_location_p, LogicalComparisonJoin &join_p,
	                            LogicalCTERef &domain_ref_p)
	    : join_location(join_location_p), join(join_p), domain_ref(domain_ref_p) {
	}

	reference<unique_ptr<LogicalOperator>> join_location;
	reference<LogicalComparisonJoin> join;
	reference<LogicalCTERef> domain_ref;
};

struct AggregateDomainUse {
	AggregateDomainUse(LogicalAggregate &aggregate_p, LogicalComparisonJoin &domain_join_p, LogicalCTERef &domain_ref_p,
	                   idx_t domain_child_p, vector<reference<LogicalOperator>> above_aggregate_p,
	                   vector<reference<LogicalOperator>> below_aggregate_p,
	                   unique_ptr<AggregateDomainPreservation> preservation_p)
	    : aggregate(aggregate_p), domain_join(domain_join_p), domain_ref(domain_ref_p), domain_child(domain_child_p),
	      above_aggregate(std::move(above_aggregate_p)), below_aggregate(std::move(below_aggregate_p)),
	      preservation(std::move(preservation_p)) {
	}

	reference<LogicalAggregate> aggregate;
	reference<LogicalComparisonJoin> domain_join;
	reference<LogicalCTERef> domain_ref;
	idx_t domain_child;
	vector<reference<LogicalOperator>> above_aggregate;
	vector<reference<LogicalOperator>> below_aggregate;
	unique_ptr<AggregateDomainPreservation> preservation;
};

static idx_t CountDomainCTERefs(LogicalOperator &op, TableIndex cte_index) {
	idx_t count = 0;
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF && op.Cast<LogicalCTERef>().cte_index == cte_index) {
		count++;
	}
	for (auto &child : op.children) {
		count += CountDomainCTERefs(*child, cte_index);
	}
	return count;
}

static bool IsDirectPathOperator(LogicalOperator &op) {
	if (op.children.size() != 1) {
		return false;
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		return true;
	}
	return op.type == LogicalOperatorType::LOGICAL_FILTER && !op.HasProjectionMap();
}

static bool IsGroupingOnlyAggregate(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || op.children.size() != 1) {
		return false;
	}
	auto &aggregate = op.Cast<LogicalAggregate>();
	return !aggregate.groups.empty() && aggregate.expressions.empty() && aggregate.grouping_functions.empty() &&
	       aggregate.grouping_sets.empty();
}

static unique_ptr<AggregateDomainUse> FindDomainJoin(unique_ptr<LogicalOperator> &op, LogicalAggregate &aggregate,
                                                     TableIndex cte_index,
                                                     const vector<reference<LogicalOperator>> &above_aggregate,
                                                     vector<reference<LogicalOperator>> below_aggregate,
                                                     unique_ptr<AggregateDomainPreservation> preservation) {
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op->children.size() == 2) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if ((join.join_type != JoinType::INNER && join.join_type != JoinType::SEMI) || join.HasProjectionMap()) {
			return nullptr;
		}
		optional_idx domain_child;
		for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {
			auto &child = *op->children[child_idx];
			if (child.type == LogicalOperatorType::LOGICAL_CTE_REF &&
			    child.Cast<LogicalCTERef>().cte_index == cte_index) {
				if (domain_child.IsValid()) {
					return nullptr;
				}
				domain_child = child_idx;
			}
		}
		if (!domain_child.IsValid()) {
			return nullptr;
		}
		if (join.join_type == JoinType::SEMI && domain_child.GetIndex() != 1) {
			return nullptr;
		}
		auto &cteref = op->children[domain_child.GetIndex()]->Cast<LogicalCTERef>();
		return make_uniq<AggregateDomainUse>(aggregate, join, cteref, domain_child.GetIndex(), above_aggregate,
		                                     std::move(below_aggregate), std::move(preservation));
	}
	if (!IsDirectPathOperator(*op) && !IsGroupingOnlyAggregate(*op)) {
		return nullptr;
	}
	below_aggregate.push_back(*op);
	return FindDomainJoin(op->children[0], aggregate, cte_index, above_aggregate, std::move(below_aggregate),
	                      std::move(preservation));
}

static unique_ptr<AggregateDomainUse>
FindAggregateDomainUse(unique_ptr<LogicalOperator> &op, TableIndex cte_index,
                       vector<reference<LogicalOperator>> above_aggregate = {},
                       unique_ptr<AggregateDomainPreservation> preservation = nullptr) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && op->children.size() == 1) {
		auto &aggregate = op->Cast<LogicalAggregate>();
		if (aggregate.groups.empty() || aggregate.expressions.empty() || !aggregate.grouping_functions.empty() ||
		    !aggregate.grouping_sets.empty()) {
			return nullptr;
		}
		for (auto &expression : aggregate.expressions) {
			if (expression->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				return nullptr;
			}
		}
		return FindDomainJoin(op->children[0], aggregate, cte_index, above_aggregate, {}, std::move(preservation));
	}
	if (!preservation && op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op->children.size() == 2) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::LEFT && !join.HasProjectionMap()) {
			optional_idx domain_child;
			for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {
				auto &child = *op->children[child_idx];
				if (child.type == LogicalOperatorType::LOGICAL_CTE_REF &&
				    child.Cast<LogicalCTERef>().cte_index == cte_index) {
					if (domain_child.IsValid()) {
						return nullptr;
					}
					domain_child = child_idx;
				}
			}
			if (domain_child.IsValid() && domain_child.GetIndex() == 0) {
				auto domain_idx = domain_child.GetIndex();
				auto preserved_domain =
				    make_uniq<AggregateDomainPreservation>(op, join, op->children[domain_idx]->Cast<LogicalCTERef>());
				return FindAggregateDomainUse(op->children[1 - domain_idx], cte_index, std::move(above_aggregate),
				                              std::move(preserved_domain));
			}
		}
	}
	if (!IsDirectPathOperator(*op)) {
		return nullptr;
	}
	above_aggregate.push_back(*op);
	return FindAggregateDomainUse(op->children[0], cte_index, std::move(above_aggregate), std::move(preservation));
}

static bool TraceBindingThroughProjection(LogicalProjection &projection, ColumnBinding &binding) {
	auto output_bindings = projection.GetColumnBindings();
	auto binding_idx = std::find(output_bindings.begin(), output_bindings.end(), binding);
	if (binding_idx == output_bindings.end()) {
		return false;
	}
	auto expression_idx = NumericCast<idx_t>(binding_idx - output_bindings.begin());
	return GetDomainBinding(*projection.expressions[expression_idx], binding);
}

static bool TraceBindingThroughAggregate(LogicalAggregate &aggregate, ColumnBinding &binding) {
	auto output_bindings = aggregate.GetColumnBindings();
	auto binding_idx = std::find(output_bindings.begin(), output_bindings.end(), binding);
	if (binding_idx == output_bindings.end()) {
		return false;
	}
	auto group_idx = NumericCast<idx_t>(binding_idx - output_bindings.begin());
	return group_idx < aggregate.groups.size() && GetDomainBinding(*aggregate.groups[group_idx], binding);
}

static bool AggregateGroupsMatchDomain(const AggregateDomainUse &use) {
	auto &aggregate = use.aggregate.get();
	auto domain_bindings = use.domain_ref.get().GetColumnBindings();
	if (aggregate.groups.size() != domain_bindings.size()) {
		return false;
	}
	column_binding_set_t group_candidates(domain_bindings.begin(), domain_bindings.end());
	for (auto &domain_binding : domain_bindings) {
		bool found_counterpart = false;
		for (auto &condition : use.domain_join.get().conditions) {
			if (!IsDomainJoinEquivalenceComparison(condition)) {
				continue;
			}
			ColumnBinding left;
			ColumnBinding right;
			if (!GetDomainBinding(condition.GetLHS(), left) || !GetDomainBinding(condition.GetRHS(), right)) {
				continue;
			}
			if (left == domain_binding) {
				group_candidates.insert(right);
				found_counterpart = true;
			} else if (right == domain_binding) {
				group_candidates.insert(left);
				found_counterpart = true;
			}
		}
		if (!found_counterpart) {
			return false;
		}
	}
	column_binding_set_t covered;
	for (auto &group : aggregate.groups) {
		ColumnBinding binding;
		if (!GetDomainBinding(*group, binding)) {
			return false;
		}
		for (auto &path_op : use.below_aggregate) {
			auto &op = path_op.get();
			if (op.type == LogicalOperatorType::LOGICAL_PROJECTION &&
			    !TraceBindingThroughProjection(op.Cast<LogicalProjection>(), binding)) {
				return false;
			}
			if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY &&
			    !TraceBindingThroughAggregate(op.Cast<LogicalAggregate>(), binding)) {
				return false;
			}
		}
		if (group_candidates.find(binding) == group_candidates.end() || !covered.insert(binding).second) {
			return false;
		}
	}
	return covered.size() == aggregate.groups.size();
}

static bool TraceCandidateBindingToGet(LogicalOperator &op, ColumnBinding binding, optional_ptr<LogicalGet> &get,
                                       optional_idx &get_column) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto bindings = op.GetColumnBindings();
		auto entry = std::find(bindings.begin(), bindings.end(), binding);
		if (entry == bindings.end()) {
			return false;
		}
		get = op.Cast<LogicalGet>();
		get_column = NumericCast<idx_t>(entry - bindings.begin());
		return true;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
		if (op.children.size() != 1 || op.HasProjectionMap()) {
			return false;
		}
		return TraceCandidateBindingToGet(*op.children[0], binding, get, get_column);
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (op.children.size() != 1) {
			return false;
		}
		auto &projection = op.Cast<LogicalProjection>();
		if (!TraceBindingThroughProjection(projection, binding)) {
			return false;
		}
		return TraceCandidateBindingToGet(*op.children[0], binding, get, get_column);
	}
	default:
		return false;
	}
}

struct ScanIdentityDescriptor {
	ScanIdentityDescriptor(LogicalGet &get_p, vector<column_t> column_ids_p, vector<TableColumn> columns_p)
	    : get(get_p), column_ids(std::move(column_ids_p)), columns(std::move(columns_p)) {
	}

	reference<LogicalGet> get;
	vector<column_t> column_ids;
	vector<TableColumn> columns;
};

static unique_ptr<ScanIdentityDescriptor> FindEligibleScanIdentity(ClientContext &context,
                                                                   DuplicateEliminatedDomainCandidate &candidate) {
	auto source_bindings = candidate.source.get()->GetColumnBindings();
	optional_ptr<LogicalGet> candidate_get;
	vector<idx_t> get_columns;
	get_columns.reserve(candidate.key_indices.size());
	for (auto key_idx : candidate.key_indices) {
		if (key_idx >= source_bindings.size()) {
			return nullptr;
		}
		optional_ptr<LogicalGet> get;
		optional_idx get_column;
		if (!TraceCandidateBindingToGet(*candidate.source.get(), source_bindings[key_idx], get, get_column)) {
			return nullptr;
		}
		if (!candidate_get) {
			candidate_get = get;
		} else if (candidate_get.get() != get.get()) {
			return nullptr;
		}
		get_columns.push_back(get_column.GetIndex());
	}
	if (!candidate_get || !candidate_get->function.get_row_id_columns || !candidate_get->bind_data) {
		return nullptr;
	}
	auto row_id_columns = candidate_get->function.get_row_id_columns(context, candidate_get->bind_data.get());
	if (row_id_columns.empty()) {
		return nullptr;
	}
	vector<TableColumn> identity_columns;
	identity_columns.reserve(row_id_columns.size());
	for (auto column_id : row_id_columns) {
		auto entry = candidate_get->virtual_columns.find(column_id);
		if (entry == candidate_get->virtual_columns.end()) {
			return nullptr;
		}
		identity_columns.push_back(entry->second);
	}

	auto stats = RelationStatisticsHelper::ExtractGetStats(*candidate_get, context);
	if (stats.cardinality == 0) {
		return nullptr;
	}
	for (auto column_idx : get_columns) {
		if (column_idx < stats.column_distinct_count.size() &&
		    stats.column_distinct_count[column_idx].distinct_count >= stats.cardinality) {
			return make_uniq<ScanIdentityDescriptor>(*candidate_get, std::move(row_id_columns),
			                                         std::move(identity_columns));
		}
	}
	return nullptr;
}

static bool AddScanIdentity(LogicalOperator &op, const ScanIdentityDescriptor &identity,
                            vector<TrackedBinding> &identities) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (&get != &identity.get.get()) {
			return false;
		}
		auto row_id_indices = LateMaterializationHelper::GetOrInsertRowIds(get, identity.column_ids, identity.columns);
		auto bindings = get.GetColumnBindings();
		for (auto row_id_index : row_id_indices) {
			identities.emplace_back(bindings[row_id_index.GetIndex()], get.types[row_id_index.GetIndex()]);
		}
		return !identities.empty();
	}
	case LogicalOperatorType::LOGICAL_FILTER:
		if (op.children.size() != 1 || op.HasProjectionMap() ||
		    !AddScanIdentity(*op.children[0], identity, identities)) {
			return false;
		}
		op.ResolveOperatorTypes();
		return true;
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (op.children.size() != 1 || !AddScanIdentity(*op.children[0], identity, identities)) {
			return false;
		}
		auto &projection = op.Cast<LogicalProjection>();
		vector<TrackedBinding> projected;
		projected.reserve(identities.size());
		for (auto &identity : identities) {
			auto output_idx = ColumnBinding::PushExpression(
			    projection.expressions, make_uniq<BoundColumnRefExpression>(identity.type, identity.binding));
			projected.emplace_back(ColumnBinding(projection.table_index, output_idx), identity.type);
		}
		identities = std::move(projected);
		op.ResolveOperatorTypes();
		return true;
	}
	default:
		return false;
	}
}

using operator_location_path_t = vector<reference<unique_ptr<LogicalOperator>>>;

static bool FindOperatorPath(unique_ptr<LogicalOperator> &op, const LogicalOperator &target,
                             operator_location_path_t &path) {
	path.push_back(op);
	if (op.get() == &target) {
		return true;
	}
	for (auto &child : op->children) {
		if (FindOperatorPath(child, target, path)) {
			return true;
		}
	}
	path.pop_back();
	return false;
}

static bool IsDirectScanSource(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return true;
	}
	if ((op.type != LogicalOperatorType::LOGICAL_FILTER && op.type != LogicalOperatorType::LOGICAL_PROJECTION) ||
	    op.children.size() != 1) {
		return false;
	}
	return IsDirectScanSource(*op.children[0]);
}

static bool FindDirectSourcePath(unique_ptr<LogicalOperator> &root, const LogicalOperator &target,
                                 operator_location_path_t &path) {
	if (!FindOperatorPath(root, target, path)) {
		return false;
	}
	for (idx_t path_idx = 0; path_idx < path.size(); path_idx++) {
		if (IsDirectScanSource(*path[path_idx].get())) {
			path.erase(path.begin() + NumericCast<operator_location_path_t::difference_type>(path_idx + 1), path.end());
			return true;
		}
	}
	path.clear();
	return false;
}

static bool ExpressionReferencesBindings(const Expression &expression, const column_binding_set_t &bindings) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expression, [&](const BoundColumnRefExpression &column) {
		    if (column.Depth() == 0 && bindings.find(column.Binding()) != bindings.end()) {
			    found = true;
		    }
	    });
	return found;
}

static bool ConditionReferencesBindings(const JoinCondition &condition, const column_binding_set_t &bindings) {
	if (!condition.IsComparison()) {
		return ExpressionReferencesBindings(condition.GetJoinExpression(), bindings);
	}
	return ExpressionReferencesBindings(condition.GetLHS(), bindings) ||
	       ExpressionReferencesBindings(condition.GetRHS(), bindings);
}

static bool CanDetachCandidate(const operator_location_path_t &path, const column_binding_set_t &source_bindings) {
	if (path.empty()) {
		return false;
	}
	bool has_boundary_condition = false;
	for (idx_t path_idx = 0; path_idx + 1 < path.size(); path_idx++) {
		auto &op = *path[path_idx].get();
		if (op.children.size() != 2) {
			return false;
		}
		auto &next = path[path_idx + 1].get();
		if (op.children[0].get() != next.get() && op.children[1].get() != next.get()) {
			return false;
		}
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = op.Cast<LogicalComparisonJoin>();
			if (join.join_type != JoinType::INNER || join.HasProjectionMap()) {
				return false;
			}
			for (auto &condition : join.conditions) {
				has_boundary_condition =
				    has_boundary_condition || ConditionReferencesBindings(condition, source_bindings);
			}
		} else if (op.type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			return false;
		}
	}
	return has_boundary_condition;
}

static void DetachCandidate(const operator_location_path_t &path, idx_t path_idx,
                            const column_binding_set_t &source_bindings, unique_ptr<LogicalOperator> &source,
                            vector<JoinCondition> &boundary_conditions) {
	auto &op = path[path_idx].get();
	if (path_idx + 1 == path.size()) {
		source = std::move(op);
		return;
	}
	auto &next = path[path_idx + 1].get();
	idx_t child_idx = op->children[0].get() == next.get() ? 0 : 1;
	DetachCandidate(path, path_idx + 1, source_bindings, source, boundary_conditions);
	auto other_idx = 1 - child_idx;
	if (!op->children[child_idx]) {
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = op->Cast<LogicalComparisonJoin>();
			for (auto &condition : join.conditions) {
				boundary_conditions.push_back(std::move(condition));
			}
		}
		op = std::move(op->children[other_idx]);
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		vector<JoinCondition> retained;
		for (auto &condition : join.conditions) {
			if (ConditionReferencesBindings(condition, source_bindings)) {
				boundary_conditions.push_back(std::move(condition));
			} else {
				retained.push_back(std::move(condition));
			}
		}
		join.conditions = std::move(retained);
		if (join.conditions.empty()) {
			auto cross_product = make_uniq<LogicalCrossProduct>(std::move(op->children[0]), std::move(op->children[1]));
			op = std::move(cross_product);
		}
	}
}

static bool GetAlignmentConditions(LogicalComparisonJoin &join, vector<idx_t> &alignment_condition_indices) {
	auto rhs_bindings = join.children[1]->GetColumnBindings();
	unordered_set<idx_t> used_conditions;
	for (auto &key : join.duplicate_eliminated_columns) {
		ColumnBinding key_binding;
		if (!GetDomainBinding(*key, key_binding)) {
			return false;
		}
		optional_idx match;
		for (idx_t condition_idx = 0; condition_idx < join.conditions.size(); condition_idx++) {
			if (used_conditions.find(condition_idx) != used_conditions.end()) {
				continue;
			}
			auto &condition = join.conditions[condition_idx];
			if (!IsDomainJoinEquivalenceComparison(condition)) {
				continue;
			}
			ColumnBinding left;
			ColumnBinding right;
			if (!GetDomainBinding(condition.GetLHS(), left) || !GetDomainBinding(condition.GetRHS(), right)) {
				continue;
			}
			bool left_matches =
			    left == key_binding && std::find(rhs_bindings.begin(), rhs_bindings.end(), right) != rhs_bindings.end();
			bool right_matches =
			    right == key_binding && std::find(rhs_bindings.begin(), rhs_bindings.end(), left) != rhs_bindings.end();
			if (left_matches || right_matches) {
				match = condition_idx;
				break;
			}
		}
		if (!match.IsValid()) {
			return false;
		}
		used_conditions.insert(match.GetIndex());
		alignment_condition_indices.push_back(match.GetIndex());
	}
	return true;
}

static void PropagateBindings(vector<reference<LogicalOperator>> &path, vector<TrackedBinding> &bindings) {
	for (auto entry = path.rbegin(); entry != path.rend(); entry++) {
		auto &op = entry->get();
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			auto &aggregate = op.Cast<LogicalAggregate>();
			vector<TrackedBinding> grouped;
			grouped.reserve(bindings.size());
			for (auto &binding : bindings) {
				auto output_idx = ColumnBinding::PushExpression(
				    aggregate.groups, make_uniq<BoundColumnRefExpression>(binding.type, binding.binding));
				grouped.emplace_back(ColumnBinding(aggregate.group_index, output_idx), binding.type);
			}
			bindings = std::move(grouped);
			op.ResolveOperatorTypes();
			continue;
		}
		if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
			op.ResolveOperatorTypes();
			continue;
		}
		auto &projection = op.Cast<LogicalProjection>();
		vector<TrackedBinding> projected;
		projected.reserve(bindings.size());
		for (auto &binding : bindings) {
			auto output_idx = ColumnBinding::PushExpression(
			    projection.expressions, make_uniq<BoundColumnRefExpression>(binding.type, binding.binding));
			projected.emplace_back(ColumnBinding(projection.table_index, output_idx), binding.type);
		}
		bindings = std::move(projected);
		op.ResolveOperatorTypes();
	}
}

static TrackedBinding AddRealRowMarker(Binder &binder, unique_ptr<LogicalOperator> &rhs_root, AggregateDomainUse &use) {
	auto &domain_join = use.domain_join.get();
	auto old_bindings = domain_join.children[0]->GetColumnBindings();
	auto old_types = domain_join.children[0]->types;
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(old_bindings.size() + 1);
	for (idx_t binding_idx = 0; binding_idx < old_bindings.size(); binding_idx++) {
		expressions.push_back(make_uniq<BoundColumnRefExpression>(old_types[binding_idx], old_bindings[binding_idx]));
	}
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
	auto projection_index = binder.GenerateTableIndex();
	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(expressions));
	auto original_child = std::move(domain_join.children[0]);
	auto stop_operator = projection.get();
	projection->children.push_back(std::move(original_child));
	projection->ResolveOperatorTypes();
	auto new_bindings = projection->GetColumnBindings();
	BindingReplacementGraph replacements;
	for (idx_t binding_idx = 0; binding_idx < old_bindings.size(); binding_idx++) {
		replacements.Add(old_bindings[binding_idx], new_bindings[binding_idx]);
	}
	domain_join.children[0] = std::move(projection);

	CorrelatedColumnBindingReplacer replacer;
	replacements.AddTo(replacer);
	replacer.stop_operator = *stop_operator;
	replacer.VisitOperator(*rhs_root);
	return TrackedBinding(new_bindings.back(), LogicalType::BOOLEAN);
}

static void AddAggregateFilter(LogicalAggregate &aggregate, const TrackedBinding &marker) {
	for (auto &expression : aggregate.expressions) {
		auto &bound_aggregate = expression->Cast<BoundAggregateExpression>();
		auto marker_ref = make_uniq<BoundColumnRefExpression>(marker.type, marker.binding);
		if (bound_aggregate.GetFilter()) {
			bound_aggregate.GetFilterMutable() = make_uniq<BoundConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(bound_aggregate.GetFilterMutable()), std::move(marker_ref));
		} else {
			bound_aggregate.GetFilterMutable() = std::move(marker_ref);
		}
	}
}

static bool GetAggregateDomainPreservationReplacements(AggregateDomainUse &use, BindingReplacementGraph &replacements) {
	if (!use.preservation) {
		return true;
	}
	auto &preservation = *use.preservation;
	auto &join = preservation.join.get();
	if (join.join_type != JoinType::LEFT || join.HasProjectionMap() ||
	    join.conditions.size() != preservation.domain_ref.get().chunk_types.size()) {
		return false;
	}

	auto domain_bindings = preservation.domain_ref.get().GetColumnBindings();
	auto aggregate_bindings = join.children[1]->GetColumnBindings();
	column_binding_set_t covered_domain_bindings;
	for (auto &condition : join.conditions) {
		if (!IsDomainJoinEquivalenceComparison(condition)) {
			return false;
		}
		ColumnBinding left;
		ColumnBinding right;
		if (!GetDomainBinding(condition.GetLHS(), left) || !GetDomainBinding(condition.GetRHS(), right)) {
			return false;
		}
		ColumnBinding domain_binding;
		ColumnBinding aggregate_binding;
		if (std::find(domain_bindings.begin(), domain_bindings.end(), left) != domain_bindings.end() &&
		    std::find(aggregate_bindings.begin(), aggregate_bindings.end(), right) != aggregate_bindings.end()) {
			domain_binding = left;
			aggregate_binding = right;
		} else if (std::find(domain_bindings.begin(), domain_bindings.end(), right) != domain_bindings.end() &&
		           std::find(aggregate_bindings.begin(), aggregate_bindings.end(), left) != aggregate_bindings.end()) {
			domain_binding = right;
			aggregate_binding = left;
		} else {
			return false;
		}
		if (!covered_domain_bindings.insert(domain_binding).second ||
		    !replacements.TryAdd(ReplacementBinding(domain_binding, aggregate_binding))) {
			return false;
		}
	}
	if (covered_domain_bindings.size() != domain_bindings.size()) {
		return false;
	}
	return true;
}

static void RemoveAggregateDomainPreservation(unique_ptr<LogicalOperator> &rhs_root, AggregateDomainUse &use,
                                              const BindingReplacementGraph &replacements) {
	if (!use.preservation) {
		return;
	}
	auto &preservation = *use.preservation;
	auto &join = preservation.join.get();
	CorrelatedColumnBindingReplacer replacer;
	replacements.AddTo(replacer);
	replacer.stop_operator = *join.children[1];
	replacer.VisitOperator(*rhs_root);
	preservation.join_location.get() = std::move(join.children[1]);
}

class DirectAggregateRewrite {
public:
	DirectAggregateRewrite(Binder &binder_p, unique_ptr<LogicalOperator> &join_op_p, TableIndex domain_cte_index_p,
	                       LogicalOperator &rewrite_root_p, DuplicateEliminatedDomainCandidate &selected_candidate_p)
	    : binder(binder_p), join_op(join_op_p), join(join_op_p->Cast<LogicalComparisonJoin>()),
	      domain_cte_index(domain_cte_index_p), rewrite_root(rewrite_root_p), selected_candidate(selected_candidate_p) {
	}

	bool TryRewrite(BindingReplacementGraph &output_replacements) {
		if (!Analyze()) {
			return false;
		}
		Apply(output_replacements);
		return true;
	}

private:
	bool Analyze() {
		if (join.children.size() != 2 || join.duplicate_eliminated_columns.empty() ||
		    (join.join_type != JoinType::INNER && join.join_type != JoinType::LEFT &&
		     join.join_type != JoinType::SINGLE)) {
			return false;
		}
		auto domain_ref_count = CountDomainCTERefs(*join.children[1], domain_cte_index);
		if (domain_ref_count != 1 && domain_ref_count != 2) {
			return false;
		}
		old_output_bindings = join_op->GetColumnBindings();
		domain_use = FindAggregateDomainUse(join.children[1], domain_cte_index);
		if (!domain_use || domain_ref_count != (domain_use->preservation ? 2 : 1) ||
		    domain_use->domain_ref.get().chunk_types.size() != join.duplicate_eliminated_columns.size() ||
		    !AggregateGroupsMatchDomain(*domain_use) ||
		    !GetAggregateDomainPreservationReplacements(*domain_use, preservation_replacements)) {
			return false;
		}

		if (!FindDirectSourcePath(join.children[0], *selected_candidate.source.get(), source_path)) {
			return false;
		}
		auto &direct_source_location = source_path.back().get();
		source_candidate = DuplicateEliminatedDomainCandidateFinder::CreateForSource(join, direct_source_location,
		                                                                             selected_candidate.joins_above);
		if (!source_candidate) {
			return false;
		}
		scan_identity = FindEligibleScanIdentity(binder.context, *source_candidate);
		if (!scan_identity) {
			return false;
		}
		auto &selected_source = *source_candidate->source.get();
		if (&selected_source == join.children[0].get()) {
			return false;
		}
		source_bindings = selected_source.GetColumnBindings();
		source_types = selected_source.types;
		if (source_bindings.size() != source_types.size()) {
			return false;
		}
		source_binding_set.insert(source_bindings.begin(), source_bindings.end());
		if (!CanDetachCandidate(source_path, source_binding_set) ||
		    !GetAlignmentConditions(join, alignment_condition_indices)) {
			return false;
		}
		drop_empty_groups = join.join_type == JoinType::INNER;
		return true;
	}

	void Apply(BindingReplacementGraph &output_replacements) {
		unique_ptr<LogicalOperator> source;
		vector<JoinCondition> boundary_conditions;
		DetachCandidate(source_path, 0, source_binding_set, source, boundary_conditions);
		if (!source || !join.children[0] || boundary_conditions.empty()) {
			throw InternalException("Validated duplicate-eliminated aggregate source could not be detached");
		}

		vector<TrackedBinding> identities;
		if (!AddScanIdentity(*source, *scan_identity, identities)) {
			throw InternalException("Validated duplicate-eliminated aggregate source lost its scan identity");
		}
		source->ResolveOperatorTypes();
		auto new_source_bindings = source->GetColumnBindings();
		vector<TrackedBinding> source_columns;
		source_columns.reserve(source_bindings.size());
		for (idx_t binding_idx = 0; binding_idx < source_bindings.size(); binding_idx++) {
			source_columns.emplace_back(new_source_bindings[binding_idx], source_types[binding_idx]);
		}

		auto domain_bindings = domain_use->domain_ref.get().GetColumnBindings();
		BindingReplacementGraph domain_replacements;
		for (idx_t key_idx = 0; key_idx < source_candidate->key_indices.size(); key_idx++) {
			auto source_idx = source_candidate->key_indices[key_idx];
			domain_replacements.Add(domain_bindings[key_idx], source_columns[source_idx].binding);
		}
		domain_use->domain_join.get().children[domain_use->domain_child] = std::move(source);
		if (domain_use->domain_child == 0) {
			std::swap(domain_use->domain_join.get().children[0], domain_use->domain_join.get().children[1]);
			for (auto &condition : domain_use->domain_join.get().conditions) {
				condition.Swap();
			}
		}
		domain_use->domain_join.get().join_type = drop_empty_groups ? JoinType::INNER : JoinType::RIGHT;

		CorrelatedColumnBindingReplacer domain_replacer;
		domain_replacements.AddTo(domain_replacer);
		domain_replacer.stop_operator = *domain_use->domain_join.get().children[1];
		domain_replacer.VisitOperator(*join.children[1]);

		vector<TrackedBinding> aggregate_marker;
		if (!drop_empty_groups) {
			aggregate_marker.push_back(AddRealRowMarker(binder, join.children[1], *domain_use));
		}
		domain_use->domain_join.get().ResolveOperatorTypes();

		vector<TrackedBinding> aggregate_source_columns = source_columns;
		vector<TrackedBinding> aggregate_identities = identities;
		PropagateBindings(domain_use->below_aggregate, aggregate_source_columns);
		PropagateBindings(domain_use->below_aggregate, aggregate_identities);
		if (!drop_empty_groups) {
			PropagateBindings(domain_use->below_aggregate, aggregate_marker);
		}

		auto &aggregate = domain_use->aggregate.get();
		vector<TrackedBinding> output_source_columns;
		output_source_columns.reserve(aggregate_source_columns.size());
		for (auto &source_column : aggregate_source_columns) {
			auto group_idx = ColumnBinding::PushExpression(
			    aggregate.groups, make_uniq<BoundColumnRefExpression>(source_column.type, source_column.binding));
			output_source_columns.emplace_back(ColumnBinding(aggregate.group_index, group_idx), source_column.type);
		}
		for (auto &identity : aggregate_identities) {
			ColumnBinding::PushExpression(aggregate.groups,
			                              make_uniq<BoundColumnRefExpression>(identity.type, identity.binding));
		}
		if (!drop_empty_groups) {
			AddAggregateFilter(aggregate, aggregate_marker[0]);
		}
		aggregate.ResolveOperatorTypes();

		RemoveAggregateDomainPreservation(join.children[1], *domain_use, preservation_replacements);
		PropagateBindings(domain_use->above_aggregate, output_source_columns);
		join.children[1]->ResolveOperatorTypes();
		if (CountDomainCTERefs(*join.children[1], domain_cte_index) != 0) {
			throw InternalException("CTE-free correlated aggregate retained a generated domain reference");
		}
		BindingReplacementGraph source_replacements;
		for (idx_t binding_idx = 0; binding_idx < source_bindings.size(); binding_idx++) {
			source_replacements.Add(source_bindings[binding_idx], output_source_columns[binding_idx].binding);
		}

		std::sort(alignment_condition_indices.begin(), alignment_condition_indices.end(), std::greater<idx_t>());
		for (auto condition_idx : alignment_condition_indices) {
			join.conditions.erase(join.conditions.begin() +
			                      NumericCast<vector<JoinCondition>::difference_type>(condition_idx));
		}
		for (auto &condition : boundary_conditions) {
			join.conditions.push_back(std::move(condition));
		}
		if (join.conditions.empty()) {
			throw InternalException("CTE-free correlated aggregate rewrite produced a disconnected payload");
		}
		join.join_type = JoinType::INNER;
		join.duplicate_eliminated_columns.clear();
		// These maps are pruning metadata inherited from the delimiter join. Keeping them would prevent the resulting
		// ordinary comparison join from participating in join-order enumeration.
		join.left_projection_map.clear();
		join.right_projection_map.clear();

		CorrelatedColumnBindingReplacer source_replacer;
		source_replacements.AddTo(source_replacer);
		source_replacer.stop_operator = *join.children[1];
		source_replacer.VisitOperator(rewrite_root);
		BindingReplacementGraph direct_replacements;
		direct_replacements.Merge(preservation_replacements);
		direct_replacements.Merge(source_replacements);
		join_op->ResolveOperatorTypes();
		ColumnBindingRewrite::ValidateOutput(old_output_bindings, join_op->GetColumnBindings(), direct_replacements);
		output_replacements.Merge(preservation_replacements);
		output_replacements.Merge(source_replacements);
	}

private:
	Binder &binder;
	unique_ptr<LogicalOperator> &join_op;
	LogicalComparisonJoin &join;
	TableIndex domain_cte_index;
	LogicalOperator &rewrite_root;
	DuplicateEliminatedDomainCandidate &selected_candidate;

	unique_ptr<AggregateDomainUse> domain_use;
	unique_ptr<DuplicateEliminatedDomainCandidate> source_candidate;
	unique_ptr<ScanIdentityDescriptor> scan_identity;
	operator_location_path_t source_path;
	vector<ColumnBinding> old_output_bindings;
	vector<ColumnBinding> source_bindings;
	vector<LogicalType> source_types;
	column_binding_set_t source_binding_set;
	vector<idx_t> alignment_condition_indices;
	BindingReplacementGraph preservation_replacements;
	bool drop_empty_groups = false;
};

bool DuplicateEliminatedAggregateRewriter::TryRewrite(Binder &binder, unique_ptr<LogicalOperator> &join_op,
                                                      TableIndex domain_cte_index, LogicalOperator &rewrite_root,
                                                      DuplicateEliminatedDomainCandidate &candidate,
                                                      BindingReplacementGraph &output_replacements) {
	DirectAggregateRewrite rewrite(binder, join_op, domain_cte_index, rewrite_root, candidate);
	return rewrite.TryRewrite(output_replacements);
}

} // namespace duckdb
