#include "duckdb/optimizer/projection_placement.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/column_lifetime_analyzer.hpp"
#include "duckdb/optimizer/expression_heuristics.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/projection_pullup.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb {

namespace {

constexpr double MINIMUM_COST_RATIO = 0.75;

struct PathStep {
	LogicalOperator *op;
	idx_t child_index;
};

struct ExpressionPlacement {
	idx_t expression_index = 0;
	unique_ptr<LogicalOperator> *target = nullptr;
	vector<PathStep> path;
	unique_ptr<Expression> expression;
	double evaluation_cardinality = 0;
	double output_transport_cardinality = 0;
	double input_transport_savings_cardinality = 0;
};

struct PlacementGroup {
	unique_ptr<LogicalOperator> *target = nullptr;
	vector<PathStep> path;
	vector<ExpressionPlacement> placements;
};

static void CopyStatistics(column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                           const ColumnBinding &source, const ColumnBinding &target) {
	auto entry = statistics_map.find(source);
	if (entry == statistics_map.end()) {
		return;
	}
	statistics_map.erase(target);
	statistics_map.emplace(target, entry->second->Copy().ToUnique());
}

static bool GetBindingWidth(const ColumnBinding &binding, const LogicalType &type,
                            const column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map, idx_t &width) {
	auto physical_type = type.InternalType();
	if (TypeIsConstantSize(physical_type)) {
		width = GetTypeIdSize(physical_type);
		return true;
	}
	if (physical_type != PhysicalType::VARCHAR) {
		return false;
	}
	// Variable-width values are only costed when statistics provide a safe upper bound.
	auto entry = statistics_map.find(binding);
	if (entry == statistics_map.end() || !StringStats::HasMaxStringLength(*entry->second)) {
		return false;
	}
	width = GetTypeIdSize(physical_type) + StringStats::MaxStringLength(*entry->second);
	return true;
}

static bool GetExpressionWidth(const Expression &expression,
                               const column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                               idx_t &input_width, vector<pair<ColumnBinding, idx_t>> *binding_widths = nullptr) {
	vector<pair<ColumnBinding, LogicalType>> bindings;
	bool valid = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expression, [&](const BoundColumnRefExpression &colref) {
		    if (colref.Depth() != 0) {
			    valid = false;
			    return;
		    }
		    for (auto &entry : bindings) {
			    if (entry.first == colref.Binding()) {
				    return;
			    }
		    }
		    bindings.emplace_back(colref.Binding(), colref.GetReturnType());
	    });
	if (!valid || bindings.empty()) {
		return false;
	}
	input_width = 0;
	for (auto &entry : bindings) {
		idx_t binding_width;
		if (!GetBindingWidth(entry.first, entry.second, statistics_map, binding_width)) {
			return false;
		}
		input_width += binding_width;
		if (binding_widths) {
			binding_widths->emplace_back(entry.first, binding_width);
		}
	}
	return true;
}

static idx_t GetExclusiveInputWidth(const LogicalProjection &projection, idx_t expression_index,
                                    const vector<pair<ColumnBinding, idx_t>> &binding_widths) {
	idx_t result = 0;
	for (auto &binding_width : binding_widths) {
		auto &binding = binding_width.first;
		bool referenced_elsewhere = false;
		for (idx_t other_index = 0; other_index < projection.expressions.size(); other_index++) {
			if (other_index == expression_index) {
				continue;
			}
			ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
			    *projection.expressions[other_index], [&](const BoundColumnRefExpression &colref) {
				    if (colref.Depth() == 0 && colref.Binding() == binding) {
					    referenced_elsewhere = true;
				    }
			    });
		}
		if (referenced_elsewhere) {
			continue;
		}
		result += binding_width.second;
	}
	return result;
}

static bool BindingsBelongToChild(const Expression &expression, LogicalOperator &child) {
	auto child_bindings = child.GetColumnBindings();
	bool found = false;
	bool valid = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expression, [&](const BoundColumnRefExpression &colref) {
		    found = true;
		    if (colref.Depth() != 0 ||
		        std::find(child_bindings.begin(), child_bindings.end(), colref.Binding()) == child_bindings.end()) {
			    valid = false;
		    }
	    });
	return found && valid;
}

static bool RewriteThroughProjection(unique_ptr<Expression> &expression, LogicalProjection &projection) {
	bool valid = true;
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expression, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
		    if (!valid || colref.Depth() != 0 || colref.Binding().table_index != projection.table_index ||
		        colref.Binding().column_index.GetIndex() >= projection.expressions.size()) {
			    valid = false;
			    return;
		    }
		    auto &source = *projection.expressions[colref.Binding().column_index.GetIndex()];
		    if (source.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		        source.GetReturnType() != colref.GetReturnType()) {
			    valid = false;
			    return;
		    }
		    auto &source_colref = source.Cast<BoundColumnRefExpression>();
		    if (source_colref.Depth() != 0) {
			    valid = false;
			    return;
		    }
		    colref.BindingMutable() = source_colref.Binding();
	    });
	return valid;
}

static bool IsEligibleJoin(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op.type != LogicalOperatorType::LOGICAL_ANY_JOIN) {
		return false;
	}
	return op.Cast<LogicalJoin>().join_type == JoinType::INNER;
}

static bool ProjectionExpressionUsedByJoin(LogicalOperator &join, LogicalProjection &projection) {
	bool result = false;
	LogicalOperatorVisitor::EnumerateExpressions(join, [&](unique_ptr<Expression> *expression) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    **expression, [&](const BoundColumnRefExpression &colref) {
			    if (colref.Depth() != 0 || colref.Binding().table_index != projection.table_index) {
				    return;
			    }
			    auto column_index = colref.Binding().column_index.GetIndex();
			    if (column_index >= projection.expressions.size() ||
			        projection.expressions[column_index]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				    result = true;
			    }
		    });
	});
	return result;
}

static bool ShouldLiftProjection(LogicalOperator &join, LogicalProjection &projection,
                                 const column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map) {
	if (!join.has_estimated_cardinality || !projection.children[0]->has_estimated_cardinality ||
	    ProjectionExpressionUsedByJoin(join, projection)) {
		return false;
	}
	bool has_computed_expression = false;
	for (idx_t expression_index = 0; expression_index < projection.expressions.size(); expression_index++) {
		auto &expression = *projection.expressions[expression_index];
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		has_computed_expression = true;
		if (expression.IsVolatile() || expression.CanThrow()) {
			return false;
		}
		idx_t input_width;
		idx_t output_width;
		vector<pair<ColumnBinding, idx_t>> input_bindings;
		if (!GetExpressionWidth(expression, statistics_map, input_width, &input_bindings) ||
		    !GetBindingWidth(ColumnBinding(projection.table_index, ProjectionIndex(expression_index)),
		                     expression.GetReturnType(), statistics_map, output_width)) {
			return false;
		}
		auto exclusive_input_width = GetExclusiveInputWidth(projection, expression_index, input_bindings);
		auto evaluation_width = static_cast<double>(input_width) + static_cast<double>(output_width) +
		                        static_cast<double>(ExpressionHeuristics::Cost(expression));
		auto transport_cardinality = static_cast<double>(join.estimated_cardinality);
		auto lower_cost = static_cast<double>(projection.children[0]->estimated_cardinality) * evaluation_width +
		                  transport_cardinality * static_cast<double>(output_width);
		auto upper_cost = transport_cardinality * evaluation_width +
		                  transport_cardinality * static_cast<double>(exclusive_input_width);
		if (lower_cost == 0 || upper_cost > lower_cost * MINIMUM_COST_RATIO) {
			return false;
		}
	}
	return has_computed_expression;
}

// Lift one lower projection at a time, then re-evaluate the changed path.
static bool LiftFirstProfitableProjection(Optimizer &optimizer,
                                          column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                                          unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		if (LiftFirstProfitableProjection(optimizer, statistics_map, root, child)) {
			return true;
		}
	}
	if (!IsEligibleJoin(*op)) {
		return false;
	}
	for (idx_t child_index = 0; child_index < op->children.size(); child_index++) {
		if (op->children[child_index]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
			continue;
		}
		auto &projection = op->children[child_index]->Cast<LogicalProjection>();
		if (!ShouldLiftProjection(*op, projection, statistics_map)) {
			continue;
		}
		auto join = op.get();
		ProjectionPullup pullup(optimizer, root);
		return pullup.OptimizeJoinChild(*join, join->children[child_index]);
	}
	return false;
}

static optional_idx GetExpressionChild(const Expression &expression, LogicalOperator &op) {
	const bool references_left = BindingsBelongToChild(expression, *op.children[0]);
	const bool references_right = BindingsBelongToChild(expression, *op.children[1]);
	if (references_left == references_right) {
		return optional_idx();
	}
	return optional_idx(references_right ? 1 : 0);
}

static bool FindPlacement(LogicalProjection &projection, idx_t expression_index,
                          const column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                          ExpressionPlacement &result) {
	auto &expression = *projection.expressions[expression_index];
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF || expression.IsVolatile() ||
	    expression.CanThrow() || !projection.children[0]->has_estimated_cardinality) {
		return false;
	}

	idx_t input_width;
	vector<pair<ColumnBinding, idx_t>> input_bindings;
	if (!GetExpressionWidth(expression, statistics_map, input_width, &input_bindings)) {
		return false;
	}
	auto exclusive_input_width = GetExclusiveInputWidth(projection, expression_index, input_bindings);
	idx_t output_width;
	if (!GetBindingWidth(ColumnBinding(projection.table_index, ProjectionIndex(expression_index)),
	                     expression.GetReturnType(), statistics_map, output_width)) {
		return false;
	}

	const double evaluation_width = static_cast<double>(input_width) + static_cast<double>(output_width) +
	                                static_cast<double>(ExpressionHeuristics::Cost(expression));
	const double current_evaluation_cost =
	    static_cast<double>(projection.children[0]->estimated_cardinality) * evaluation_width;
	if (current_evaluation_cost == 0) {
		return false;
	}

	auto current_expression = expression.Copy();
	auto current = &projection.children[0];
	vector<PathStep> path;
	double transport_cardinality = 0;
	double input_transport_savings_cardinality = 0;
	// Beyond the first direct join, the source can be live for another path expression or join condition.
	bool can_credit_input_transport = true;
	vector<ExpressionPlacement> candidates;

	// Compare every position on the dependency path instead of making a greedy per-join decision.
	while (current && current->get()) {
		auto &op = **current;
		if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			if (op.children.size() != 1 || !BindingsBelongToChild(*current_expression, op)) {
				break;
			}
			path.push_back({&op, 0});
			can_credit_input_transport = false;
			current = &op.children[0];
			continue;
		}
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &lower_projection = op.Cast<LogicalProjection>();
			if (!RewriteThroughProjection(current_expression, lower_projection)) {
				break;
			}
			path.push_back({&op, 0});
			can_credit_input_transport = false;
			current = &op.children[0];
			continue;
		}
		if (!IsEligibleJoin(op) || !op.has_estimated_cardinality) {
			break;
		}
		auto child_index = GetExpressionChild(*current_expression, op);
		if (!child_index.IsValid()) {
			break;
		}
		auto &child = op.children[child_index.GetIndex()];
		if (!child->has_estimated_cardinality) {
			break;
		}
		path.push_back({&op, child_index.GetIndex()});
		transport_cardinality += static_cast<double>(op.estimated_cardinality);
		if (can_credit_input_transport) {
			input_transport_savings_cardinality += static_cast<double>(op.estimated_cardinality);
		}
		can_credit_input_transport = false;
		ExpressionPlacement candidate;
		candidate.expression_index = expression_index;
		candidate.target = &child;
		candidate.path = path;
		candidate.expression = current_expression->Copy();
		candidate.evaluation_cardinality = static_cast<double>(child->estimated_cardinality);
		candidate.output_transport_cardinality = transport_cardinality;
		candidate.input_transport_savings_cardinality = input_transport_savings_cardinality;
		candidates.push_back(std::move(candidate));
		current = &child;
	}

	const double current_cost =
	    current_evaluation_cost + input_transport_savings_cardinality * static_cast<double>(exclusive_input_width);
	double best_cost = current_cost * MINIMUM_COST_RATIO;
	bool found = false;
	for (auto &candidate : candidates) {
		const double remaining_input_transport =
		    input_transport_savings_cardinality - candidate.input_transport_savings_cardinality;
		const double candidate_cost = candidate.evaluation_cardinality * evaluation_width +
		                              candidate.output_transport_cardinality * static_cast<double>(output_width) +
		                              remaining_input_transport * static_cast<double>(exclusive_input_width);
		if (candidate_cost < best_cost) {
			best_cost = candidate_cost;
			result = std::move(candidate);
			found = true;
		}
	}
	return found;
}

static void ExposeBinding(PathStep &step, const ColumnBinding &binding) {
	auto &child = *step.op->children[step.child_index];
	auto child_bindings = child.GetColumnBindings();
	auto entry = std::find(child_bindings.begin(), child_bindings.end(), binding);
	if (entry == child_bindings.end()) {
		throw InternalException("Projection placement lost binding %s", binding.ToString());
	}
	auto projection_map = LogicalOperatorVisitor::GetProjectionMap(*step.op, step.child_index);
	if (!projection_map || projection_map->empty()) {
		return;
	}
	auto projection_index = ProjectionIndex(NumericCast<idx_t>(entry - child_bindings.begin()));
	if (std::find(projection_map->begin(), projection_map->end(), projection_index) == projection_map->end()) {
		projection_map->push_back(projection_index);
	}
}

static void ApplyPlacementGroup(Optimizer &optimizer, column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                                LogicalProjection &upper_projection, PlacementGroup &group) {
	auto &target = *group.target;
	target->ResolveOperatorTypes();
	auto old_bindings = target->GetColumnBindings();
	auto old_types = target->types;

	vector<unique_ptr<Expression>> lower_expressions;
	lower_expressions.reserve(old_bindings.size() + group.placements.size());
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		lower_expressions.push_back(make_uniq<BoundColumnRefExpression>(old_types[i], old_bindings[i]));
	}
	for (auto &placement : group.placements) {
		lower_expressions.push_back(std::move(placement.expression));
	}

	auto lower_index = optimizer.binder.GenerateTableIndex();
	auto lower_projection = make_uniq<LogicalProjection>(lower_index, std::move(lower_expressions));
	if (target->has_estimated_cardinality) {
		lower_projection->SetEstimatedCardinality(target->estimated_cardinality);
	}
	lower_projection->children.push_back(std::move(target));
	target = std::move(lower_projection);

	ColumnBindingReplacer replacer;
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		auto new_binding = ColumnBinding(lower_index, ProjectionIndex(i));
		replacer.replacement_bindings.emplace_back(old_bindings[i], new_binding);
		CopyStatistics(statistics_map, old_bindings[i], new_binding);
	}
	replacer.stop_operator = target.get();
	replacer.VisitOperator(*upper_projection.children[0]);
	replacer.VisitOperatorBindings(upper_projection);

	vector<ColumnBinding> computed_bindings;
	computed_bindings.reserve(group.placements.size());
	for (idx_t i = 0; i < group.placements.size(); i++) {
		auto binding = ColumnBinding(lower_index, ProjectionIndex(old_bindings.size() + i));
		computed_bindings.push_back(binding);
		CopyStatistics(
		    statistics_map,
		    ColumnBinding(upper_projection.table_index, ProjectionIndex(group.placements[i].expression_index)),
		    binding);
	}

	for (idx_t path_idx = group.path.size(); path_idx > 0; path_idx--) {
		auto &step = group.path[path_idx - 1];
		if (step.op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = step.op->Cast<LogicalProjection>();
			for (idx_t i = 0; i < computed_bindings.size(); i++) {
				auto type = upper_projection.expressions[group.placements[i].expression_index]->GetReturnType();
				projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(type, computed_bindings[i]));
				auto new_binding =
				    ColumnBinding(projection.table_index, ProjectionIndex(projection.expressions.size() - 1));
				CopyStatistics(statistics_map, computed_bindings[i], new_binding);
				computed_bindings[i] = new_binding;
			}
		} else {
			for (auto &binding : computed_bindings) {
				ExposeBinding(step, binding);
			}
		}
	}

	for (idx_t i = 0; i < group.placements.size(); i++) {
		auto expression_index = group.placements[i].expression_index;
		auto alias = upper_projection.expressions[expression_index]->GetAlias();
		auto replacement = make_uniq<BoundColumnRefExpression>(
		    upper_projection.expressions[expression_index]->GetReturnType(), computed_bindings[i]);
		replacement->SetAlias(std::move(alias));
		upper_projection.expressions[expression_index] = std::move(replacement);
	}
}

static void OptimizeOperator(Optimizer &optimizer, column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                             unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		OptimizeOperator(optimizer, statistics_map, child);
	}
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &projection = op->Cast<LogicalProjection>();
	while (true) {
		PlacementGroup group;
		for (idx_t expression_index = 0; expression_index < projection.expressions.size(); expression_index++) {
			ExpressionPlacement placement;
			if (!FindPlacement(projection, expression_index, statistics_map, placement)) {
				continue;
			}
			if (!group.target) {
				group.target = placement.target;
				group.path = placement.path;
			}
			if (group.target == placement.target) {
				group.placements.push_back(std::move(placement));
			}
		}
		if (group.placements.empty()) {
			break;
		}
		ApplyPlacementGroup(optimizer, statistics_map, projection, group);
	}
}

} // namespace

ProjectionPlacementOptimizer::ProjectionPlacementOptimizer(
    Optimizer &optimizer_p, column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map_p)
    : optimizer(optimizer_p), statistics_map(statistics_map_p) {
}

void ProjectionPlacementOptimizer::Optimize(unique_ptr<LogicalOperator> &plan) {
	while (LiftFirstProfitableProjection(optimizer, statistics_map, plan, plan)) {
		plan->ResolveOperatorTypes();
	}
	OptimizeOperator(optimizer, statistics_map, plan);
	plan->ResolveOperatorTypes();
	if (!optimizer.OptimizerDisabled(OptimizerType::COLUMN_LIFETIME)) {
		ColumnLifetimeAnalyzer column_lifetime(optimizer, *plan, true);
		column_lifetime.VisitOperator(*plan);
		plan->ResolveOperatorTypes();
	}
}

} // namespace duckdb
