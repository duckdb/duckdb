#include "duckdb/optimizer/projection_placement.hpp"

#include "duckdb/common/reference_map.hpp"
#include "duckdb/function/scalar/compressed_materialization_utils.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/expression_heuristics.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb {

namespace {

constexpr double MINIMUM_COST_RATIO = 0.75;
//! An expression up to this cost (e.g. a comparison of a numeric column with a constant) is not moved below joins
//! unless that frees one of its inputs
constexpr idx_t MAXIMUM_CHEAP_EXPRESSION_COST = 20;

struct PathStep {
	reference<LogicalOperator> op;
	idx_t child_index;
};

struct ExpressionPlacement {
	idx_t expression_index = 0;
	vector<PathStep> path;
	unique_ptr<Expression> expression;
};

struct PlacementCandidate {
	idx_t path_size;
	unique_ptr<Expression> expression;
	double evaluation_cardinality = 0;
	double output_transport_cardinality = 0;
	double input_transport_savings_cardinality = 0;
};

struct PlacementGroup {
	vector<PathStep> path;
	vector<ExpressionPlacement> placements;
};

struct ExpressionBindingWidth {
	ColumnBinding binding;
	LogicalType type;
	idx_t width;
};

struct LiftPathStep {
	reference<LogicalOperator> op;
	idx_t child_index;
	vector<ColumnBinding> output_bindings;
	column_binding_set_t result_bindings;
};

struct LiftOption {
	idx_t path_size;
	double evaluation_cardinality;
	double input_transport_cardinality;
	column_binding_set_t result_bindings;
	vector<ColumnBinding> target_bindings;
};

struct LiftPlacement {
	TableIndex source_table_index;
	idx_t expression_index;
	ColumnBinding source_binding;
	vector<ExpressionBindingWidth> dependencies;
	vector<LiftPathStep> path;
	column_binding_set_t result_bindings;
	vector<ColumnBinding> target_bindings;
};

struct LiftDependency {
	ColumnBinding original_binding;
	ColumnBinding current_binding;
	LogicalType type;
};

struct LiftedExpression {
	idx_t expression_index;
	ColumnBinding source_binding;
};

struct LiftPlacementGroup {
	TableIndex source_table_index;
	vector<LiftedExpression> expressions;
	vector<LiftPathStep> path;
	vector<ColumnBinding> target_bindings;
	vector<LiftDependency> dependencies;
	column_binding_map_t<idx_t> dependency_indexes;
	column_binding_map_t<idx_t> result_indexes;

	bool TryAdd(LiftPlacement placement) {
		if (source_table_index.IsValid()) {
			if (source_table_index != placement.source_table_index ||
			    !RefersToSameObject(path.back().op.get(), placement.path.back().op.get())) {
				return false;
			}
			D_ASSERT(path.size() == placement.path.size());
			for (idx_t path_idx = 0; path_idx < path.size(); path_idx++) {
				D_ASSERT(RefersToSameObject(path[path_idx].op.get(), placement.path[path_idx].op.get()));
				auto &bindings = placement.path[path_idx].result_bindings;
				path[path_idx].result_bindings.insert(bindings.begin(), bindings.end());
			}
		} else {
			source_table_index = placement.source_table_index;
			path = std::move(placement.path);
			target_bindings = std::move(placement.target_bindings);
		}
		for (auto &dependency : placement.dependencies) {
			auto entry = dependency_indexes.emplace(dependency.binding, dependencies.size());
			if (entry.second) {
				dependencies.push_back({dependency.binding, dependency.binding, dependency.type});
			} else {
				D_ASSERT(dependencies[entry.first->second].type == dependency.type);
			}
		}
		for (auto &binding : placement.result_bindings) {
			auto inserted = result_indexes.emplace(binding, expressions.size());
			D_ASSERT(inserted.second);
			(void)inserted;
		}
		expressions.push_back({placement.expression_index, placement.source_binding});
		return true;
	}
};

static unique_ptr<LogicalOperator> &GetPathTarget(const vector<PathStep> &path) {
	D_ASSERT(!path.empty());
	auto &last_step = path.back();
	return last_step.op.get().children[last_step.child_index];
}

static unique_ptr<LogicalOperator> &GetPathOperator(unique_ptr<LogicalOperator> &root, const vector<PathStep> &path,
                                                    idx_t index) {
	D_ASSERT(index < path.size());
	auto &op = index == 0 ? root : path[index - 1].op.get().children[path[index - 1].child_index];
	D_ASSERT(RefersToSameObject(*op, path[index].op.get()));
	return op;
}

static void CopyStatistics(column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                           const ColumnBinding &source, const ColumnBinding &target) {
	auto entry = statistics_map.find(source);
	if (entry == statistics_map.end()) {
		return;
	}
	unique_ptr<BaseStatistics> statistics;
	if (entry->second) {
		statistics = entry->second->Copy().ToUnique();
	}
	statistics_map.erase(target);
	if (statistics) {
		statistics_map.emplace(target, std::move(statistics));
	}
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
	if (entry == statistics_map.end() || !entry->second ||
	    entry->second->GetStatsType() != StatisticsType::STRING_STATS ||
	    !StringStats::HasMaxStringLength(*entry->second)) {
		return false;
	}
	width = GetTypeIdSize(physical_type) + StringStats::MaxStringLength(*entry->second);
	return true;
}

static bool GetExpressionWidth(const Expression &expression,
                               const column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                               idx_t &input_width, vector<ExpressionBindingWidth> &binding_widths) {
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
		binding_widths.push_back({entry.first, entry.second, binding_width});
	}
	return true;
}

//! The number of expressions of the projection that read each binding
static column_binding_map_t<idx_t> CountReadingExpressions(const LogicalProjection &projection) {
	column_binding_map_t<idx_t> result;
	for (auto &expression : projection.expressions) {
		column_binding_set_t read_bindings;
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *expression, [&](const BoundColumnRefExpression &colref) {
			    if (colref.Depth() == 0 && read_bindings.insert(colref.Binding()).second) {
				    result[colref.Binding()]++;
			    }
		    });
	}
	return result;
}

//! The width of the bindings that no other expression of the projection reads
static idx_t GetExclusiveInputWidth(const column_binding_map_t<idx_t> &reading_expressions,
                                    const vector<ExpressionBindingWidth> &binding_widths) {
	idx_t result = 0;
	for (auto &binding_width : binding_widths) {
		auto entry = reading_expressions.find(binding_width.binding);
		if (entry != reading_expressions.end() && entry->second > 1) {
			continue;
		}
		result += binding_width.width;
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
	expression = ExpressionIterator::PreorderReplace<BoundColumnRefExpression>(
	    std::move(expression), [&](const BoundColumnRefExpression &colref) -> unique_ptr<Expression> {
		    if (!valid || colref.Depth() != 0 || colref.Binding().table_index != projection.table_index ||
		        colref.Binding().column_index.GetIndex() >= projection.expressions.size()) {
			    valid = false;
			    return nullptr;
		    }
		    auto &source = *projection.expressions[colref.Binding().column_index.GetIndex()];
		    if (source.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		        source.GetReturnType() != colref.GetReturnType()) {
			    valid = false;
			    return nullptr;
		    }
		    auto &source_colref = source.Cast<BoundColumnRefExpression>();
		    if (source_colref.Depth() != 0) {
			    valid = false;
			    return nullptr;
		    }
		    return colref.WithBinding(source_colref.Binding());
	    });
	return valid;
}

static bool IsEligibleJoin(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op.type != LogicalOperatorType::LOGICAL_ANY_JOIN) {
		return false;
	}
	return op.Cast<LogicalJoin>().join_type == JoinType::INNER;
}

static bool ReferencesAnyBinding(const Expression &expression, const column_binding_set_t &bindings) {
	bool result = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expression, [&](const BoundColumnRefExpression &colref) {
		    if (colref.Depth() == 0 && bindings.find(colref.Binding()) != bindings.end()) {
			    result = true;
		    }
	    });
	return result;
}

static bool OperatorReferencesAnyBinding(LogicalOperator &op, const column_binding_set_t &bindings) {
	bool result = false;
	const auto &constant_op = op;
	LogicalOperatorVisitor::EnumerateExpressions(constant_op, [&](const auto &expression) {
		if (!result && ReferencesAnyBinding(**expression, bindings)) {
			result = true;
		}
	});
	return result;
}

static column_binding_set_t RetainedBindings(LogicalOperator &op, const column_binding_set_t &bindings) {
	column_binding_set_t result;
	for (auto &binding : op.GetColumnBindings()) {
		if (bindings.find(binding) != bindings.end()) {
			result.insert(binding);
		}
	}
	return result;
}

static bool TraceProjectionResult(LogicalProjection &projection, const column_binding_set_t &input_bindings,
                                  column_binding_set_t &output_bindings) {
	for (idx_t expression_idx = 0; expression_idx < projection.expressions.size(); expression_idx++) {
		auto &expression = *projection.expressions[expression_idx];
		if (!ReferencesAnyBinding(expression, input_bindings)) {
			continue;
		}
		if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		if (colref.Depth() != 0 || input_bindings.find(colref.Binding()) == input_bindings.end()) {
			return false;
		}
		output_bindings.insert(ColumnBinding(projection.table_index, ProjectionIndex(expression_idx)));
	}
	return !output_bindings.empty();
}

static bool CanMoveExpression(const Expression &expression) {
	// Preserve evaluation effects and the placement chosen by compressed materialization.
	return expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF && !expression.IsVolatile() &&
	       !expression.CanThrow() && !CMUtils::GetWrappedInput(expression);
}

static bool FindLiftPlacement(LogicalProjection &projection, idx_t expression_index, const vector<PathStep> &ancestors,
                              const column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                              const column_binding_map_t<idx_t> &reading_expressions, LiftPlacement &result) {
	auto &expression = *projection.expressions[expression_index];
	if (!CanMoveExpression(expression) || !projection.children[0]->has_estimated_cardinality) {
		return false;
	}

	idx_t input_width;
	vector<ExpressionBindingWidth> dependencies;
	auto source_binding = ColumnBinding(projection.table_index, ProjectionIndex(expression_index));
	if (!GetExpressionWidth(expression, statistics_map, input_width, dependencies)) {
		return false;
	}
	auto exclusive_input_width = GetExclusiveInputWidth(reading_expressions, dependencies);

	column_binding_set_t current_bindings;
	current_bindings.insert(source_binding);
	vector<LiftPathStep> path;
	vector<LiftOption> options;
	double transport_cardinality = 0;

	for (idx_t ancestor_idx = ancestors.size(); ancestor_idx > 0; ancestor_idx--) {
		auto &ancestor = ancestors[ancestor_idx - 1];
		auto &op = ancestor.op.get();
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			column_binding_set_t projected_bindings;
			if (!TraceProjectionResult(op.Cast<LogicalProjection>(), current_bindings, projected_bindings)) {
				break;
			}
			current_bindings = std::move(projected_bindings);
		} else if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			if (op.children.size() != 1 || OperatorReferencesAnyBinding(op, current_bindings)) {
				break;
			}
			current_bindings = RetainedBindings(op, current_bindings);
			if (current_bindings.empty()) {
				break;
			}
		} else if (IsEligibleJoin(op)) {
			if (!op.has_estimated_cardinality || OperatorReferencesAnyBinding(op, current_bindings)) {
				break;
			}
			current_bindings = RetainedBindings(op, current_bindings);
			if (current_bindings.empty()) {
				break;
			}
		} else {
			break;
		}

		LiftPathStep step {op, ancestor.child_index, op.GetColumnBindings(), current_bindings};
		path.push_back(std::move(step));
		if (!IsEligibleJoin(op)) {
			continue;
		}
		transport_cardinality += static_cast<double>(op.estimated_cardinality);
		options.push_back({path.size(), static_cast<double>(op.estimated_cardinality), transport_cardinality,
		                   current_bindings, op.GetColumnBindings()});
	}

	if (options.empty()) {
		return false;
	}
	idx_t output_width;
	bool has_output_width = GetBindingWidth(source_binding, expression.GetReturnType(), statistics_map, output_width);
	for (auto &option : options) {
		for (auto &binding : option.result_bindings) {
			if (!has_output_width) {
				has_output_width = GetBindingWidth(binding, expression.GetReturnType(), statistics_map, output_width);
			}
		}
	}
	if (!has_output_width) {
		return false;
	}
	auto evaluation_width = static_cast<double>(input_width) + static_cast<double>(output_width) +
	                        static_cast<double>(ExpressionHeuristics::Cost(expression));
	const auto source_cardinality = static_cast<double>(projection.children[0]->estimated_cardinality);
	const auto current_cost =
	    source_cardinality * evaluation_width + transport_cardinality * static_cast<double>(output_width);
	if (current_cost == 0) {
		return false;
	}
	double best_cost = current_cost * MINIMUM_COST_RATIO;
	optional_idx best_option;
	for (idx_t option_idx = 0; option_idx < options.size(); option_idx++) {
		auto &option = options[option_idx];
		auto remaining_output_transport = transport_cardinality - option.input_transport_cardinality;
		auto candidate_cost = option.evaluation_cardinality * evaluation_width +
		                      option.input_transport_cardinality * static_cast<double>(exclusive_input_width) +
		                      remaining_output_transport * static_cast<double>(output_width);
		if (candidate_cost < best_cost) {
			best_cost = candidate_cost;
			best_option = optional_idx(option_idx);
		}
	}
	if (!best_option.IsValid()) {
		return false;
	}

	auto &option = options[best_option.GetIndex()];
	result.source_table_index = projection.table_index;
	result.expression_index = expression_index;
	result.source_binding = source_binding;
	result.dependencies = std::move(dependencies);
	result.path.reserve(option.path_size);
	for (idx_t path_idx = 0; path_idx < option.path_size; path_idx++) {
		result.path.push_back(path[path_idx]);
	}
	result.result_bindings = option.result_bindings;
	result.target_bindings = option.target_bindings;
	return true;
}

static LiftPlacementGroup
FindLiftPlacementGroup(LogicalProjection &projection, const vector<PathStep> &ancestors,
                       const column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map) {
	auto reading_expressions = CountReadingExpressions(projection);
	LiftPlacementGroup group;
	for (idx_t expression_idx = 0; expression_idx < projection.expressions.size(); expression_idx++) {
		LiftPlacement placement;
		if (FindLiftPlacement(projection, expression_idx, ancestors, statistics_map, reading_expressions, placement)) {
			// Other destinations must be reconsidered after bindings and input-reading counts change.
			group.TryAdd(std::move(placement));
		}
	}
	return group;
}

struct ProjectionRewrite {
	vector<ColumnBinding> old_bindings;
	vector<ColumnBinding> new_bindings;
};

static void RemapParentProjectionMap(LogicalOperator &parent, idx_t child_index,
                                     const vector<ColumnBinding> &old_bindings,
                                     const vector<ColumnBinding> &new_bindings) {
	auto projection_map = LogicalOperatorVisitor::GetProjectionMap(parent, child_index);
	if (!projection_map || projection_map->empty() || old_bindings == new_bindings) {
		return;
	}
	vector<ProjectionIndex> remapped;
	remapped.reserve(projection_map->size());
	for (auto projection_index : *projection_map) {
		auto &desired_binding = old_bindings[projection_index.GetIndex()];
		auto entry = std::find(new_bindings.begin(), new_bindings.end(), desired_binding);
		if (entry == new_bindings.end()) {
			projection_map->clear();
			return;
		}
		remapped.emplace_back(NumericCast<idx_t>(entry - new_bindings.begin()));
	}
	*projection_map = std::move(remapped);
}

static void ExposeBinding(LogicalOperator &op, idx_t child_index, const ColumnBinding &binding) {
	auto &child = *op.children[child_index];
	auto child_bindings = child.GetColumnBindings();
	auto entry = std::find(child_bindings.begin(), child_bindings.end(), binding);
	if (entry == child_bindings.end()) {
		throw InternalException("Projection placement lost binding %s", binding.ToString());
	}
	auto projection_map = LogicalOperatorVisitor::GetProjectionMap(op, child_index);
	if (!projection_map || projection_map->empty()) {
		return;
	}
	auto projection_index = ProjectionIndex(NumericCast<idx_t>(entry - child_bindings.begin()));
	if (std::find(projection_map->begin(), projection_map->end(), projection_index) == projection_map->end()) {
		projection_map->push_back(projection_index);
	}
}

static void SetProjectionTypes(LogicalProjection &projection) {
	projection.types.clear();
	for (auto &expression : projection.expressions) {
		projection.types.push_back(expression->GetReturnType());
	}
}

//! Wrap an owning slot and finish local metadata before exposing the new projection to its caller.
static LogicalProjection &WrapLiftProjection(Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                                             vector<unique_ptr<Expression>> expressions) {
	auto projection = make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(expressions));
	SetProjectionTypes(*projection);
	if (op->has_estimated_cardinality) {
		projection->SetEstimatedCardinality(op->estimated_cardinality);
	}
	projection->children.push_back(std::move(op));
	op = std::move(projection);
	return op->Cast<LogicalProjection>();
}

//! Select types from the current child layouts, including any columns exposed by a changed projection map.
//! Children on the rewritten path are already updated; other child subtrees do not need type resolution.
static void UpdateLiftTypes(LogicalOperator &op) {
	D_ASSERT(op.type == LogicalOperatorType::LOGICAL_FILTER || IsEligibleJoin(op));
	column_binding_map_t<LogicalType> types;
	for (auto &child : op.children) {
		auto bindings = child->GetColumnBindings();
		D_ASSERT(bindings.size() == child->types.size());
		for (idx_t i = 0; i < bindings.size(); i++) {
			types.emplace(bindings[i], child->types[i]);
		}
	}
	op.types.clear();
	for (auto &binding : op.GetColumnBindings()) {
		op.types.push_back(types.at(binding));
	}
}

static ProjectionRewrite
RebuildProjection(Optimizer &optimizer, column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                  unique_ptr<LogicalOperator> &op, const column_binding_set_t &removed_bindings,
                  vector<LiftDependency> &dependencies, BindingReplacementGraph &binding_replacements) {
	auto &projection = op->Cast<LogicalProjection>();
	auto old_bindings = projection.GetColumnBindings();
	vector<unique_ptr<Expression>> expressions;
	vector<optional_idx> dependency_indexes(dependencies.size());
	vector<pair<ColumnBinding, idx_t>> retained_bindings;
	for (idx_t expression_idx = 0; expression_idx < projection.expressions.size(); expression_idx++) {
		auto old_binding = old_bindings[expression_idx];
		if (removed_bindings.find(old_binding) != removed_bindings.end()) {
			continue;
		}
		auto new_index = expressions.size();
		auto &expression = projection.expressions[expression_idx];
		if (expression->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &colref = expression->Cast<BoundColumnRefExpression>();
			if (colref.Depth() == 0) {
				// Retained expressions can still refer to projections rebuilt below us.
				auto current_binding = binding_replacements.Resolve(colref.Binding());
				for (idx_t dependency_idx = 0; dependency_idx < dependencies.size(); dependency_idx++) {
					if (!dependency_indexes[dependency_idx].IsValid() &&
					    current_binding == dependencies[dependency_idx].current_binding &&
					    colref.GetReturnType() == dependencies[dependency_idx].type) {
						dependency_indexes[dependency_idx] = optional_idx(new_index);
					}
				}
			}
		}
		expressions.push_back(std::move(expression));
		retained_bindings.emplace_back(old_binding, new_index);
	}

	for (idx_t dependency_idx = 0; dependency_idx < dependencies.size(); dependency_idx++) {
		if (dependency_indexes[dependency_idx].IsValid()) {
			continue;
		}
		dependency_indexes[dependency_idx] = optional_idx(expressions.size());
		expressions.push_back(make_uniq<BoundColumnRefExpression>(dependencies[dependency_idx].type,
		                                                          dependencies[dependency_idx].current_binding));
	}

	auto table_index = optimizer.binder.GenerateTableIndex();
	auto replacement = make_uniq<LogicalProjection>(table_index, std::move(expressions));
	if (projection.has_estimated_cardinality) {
		replacement->SetEstimatedCardinality(projection.estimated_cardinality);
	}
	replacement->children.push_back(std::move(projection.children[0]));
	op = std::move(replacement);

	for (auto &retained_binding : retained_bindings) {
		auto new_binding = ColumnBinding(table_index, ProjectionIndex(retained_binding.second));
		binding_replacements.Add(retained_binding.first, new_binding);
		CopyStatistics(statistics_map, retained_binding.first, new_binding);
	}
	for (idx_t dependency_idx = 0; dependency_idx < dependencies.size(); dependency_idx++) {
		auto old_binding = dependencies[dependency_idx].current_binding;
		auto new_binding = ColumnBinding(table_index, ProjectionIndex(dependency_indexes[dependency_idx].GetIndex()));
		CopyStatistics(statistics_map, old_binding, new_binding);
		dependencies[dependency_idx].current_binding = new_binding;
	}
	// Only visit this projection's expressions. The enclosing path still has partially rebuilt layouts,
	// but resolving its child bindings does not require inspecting those layouts or walking other subtrees.
	ColumnBindingReplacer replacer(binding_replacements);
	replacer.VisitOperatorBindings(*op);
	SetProjectionTypes(op->Cast<LogicalProjection>());
	return {std::move(old_bindings), op->GetColumnBindings()};
}

static void RewriteLiftedExpression(unique_ptr<Expression> &expression, const vector<LiftDependency> &dependencies) {
	expression = ExpressionIterator::PreorderReplace<BoundColumnRefExpression>(
	    std::move(expression), [&](const BoundColumnRefExpression &colref) -> unique_ptr<Expression> {
		    if (colref.Depth() != 0) {
			    return nullptr;
		    }
		    for (auto &dependency : dependencies) {
			    if (colref.Binding() == dependency.original_binding) {
				    return colref.WithBinding(dependency.current_binding);
			    }
		    }
		    return nullptr;
	    });
}

static BindingReplacementGraph RebuildLiftPath(Optimizer &optimizer,
                                               column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                                               LiftPlacementGroup &group) {
	auto &path = group.path;
	auto &source_op = path.front().op.get().children[path.front().child_index];
	auto &dependencies = group.dependencies;
	column_binding_set_t source_bindings;
	for (auto &placement : group.expressions) {
		source_bindings.insert(placement.source_binding);
	}
	BindingReplacementGraph binding_replacements;

	auto source_rewrite =
	    RebuildProjection(optimizer, statistics_map, source_op, source_bindings, dependencies, binding_replacements);
	RemapParentProjectionMap(path[0].op.get(), path[0].child_index, source_rewrite.old_bindings,
	                         source_rewrite.new_bindings);

	for (idx_t path_idx = 0; path_idx < path.size(); path_idx++) {
		auto &step = path[path_idx];
		ProjectionRewrite rewrite {step.output_bindings, step.output_bindings};
		if (step.op.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			// The destination is a join, so every rebuilt projection has an unchanged parent above it.
			// Replace its owning child slot and immediately refresh the borrowed path reference.
			D_ASSERT(path_idx + 1 < path.size());
			auto &parent = path[path_idx + 1];
			auto &projection = parent.op.get().children[parent.child_index];
			rewrite = RebuildProjection(optimizer, statistics_map, projection, step.result_bindings, dependencies,
			                            binding_replacements);
			step.op = *projection;
		} else {
			for (auto &dependency : dependencies) {
				ExposeBinding(step.op.get(), step.child_index, dependency.current_binding);
			}
			rewrite.new_bindings = step.op.get().GetColumnBindings();
			ColumnBindingReplacer replacer(binding_replacements);
			replacer.VisitOperatorBindings(step.op.get());
			UpdateLiftTypes(step.op.get());
		}
		if (path_idx + 1 < path.size()) {
			RemapParentProjectionMap(path[path_idx + 1].op.get(), path[path_idx + 1].child_index, rewrite.old_bindings,
			                         rewrite.new_bindings);
		}
	}
	return binding_replacements;
}

struct LiftRewriteResult {
	BindingReplacementGraph output_replacements;
	//! Updated root-to-source-parent path, including every projection added by the rewrite.
	vector<PathStep> path_to_source;
};

static LiftRewriteResult RestoreLiftedOutputs(Optimizer &optimizer,
                                              column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                                              unique_ptr<LogicalOperator> &target, const LiftPlacementGroup &group,
                                              const BindingReplacementGraph &binding_replacements,
                                              vector<unique_ptr<Expression>> expressions,
                                              const vector<LogicalType> &target_types) {
	LiftRewriteResult result;
	for (idx_t path_idx = group.path.size(); path_idx > 0; path_idx--) {
		auto &step = group.path[path_idx - 1];
		result.path_to_source.push_back({step.op, step.child_index});
	}
	auto &result_indexes = group.result_indexes;
	auto current_target_bindings = target->GetColumnBindings();

	vector<unique_ptr<Expression>> compute_expressions;
	vector<ColumnBinding> compute_inputs;
	vector<idx_t> restore_indexes;
	restore_indexes.reserve(group.target_bindings.size());
	for (idx_t target_idx = 0; target_idx < group.target_bindings.size(); target_idx++) {
		auto &old_binding = group.target_bindings[target_idx];
		if (result_indexes.find(old_binding) != result_indexes.end()) {
			restore_indexes.push_back(DConstants::INVALID_INDEX);
			continue;
		}
		auto current_binding = binding_replacements.Resolve(old_binding);
		auto current_entry = std::find(current_target_bindings.begin(), current_target_bindings.end(), current_binding);
		if (current_entry == current_target_bindings.end()) {
			throw InternalException("Projection placement lost target binding %s", current_binding.ToString());
		}
		auto compute_entry = std::find(compute_inputs.begin(), compute_inputs.end(), current_binding);
		idx_t compute_idx;
		if (compute_entry == compute_inputs.end()) {
			compute_idx = compute_inputs.size();
			compute_inputs.push_back(current_binding);
			compute_expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(target_types[target_idx], current_binding));
		} else {
			compute_idx = NumericCast<idx_t>(compute_entry - compute_inputs.begin());
		}
		restore_indexes.push_back(compute_idx);
	}

	auto first_candidate_idx = compute_expressions.size();
	for (auto &expression : expressions) {
		compute_expressions.push_back(std::move(expression));
	}
	auto &compute_projection = WrapLiftProjection(optimizer, target, std::move(compute_expressions));
	auto compute_table_index = compute_projection.table_index;
	result.path_to_source.insert(result.path_to_source.begin(), {compute_projection, 0});

	vector<unique_ptr<Expression>> restore_expressions;
	restore_expressions.reserve(group.target_bindings.size());
	for (idx_t target_idx = 0; target_idx < group.target_bindings.size(); target_idx++) {
		auto &compute_idx = restore_indexes[target_idx];
		if (compute_idx == DConstants::INVALID_INDEX) {
			compute_idx = first_candidate_idx + result_indexes.at(group.target_bindings[target_idx]);
		}
		restore_expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    target_types[target_idx], ColumnBinding(compute_table_index, ProjectionIndex(compute_idx))));
	}
	auto &restore_projection = WrapLiftProjection(optimizer, target, std::move(restore_expressions));
	auto restore_table_index = restore_projection.table_index;
	result.path_to_source.insert(result.path_to_source.begin(), {restore_projection, 0});

	for (idx_t compute_idx = 0; compute_idx < compute_inputs.size(); compute_idx++) {
		CopyStatistics(statistics_map, compute_inputs[compute_idx],
		               ColumnBinding(compute_table_index, ProjectionIndex(compute_idx)));
	}
	for (idx_t placement_idx = 0; placement_idx < group.expressions.size(); placement_idx++) {
		CopyStatistics(statistics_map, group.expressions[placement_idx].source_binding,
		               ColumnBinding(compute_table_index, ProjectionIndex(first_candidate_idx + placement_idx)));
	}

	// The complete subtree now restores the original ordered values. Its callers only need these output changes;
	// references inside the subtree have already been repaired at each rebuilt operator.
	for (idx_t target_idx = 0; target_idx < group.target_bindings.size(); target_idx++) {
		auto restore_binding = ColumnBinding(restore_table_index, ProjectionIndex(target_idx));
		auto &old_binding = group.target_bindings[target_idx];
		if (!result.output_replacements.Find(old_binding)) {
			// Repeated output bindings are aliases of the same value: retain the first restore slot.
			result.output_replacements.Add(old_binding, restore_binding);
		}
		auto compute_idx = restore_indexes[target_idx];
		CopyStatistics(statistics_map, ColumnBinding(compute_table_index, ProjectionIndex(compute_idx)),
		               restore_binding);
	}
	return result;
}

static LiftRewriteResult RewriteLiftPlacementGroup(Optimizer &optimizer,
                                                   column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                                                   unique_ptr<LogicalOperator> &target, LiftPlacementGroup &group) {
	D_ASSERT(!group.expressions.empty());
	D_ASSERT(RefersToSameObject(*target, group.path.back().op.get()));
	auto &source = group.path.front().op.get().children[group.path.front().child_index]->Cast<LogicalProjection>();
	D_ASSERT(source.table_index == group.source_table_index);
	// Types are initialized once before lifting and maintained along each rewritten path.
	auto target_types = target->types;
	vector<unique_ptr<Expression>> expressions;
	for (auto &placement : group.expressions) {
		expressions.push_back(std::move(source.expressions[placement.expression_index]));
	}
	auto replacements = RebuildLiftPath(optimizer, statistics_map, group);
	for (auto &expression : expressions) {
		RewriteLiftedExpression(expression, group.dependencies);
	}
	return RestoreLiftedOutputs(optimizer, statistics_map, target, group, replacements, std::move(expressions),
	                            target_types);
}

//! Own traversal progress and its invalidation together. Rewrites must refresh the active path and forget
//! replaced operators before destruction; only newly exposed dependency producers need reconsideration.
class LiftTraversal {
public:
	LiftTraversal(Optimizer &optimizer, column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
	              unique_ptr<LogicalOperator> &root)
	    : optimizer(optimizer), statistics_map(statistics_map), root(root) {
	}

	void Optimize();

private:
	unique_ptr<LogicalOperator> &GetOperator(idx_t depth);
	void Apply(LiftPlacementGroup &group);
	bool InvalidateInputs(LogicalOperator &op, const column_binding_set_t &bindings);

	Optimizer &optimizer;
	column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map;
	unique_ptr<LogicalOperator> &root;
	vector<PathStep> ancestors;
	reference_set_t<LogicalOperator> completed;
};

unique_ptr<LogicalOperator> &LiftTraversal::GetOperator(idx_t depth) {
	if (depth == 0) {
		return root;
	}
	auto &parent = ancestors[depth - 1];
	return parent.op.get().children[parent.child_index];
}

bool LiftTraversal::InvalidateInputs(LogicalOperator &op, const column_binding_set_t &bindings) {
	if (bindings.empty()) {
		return false;
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		column_binding_set_t inputs;
		bool affected = false;
		for (auto &binding : bindings) {
			if (binding.table_index != projection.table_index) {
				continue;
			}
			affected = true;
			auto &expression = projection.GetExpression(binding);
			if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				auto &colref = expression.Cast<BoundColumnRefExpression>();
				if (colref.Depth() == 0) {
					inputs.insert(colref.Binding());
				}
			}
		}
		if (!affected) {
			return false;
		}
		completed.erase(op);
		// Follow aliases to their producers. A computed producer is reconsidered here; its own inputs
		// only gain an opportunity if that expression subsequently moves.
		InvalidateInputs(*op.children[0], inputs);
		return true;
	}
	if (op.type != LogicalOperatorType::LOGICAL_FILTER && !IsEligibleJoin(op)) {
		return false;
	}
	bool affected = false;
	for (auto &child : op.children) {
		column_binding_set_t inputs;
		for (auto &binding : child->GetColumnBindings()) {
			if (bindings.find(binding) != bindings.end()) {
				inputs.insert(binding);
			}
		}
		affected |= InvalidateInputs(*child, inputs);
	}
	if (affected) {
		completed.erase(op);
	}
	return affected;
}

void LiftTraversal::Apply(LiftPlacementGroup &group) {
	const auto target_depth = ancestors.size() - group.path.size();
	struct AncestorRewrite {
		idx_t depth;
		vector<ColumnBinding> child_bindings;
	};
	vector<AncestorRewrite> rewrites;
	column_binding_set_t affected_bindings(group.target_bindings.begin(), group.target_bindings.end());
	for (idx_t depth = target_depth; depth > 0; depth--) {
		auto &ancestor = ancestors[depth - 1];
		auto child_bindings = ancestor.op.get().children[ancestor.child_index]->GetColumnBindings();
		bool affected = false;
		for (auto &binding : child_bindings) {
			if (affected_bindings.find(binding) != affected_bindings.end()) {
				affected = true;
				break;
			}
		}
		if (!affected) {
			break;
		}
		rewrites.push_back({depth - 1, std::move(child_bindings)});
	}
	// No completion entry may outlive the operator it names, including when an allocator reuses its address.
	completed.erase(*GetOperator(ancestors.size()));
	for (auto &step : group.path) {
		completed.erase(step.op);
	}
	auto &target = GetOperator(target_depth);
	auto result = RewriteLiftPlacementGroup(optimizer, statistics_map, target, group);
	for (auto &rewrite : rewrites) {
		auto &parent = GetOperator(rewrite.depth);
		ColumnBindingRewrite::ApplyToChild(parent, ancestors[rewrite.depth].child_index,
		                                   std::move(rewrite.child_bindings), result.output_replacements);
	}
	// The rewrite supplies the complete replacement path. Traversal does not depend on the shape or
	// number of inserted operators, and child indexes preserve progress through unaffected siblings.
	ancestors.erase(ancestors.begin() + NumericCast<vector<PathStep>::difference_type>(target_depth), ancestors.end());
	ancestors.insert(ancestors.end(), result.path_to_source.begin(), result.path_to_source.end());
	column_binding_set_t inputs;
	for (auto &dependency : group.dependencies) {
		inputs.insert(dependency.original_binding);
	}
	InvalidateInputs(*GetOperator(ancestors.size())->children[0], inputs);
}

void LiftTraversal::Optimize() {
	root->ResolveOperatorTypes();
	bool visit_children = true;
	while (true) {
		auto &op = GetOperator(ancestors.size());
		if (completed.find(*op) == completed.end()) {
			if (visit_children && !op->children.empty()) {
				ancestors.push_back({*op, 0});
				continue;
			}
			if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				auto group = FindLiftPlacementGroup(op->Cast<LogicalProjection>(), ancestors, statistics_map);
				if (!group.expressions.empty()) {
					Apply(group);
					// Revisit exposed input producers and the rebuilt source, then continue through the
					// updated path. Completion entries skip unrelated descendants and sibling subtrees.
					visit_children = true;
					continue;
				}
			}
			completed.insert(*op);
		}
		if (ancestors.empty()) {
			return;
		}
		auto &parent = ancestors.back();
		if (++parent.child_index < parent.op.get().children.size()) {
			visit_children = true;
		} else {
			ancestors.pop_back();
			visit_children = false;
		}
	}
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
                          const column_binding_map_t<idx_t> &reading_expressions, ExpressionPlacement &result) {
	auto &expression = *projection.expressions[expression_index];
	if (!CanMoveExpression(expression) || !projection.children[0]->has_estimated_cardinality) {
		return false;
	}

	idx_t input_width;
	vector<ExpressionBindingWidth> input_bindings;
	if (!GetExpressionWidth(expression, statistics_map, input_width, input_bindings)) {
		return false;
	}
	auto exclusive_input_width = GetExclusiveInputWidth(reading_expressions, input_bindings);
	if (exclusive_input_width == 0 && ExpressionHeuristics::Cost(expression) <= MAXIMUM_CHEAP_EXPRESSION_COST) {
		// the other expressions of the projection still read every input, so moving a cheap expression only adds
		// a column that the joins below have to carry
		return false;
	}
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
	reference<unique_ptr<LogicalOperator>> current = projection.children[0];
	vector<PathStep> path;
	double transport_cardinality = 0;
	double input_transport_savings_cardinality = 0;
	// Beyond the first direct join, the source can be live for another path expression or join condition.
	bool can_credit_input_transport = true;
	vector<PlacementCandidate> candidates;

	// Compare every position on the dependency path instead of making a greedy per-join decision.
	while (current.get()) {
		auto &op = *current.get();
		if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			if (op.children.size() != 1 || !BindingsBelongToChild(*current_expression, op)) {
				break;
			}
			path.push_back({op, 0});
			can_credit_input_transport = false;
			current = op.children[0];
			continue;
		}
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &lower_projection = op.Cast<LogicalProjection>();
			if (!RewriteThroughProjection(current_expression, lower_projection)) {
				break;
			}
			path.push_back({op, 0});
			current = op.children[0];
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
		column_binding_set_t dependency_bindings;
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *current_expression, [&](const BoundColumnRefExpression &colref) {
			    if (colref.Depth() == 0) {
				    dependency_bindings.insert(colref.Binding());
			    }
		    });
		if (OperatorReferencesAnyBinding(op, dependency_bindings)) {
			can_credit_input_transport = false;
		}
		path.push_back({op, child_index.GetIndex()});
		transport_cardinality += static_cast<double>(op.estimated_cardinality);
		if (can_credit_input_transport) {
			input_transport_savings_cardinality += static_cast<double>(op.estimated_cardinality);
		}
		can_credit_input_transport = false;
		PlacementCandidate candidate;
		candidate.path_size = path.size();
		candidate.expression = current_expression->Copy();
		candidate.evaluation_cardinality = static_cast<double>(child->estimated_cardinality);
		candidate.output_transport_cardinality = transport_cardinality;
		candidate.input_transport_savings_cardinality = input_transport_savings_cardinality;
		candidates.push_back(std::move(candidate));
		current = child;
	}

	const double current_cost =
	    current_evaluation_cost + input_transport_savings_cardinality * static_cast<double>(exclusive_input_width);
	double best_cost = current_cost * MINIMUM_COST_RATIO;
	optional_ptr<PlacementCandidate> best_candidate;
	for (auto &candidate : candidates) {
		const double remaining_input_transport =
		    input_transport_savings_cardinality - candidate.input_transport_savings_cardinality;
		const double candidate_cost = candidate.evaluation_cardinality * evaluation_width +
		                              candidate.output_transport_cardinality * static_cast<double>(output_width) +
		                              remaining_input_transport * static_cast<double>(exclusive_input_width);
		if (candidate_cost < best_cost) {
			best_cost = candidate_cost;
			best_candidate = candidate;
		}
	}
	if (!best_candidate) {
		return false;
	}
	// Materialize only the selected prefix, without retaining capacity for the rest of the explored path.
	result.expression_index = expression_index;
	auto path_end = path.begin() + NumericCast<vector<PathStep>::difference_type>(best_candidate->path_size);
	result.path.assign(path.begin(), path_end);
	result.expression = std::move(best_candidate->expression);
	return true;
}

static void ExposeBinding(PathStep &step, const ColumnBinding &binding) {
	ExposeBinding(step.op.get(), step.child_index, binding);
}

static void ApplyPlacementGroup(Optimizer &optimizer, column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                                LogicalProjection &upper_projection, PlacementGroup &group) {
	auto old_upper_child_bindings = upper_projection.children[0]->GetColumnBindings();
	vector<vector<ColumnBinding>> old_path_child_bindings;
	old_path_child_bindings.reserve(group.path.size());
	for (auto &step : group.path) {
		old_path_child_bindings.push_back(step.op.get().children[step.child_index]->GetColumnBindings());
	}

	auto &target = GetPathTarget(group.path);
	auto old_bindings = target->GetColumnBindings();
	auto lower_index = optimizer.binder.GenerateTableIndex();
	auto lower_projection = LogicalProjection::CreateIdentity(lower_index, std::move(target));
	for (auto &placement : group.placements) {
		lower_projection->expressions.push_back(std::move(placement.expression));
	}
	if (lower_projection->children[0]->has_estimated_cardinality) {
		lower_projection->SetEstimatedCardinality(lower_projection->children[0]->estimated_cardinality);
	}
	target = std::move(lower_projection);

	BindingReplacementGraph replacements;
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		auto new_binding = ColumnBinding(lower_index, ProjectionIndex(i));
		replacements.Add(old_bindings[i], new_binding);
		CopyStatistics(statistics_map, old_bindings[i], new_binding);
	}

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
		auto &step_op = GetPathOperator(upper_projection.children[0], group.path, path_idx - 1);
		ColumnBindingRewrite::ApplyToChild(step_op, step.child_index, std::move(old_path_child_bindings[path_idx - 1]),
		                                   replacements);
		if (step.op.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = step.op.get().Cast<LogicalProjection>();
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
	ColumnBindingRewrite::ApplyToOperatorBindings(upper_projection, replacements);
	ColumnBindingRewrite::ValidateOutput(old_upper_child_bindings, upper_projection.children[0]->GetColumnBindings(),
	                                     replacements);

	for (idx_t i = 0; i < group.placements.size(); i++) {
		auto expression_index = group.placements[i].expression_index;
		auto alias = upper_projection.expressions[expression_index]->GetAlias();
		auto replacement = make_uniq<BoundColumnRefExpression>(
		    std::move(alias), upper_projection.expressions[expression_index]->GetReturnType(), computed_bindings[i]);
		upper_projection.expressions[expression_index] = std::move(replacement);
	}
}

static void LowerProfitableExpressions(Optimizer &optimizer,
                                       column_binding_map_t<unique_ptr<BaseStatistics>> &statistics_map,
                                       unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		LowerProfitableExpressions(optimizer, statistics_map, child);
	}
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &projection = op->Cast<LogicalProjection>();
	while (true) {
		PlacementGroup group;
		auto reading_expressions = CountReadingExpressions(projection);
		for (idx_t expression_index = 0; expression_index < projection.expressions.size(); expression_index++) {
			ExpressionPlacement placement;
			if (!FindPlacement(projection, expression_index, statistics_map, reading_expressions, placement)) {
				continue;
			}
			if (group.placements.empty()) {
				group.path = placement.path;
			}
			if (RefersToSameObject(*GetPathTarget(group.path), *GetPathTarget(placement.path))) {
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
	LiftTraversal(optimizer, statistics_map, plan).Optimize();
	LowerProfitableExpressions(optimizer, statistics_map, plan);
	plan->ResolveOperatorTypes();
}

} // namespace duckdb
