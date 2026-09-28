#include "duckdb/cascade/local_aggregate.hpp"

#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

namespace {

//! Point every reference at the binding its rewrite moved the column to.
using BindingMap = vector<std::pair<ColumnBinding, ColumnBinding>>;

void RemapBindings(unique_ptr<Expression> &expr, const BindingMap &map) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
		    for (auto &entry : map) {
			    if (colref.Binding() == entry.first) {
				    colref.BindingMutable() = entry.second;
				    return;
			    }
		    }
	    });
}

//! The aggregates that split into a local and a global part:
//!  * `sum`, `min` and `max` combine by themselves: f over a partition of the input,
//!    combined by f again, is f over the whole input;
//!  * `count` combines by a *sum* of the local counts, whose type is wider, so its two
//!    stages need a projection above them to put the column back;
//!  * `avg` has no local/global pair at all - the paper's footnote says it has to be
//!    decomposed into sum and count first - and is put back together by a division.
//! DISTINCT and order-sensitive aggregates do not split by rows at all.
static bool IsSplittable(const BoundAggregateExpression &aggregate) {
	if (aggregate.IsDistinct() || aggregate.GetFilter() || aggregate.GetOrderBys()) {
		return false;
	}
	auto &name = aggregate.Function().GetName();
	return name == "sum" || name == "min" || name == "max" || name == "count" || name == "count_star" ||
	       name == "avg";
}

//! Whether the expression reads only columns of the relation being aggregated, i.e.
//! whether it can be evaluated below the join at all.
static bool ReadsOnlySide(const Expression &expr, const vector<ColumnBinding> &side) {
	bool only = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    for (auto &binding : side) {
			    if (binding == colref.Binding()) {
				    return;
			    }
		    }
		    only = false;
	    });
	return only;
}

static bool HasColumnReference(const Expression &expr) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &) { found = true; });
	return found;
}

static bool IsPlainColumnOf(const Expression &expr, const vector<ColumnBinding> &side, ColumnBinding &binding) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	for (auto &candidate : side) {
		if (candidate == colref.Binding()) {
			binding = candidate;
			return true;
		}
	}
	return false;
}

//! The paper's `a x count(*)` rewrite needs the whole argument to come from the other side
//! of the join, so that it is the same for every row a local group aggregates: the local
//! `count(*)` then says how many rows the value stands for, and the sum above the join
//! multiplies it back in.
//!
//! An argument that is an addition of a part from each side is *not* supported, and the
//! reason is worth writing down: `sum(o + i)` is not `sum(o) + sum(i)`. A NULL in either
//! part makes the whole pair NULL, and SQL's `sum` ignores NULLs, so the pair contributes
//! nothing - while the two separate sums still count the non-NULL half. With
//! `ts = {(1,10),(1,10),(1,NULL)}` and `tr = {(1,100),(1,200)}` the joined `sum(ts.b +
//! tr.v)` is 640 where `sum(ts.b) + sum(tr.v)` is 940. Only the whole-argument form is an
//! identity.
bool IsOuterOnlyArgument(const Expression &argument, const vector<ColumnBinding> &side_bindings,
                         const vector<ColumnBinding> &other_bindings) {
	return !ReadsOnlySide(argument, side_bindings) && ReadsOnlySide(argument, other_bindings);
}

//! The plain case: the side can evaluate the function's argument itself, so the local
//! aggregate needs no multiplicity arithmetic at all.
bool CanLocalizeAggregatePlainly(const BoundAggregateExpression &aggregate, const vector<ColumnBinding> &side) {
	if (!IsSplittable(aggregate)) {
		return false;
	}
	auto &name = aggregate.Function().GetName();
	if (name == "count_star") {
		// Reads nothing, so either side can aggregate it.
		return true;
	}
	auto &arguments = aggregate.GetChildren();
	if (arguments.size() != 1) {
		return false;
	}
	return ReadsOnlySide(*arguments[0], side);
}

//! Whether this aggregate can be computed with a local aggregate over `side`. The plain
//! case is an argument the side can evaluate itself; the rest is the multiplicity rewrite,
//! which only the functions that can add the outer value back in support - `sum` through
//! `count(*)`, and `count`/`avg` through the same count read as a NULL indicator. `min` and
//! `max` cannot borrow a value from the other side at all.
bool CanLocalizeAggregate(const BoundAggregateExpression &aggregate, const vector<ColumnBinding> &side,
                          const vector<ColumnBinding> &other) {
	if (CanLocalizeAggregatePlainly(aggregate, side)) {
		return true;
	}
	// The rewrite has to re-check this: DISTINCT, a FILTER or an ORDER BY makes the
	// aggregate not splittable at all, and rewriting one of those as a weighted count
	// would silently drop the DISTINCT (`count(DISTINCT x)` came back as the row count).
	if (!IsSplittable(aggregate)) {
		return false;
	}
	auto &name = aggregate.Function().GetName();
	if (name != "sum" && name != "count" && name != "avg") {
		return false;
	}
	auto &arguments = aggregate.GetChildren();
	if (arguments.size() != 1) {
		return false;
	}
	return IsOuterOnlyArgument(*arguments[0], side, other);
}

void LocalAggRewriteBindings(LogicalOperator &op, const BindingMap &map) {
	for (auto &expr : op.expressions) {
		RemapBindings(expr, map);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			RemapBindings(condition.LeftReference(), map);
			RemapBindings(condition.RightReference(), map);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition) {
			RemapBindings(condition, map);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		for (auto &group : op.Cast<LogicalAggregate>().groups) {
			RemapBindings(group, map);
		}
		break;
	}
	default:
		break;
	}
}

bool LocalAggPassesBindingsThrough(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return true;
	default:
		return false;
	}
}

} // namespace

LocalAggregatePusher::LocalAggregatePusher(Binder &binder_p, ClientContext &context_p)
    : binder(binder_p), context(context_p) {
}

unique_ptr<LogicalOperator> LocalAggregatePusher::PushNode(
    unique_ptr<LogicalOperator> op, vector<std::pair<ColumnBinding, ColumnBinding>> &exports) {
	// A materialized CTE is DuckDB's common-subplan sharing: one definition, several
	// references that read it by position. A rewrite inside the definition would have to be
	// reflected in every reference's column list, which this pass does not do - the section
	// 3.1 pull-up used to crash there (`Failed to bind column reference ...`, TPC-DS q65).
	// So a CTE is a barrier, for every rule that renumbers bindings.
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
	    op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		return op;
	}
	for (auto &child : op->children) {
		vector<std::pair<ColumnBinding, ColumnBinding>> child_exports;
		child = PushNode(std::move(child), child_exports);
		if (child_exports.empty()) {
			continue;
		}
		LocalAggRewriteBindings(*op, child_exports);
		if (LocalAggPassesBindingsThrough(*op)) {
			for (auto &entry : child_exports) {
				exports.push_back(entry);
			}
		}
	}
	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return op;
	}
	auto &aggregate = op->Cast<LogicalAggregate>();
	if (aggregate.children.size() != 1) {
		return op;
	}
	if (aggregate.groups.empty()) {
		// A single-group aggregation has nothing to partition, and the projection that
		// count and avg need above it would have no group columns to pass through.
		return op;
	}
	if (aggregate.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return op;
	}
	auto &join = aggregate.children[0]->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::INNER || join.children.size() != 2) {
		return op;
	}

	// Which relation can be aggregated ahead of the join? Normally the one the aggregate
	// functions do not read, because an aggregate can only move below the join if all of its
	// input columns are available there. When an argument does read the other side, the
	// paper's rewrite makes it available anyway: the local `count(*)` carries how many rows
	// each local group stands for, and the sum above the join multiplies the other side's
	// part by it.
	// Two passes, and the order matters for cost. The first takes a side that can evaluate
	// every argument by itself: that is the side the functions read, so pre-aggregating it is
	// what reduces the rows the join sees, and its plan needs no multiplicity arithmetic.
	// Only when no side can do that does the second pass allow the rewrite that carries one
	// side's values through a `count(*)` - it makes shapes move that otherwise would not move
	// at all, rather than replacing a move that was already there.
	idx_t side_index = DConstants::INVALID_INDEX;
	for (idx_t pass = 0; pass < 2 && side_index == DConstants::INVALID_INDEX; pass++) {
		for (idx_t candidate = 0; candidate < 2; candidate++) {
			auto side_bindings = join.children[candidate]->GetColumnBindings();
			auto other_bindings = join.children[1 - candidate]->GetColumnBindings();
			bool usable = true;
			for (auto &expr : aggregate.expressions) {
				if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
					usable = false;
					break;
				}
				auto &bound = expr->Cast<BoundAggregateExpression>();
				bool can = pass == 0 ? CanLocalizeAggregatePlainly(bound, side_bindings)
				                     : CanLocalizeAggregate(bound, side_bindings, other_bindings);
				if (!can) {
					usable = false;
					break;
				}
			}
			if (usable) {
				side_index = candidate;
				break;
			}
		}
	}
	if (side_index == DConstants::INVALID_INDEX) {
		return op;
	}
	auto &side = *join.children[side_index];
	// Always recompute: a nested rewrite may have left this subtree's cached types in
	// place but out of date, and the local grouping columns are built from them.
	side.ResolveOperatorTypes();
	auto side_bindings = side.GetColumnBindings();
	auto other_bindings = join.children[1 - side_index]->GetColumnBindings();
	if (side.types.size() != side_bindings.size()) {
		return op;
	}
	auto side_type = [&](const ColumnBinding &binding, LogicalType &type) {
		for (idx_t i = 0; i < side_bindings.size(); i++) {
			if (side_bindings[i] == binding) {
				type = side.types[i];
				return true;
			}
		}
		return false;
	};

	// Everything below is built into local containers first and only spliced into the
	// plan once every check has passed - a rewrite that gives up halfway through
	// leaves a plan with moved-from expressions, which is a crash much later.
	auto local_group_index = binder.GenerateTableIndex();
	auto local_aggregate_index = binder.GenerateTableIndex();
	auto local_binding = [&](idx_t position) {
		return ColumnBinding(local_group_index, ProjectionIndex(position));
	};

	vector<unique_ptr<Expression>> local_groups;
	BindingMap column_map;
	auto expose_column = [&](const ColumnBinding &binding, const LogicalType &type) {
		for (auto &entry : column_map) {
			if (entry.first == binding) {
				return entry.second;
			}
		}
		local_groups.push_back(make_uniq<BoundColumnRefExpression>(type, binding));
		column_map.emplace_back(binding, local_binding(local_groups.size() - 1));
		return column_map.back().second;
	};
	auto expose_side_columns = [&](const Expression &expr) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    expr, [&](const BoundColumnRefExpression &colref) {
			    LogicalType type;
			    if (side_type(colref.Binding(), type)) {
				    expose_column(colref.Binding(), type);
			    }
		    });
	};

	// The grouping columns of the local aggregate. A grouping expression of the
	// global aggregate that only reads the aggregated relation is computed below the
	// join and referenced above it; every other column of that relation which the
	// global grouping or the join predicate reads has to be exposed as well, or the
	// expressions above the join could not be rebuilt.
	vector<idx_t> group_local_position(aggregate.groups.size(), DConstants::INVALID_INDEX);
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		auto &group = *aggregate.groups[i];
		if (!HasColumnReference(group) || !ReadsOnlySide(group, side_bindings)) {
			continue;
		}
		ColumnBinding plain;
		LogicalType plain_type;
		if (IsPlainColumnOf(group, side_bindings, plain) && side_type(plain, plain_type)) {
			// expose_column returns where the column now lives, which is the position
			// of the local grouping column whether it was already there or just added.
			group_local_position[i] = expose_column(plain, plain_type).column_index.GetIndexUnsafe();
			continue;
		}
		group_local_position[i] = local_groups.size();
		local_groups.push_back(group.Copy());
	}
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		if (group_local_position[i] == DConstants::INVALID_INDEX) {
			expose_side_columns(*aggregate.groups[i]);
		}
	}
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			return op;
		}
		expose_side_columns(condition.GetLHS());
		expose_side_columns(condition.GetRHS());
	}

	// The local and global halves of every aggregate function.
	vector<unique_ptr<Expression>> local_aggregates;
	vector<unique_ptr<Expression>> global_aggregates;
	// What the parent was reading, one entry per original expression, for when a
	// projection above the global aggregation is needed. A function whose two stages
	// produce it directly is passed through with a plain reference, so that an entry's
	// position always matches the position of the expression it stands for.
	vector<unique_ptr<Expression>> final_values;
	bool needs_fixup = false;
	// How many arguments the `count(*)` multiplicity carried, which is what the printed plan
	// says when it shows the rewrite instead of the plain split.
	idx_t multiplicity_rewrites = 0;
	FunctionBinder function_binder(context);
	auto bind_local = [&](const Identifier &function_name, vector<unique_ptr<Expression>> arguments,
	                      vector<LogicalType> argument_types) {
		auto function = GetBuiltinAggregateFunction(context, function_name, argument_types);
		local_aggregates.push_back(function_binder.BindAggregateFunction(std::move(function), std::move(arguments),
		                                                                nullptr, AggregateType::NON_DISTINCT));
		return local_aggregates.size() - 1;
	};
	auto local_ref = [&](idx_t position) {
		return make_uniq<BoundColumnRefExpression>(local_aggregates[position]->GetReturnType(),
		                                          ColumnBinding(local_aggregate_index, ProjectionIndex(position)));
	};
	auto bind_global_sum = [&](idx_t position) {
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(local_ref(position));
		vector<LogicalType> argument_types {local_aggregates[position]->GetReturnType()};
		auto function = GetBuiltinAggregateFunction(context, Identifier("sum"), argument_types);
		global_aggregates.push_back(function_binder.BindAggregateFunction(std::move(function), std::move(arguments),
		                                                                 nullptr, AggregateType::NON_DISTINCT));
		return global_aggregates.size() - 1;
	};
	auto global_ref = [&](idx_t position) {
		return make_uniq<BoundColumnRefExpression>(global_aggregates[position]->GetReturnType(),
		                                          ColumnBinding(aggregate.aggregate_index, ProjectionIndex(position)));
	};
	// The multiplicity of a local group, which is what lets a function borrow a value from
	// the other side of the join: the join above produces one row per local group per
	// matching outer row, while that local group stood for `count(*)` rows of the relation
	// being aggregated - the paper's `a * count(*)`.
	auto bind_local_multiplicity = [&]() {
		vector<unique_ptr<Expression>> no_arguments;
		vector<LogicalType> no_types;
		return bind_local("count", std::move(no_arguments), no_types);
	};
	auto multiply_by_multiplicity = [&](idx_t position, unique_ptr<Expression> value) {
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(std::move(value));
		arguments.push_back(local_ref(position));
		return BindBuiltinScalarFunction(context, Identifier("*"), std::move(arguments));
	};
	// `count(e)` counts the rows where `e` is not NULL, so with the multiplicity it is the
	// indicator of that test weighted by how many rows each of them stands for.
	auto not_null_indicator = [&](unique_ptr<Expression> value) {
		auto is_not_null =
		    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		is_not_null->GetChildrenMutable().push_back(std::move(value));
		return BoundCastExpression::AddCastToType(context, std::move(is_not_null), LogicalType::BIGINT);
	};
	auto bind_global = [&](const Identifier &function_name, unique_ptr<Expression> argument) {
		vector<LogicalType> argument_types {argument->GetReturnType()};
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(std::move(argument));
		auto function = GetBuiltinAggregateFunction(context, function_name, argument_types);
		global_aggregates.push_back(function_binder.BindAggregateFunction(std::move(function), std::move(arguments),
		                                                                 nullptr, AggregateType::NON_DISTINCT));
		return global_aggregates.size() - 1;
	};
	// Two stages that do not come back as the column the parent reads need the projection
	// above the aggregation; one that does is a plain pass-through.
	auto restore_type = [&](unique_ptr<Expression> value, const LogicalType &type) {
		if (value->GetReturnType() == type) {
			return value;
		}
		needs_fixup = true;
		return BoundCastExpression::AddCastToType(context, std::move(value), type);
	};

	for (auto &expr : aggregate.expressions) {
		auto &original = expr->Cast<BoundAggregateExpression>();
		auto &name = original.Function().GetName();
		auto &original_arguments = original.GetChildrenMutable();
		vector<LogicalType> argument_types;
		for (auto &argument : original_arguments) {
			argument_types.push_back(argument->GetReturnType());
		}

		// The multiplicity rewrite: a function whose argument only the other side can
		// evaluate still works, because the local `count(*)` carries how many rows each
		// local group stands for.
		bool outer_only = false;
		if (original_arguments.size() == 1 && (name == "sum" || name == "count" || name == "avg")) {
			outer_only = IsOuterOnlyArgument(*original_arguments[0], side_bindings, other_bindings);
		}
		bool handled = false;
		if (outer_only) {
			try {
				if (name == "avg") {
					// avg(a) = sum(a * count(*)) / sum(count(*) * (a IS NOT NULL)): both stages
					// read the same local count, and the division puts them back together.
					auto count_position = bind_local_multiplicity();
					auto numerator =
					    bind_global("sum", multiply_by_multiplicity(count_position, original_arguments[0]->Copy()));
					auto denominator = bind_global(
					    "sum", multiply_by_multiplicity(count_position, not_null_indicator(original_arguments[0]->Copy())));
					vector<unique_ptr<Expression>> division_arguments;
					division_arguments.push_back(global_ref(numerator));
					division_arguments.push_back(global_ref(denominator));
					auto division =
					    BindBuiltinScalarFunction(context, Identifier("/"), std::move(division_arguments));
					if (division->GetReturnType() != original.GetReturnType()) {
						return op;
					}
					needs_fixup = true;
					final_values.push_back(std::move(division));
					multiplicity_rewrites++;
					handled = true;
				} else if (name == "count") {
					// count(a) = sum(count(*) * (a IS NOT NULL)), which is a sum of counts and
					// therefore comes back wider than a count.
					auto count_position = bind_local_multiplicity();
					auto global_position = bind_global(
					    "sum", multiply_by_multiplicity(count_position, not_null_indicator(original_arguments[0]->Copy())));
					final_values.push_back(restore_type(global_ref(global_position), original.GetReturnType()));
					multiplicity_rewrites++;
					handled = true;
				} else if (name == "sum") {
					// sum(a) = sum(a * count(*)): the local aggregate counts the rows it
					// collapsed, and the sum above the join multiplies the value back in.
					auto count_position = bind_local_multiplicity();
					auto global_position = bind_global(
					    "sum", multiply_by_multiplicity(count_position, original_arguments[0]->Copy()));
					final_values.push_back(restore_type(global_ref(global_position), original.GetReturnType()));
					multiplicity_rewrites++;
					handled = true;
				}
			} catch (std::exception &) {
				// A value whose types do not combine is not a reason to fail the query: leave
				// the aggregation where it is. Nothing has been spliced into the plan yet, so
				// returning is safe.
				return op;
			}
		}
		if (handled) {
			continue;
		}

		if (name == "avg") {
			// avg(x) = sum(x) / count(x): two local aggregates, two global ones, and the
			// division that puts them back together above the global aggregation.
			needs_fixup = true;
			vector<LogicalType> sum_types {argument_types[0]};
			vector<unique_ptr<Expression>> sum_arguments;
			sum_arguments.push_back(original_arguments[0]->Copy());
			vector<unique_ptr<Expression>> count_arguments;
			count_arguments.push_back(original_arguments[0]->Copy());
			auto sum_position = bind_local("sum", std::move(sum_arguments), sum_types);
			auto count_position = bind_local("count", std::move(count_arguments), sum_types);
			auto global_sum = bind_global_sum(sum_position);
			auto global_count = bind_global_sum(count_position);
			vector<unique_ptr<Expression>> division_arguments;
			division_arguments.push_back(global_ref(global_sum));
			division_arguments.push_back(global_ref(global_count));
			auto division = BindBuiltinScalarFunction(context, Identifier("/"), std::move(division_arguments));
			if (division->GetReturnType() != original.GetReturnType()) {
				// Two stages that do not add up to the column the parent reads would
				// change the plan's shape, so this aggregate stays where it is.
				return op;
			}
			final_values.push_back(std::move(division));
			continue;
		}
		if (name == "count" || name == "count_star") {
			// A count over a partition of the input is the sum of the partial counts,
			// which is wider than a count, so the column is cast back to what the parent
			// was reading.
			needs_fixup = true;
			vector<unique_ptr<Expression>> count_arguments;
			for (auto &argument : original_arguments) {
				count_arguments.push_back(argument->Copy());
			}
			auto position = bind_local("count", std::move(count_arguments), argument_types);
			auto global_position = bind_global_sum(position);
			auto cast = BoundCastExpression::AddCastToType(context, global_ref(global_position),
			                                              original.GetReturnType());
			final_values.push_back(std::move(cast));
			continue;
		}
		// sum / min / max: local and global are the same function.
		vector<unique_ptr<Expression>> local_arguments;
		for (auto &argument : original_arguments) {
			local_arguments.push_back(argument->Copy());
		}
		auto position = bind_local(name, std::move(local_arguments), argument_types);
		vector<unique_ptr<Expression>> global_arguments;
		global_arguments.push_back(local_ref(position));
		vector<LogicalType> global_types {local_aggregates[position]->GetReturnType()};
		auto global_function = GetBuiltinAggregateFunction(context, Identifier(name), global_types);
		global_aggregates.push_back(function_binder.BindAggregateFunction(std::move(global_function),
		                                                                 std::move(global_arguments), nullptr,
		                                                                 AggregateType::NON_DISTINCT));
		auto global_position = global_aggregates.size() - 1;
		if (global_aggregates[global_position]->GetReturnType() != original.GetReturnType()) {
			return op;
		}
		final_values.push_back(global_ref(global_position));
	}

	// The global grouping: the local parts are referenced, everything else keeps its
	// shape with the aggregated side's columns repointed at the local grouping.
	vector<unique_ptr<Expression>> global_groups;
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		auto &group = aggregate.groups[i];
		if (group_local_position[i] != DConstants::INVALID_INDEX) {
			global_groups.push_back(
			    make_uniq<BoundColumnRefExpression>(group->GetReturnType(), local_binding(group_local_position[i])));
			continue;
		}
		auto copy = group->Copy();
		RemapBindings(copy, column_map);
		global_groups.push_back(std::move(copy));
	}

	// Splice: the join keeps both relations, but the aggregated side becomes the local
	// aggregate over it, and the predicate is repointed at the local grouping columns.
	if (multiplicity_rewrites > 0 && CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade: section 3.3 carried " + std::to_string(multiplicity_rewrites) +
		               " aggregate argument(s) through the local count(*) (the paper's a x count(*))");
	}
	auto local = make_uniq<LogicalAggregate>(local_group_index, local_aggregate_index, std::move(local_aggregates));
	local->groups = std::move(local_groups);
	local->children.push_back(std::move(join.children[side_index]));
	join.children[side_index] = std::move(local);
	// The join's projection maps name positions in the child they were built for, and the
	// child that side just got exposes a different set of columns. Leaving a stale map in
	// place makes the type resolution read past the end of the new child (and, when it
	// does not, quietly hands the parent positions it did not ask for). Only the join's
	// own output changes - the aggregation above it names bindings - so dropping the map
	// is safe.
	if (side_index == 0) {
		join.left_projection_map.clear();
	} else {
		join.right_projection_map.clear();
	}
	for (auto &condition : join.conditions) {
		RemapBindings(condition.LeftReference(), column_map);
		RemapBindings(condition.RightReference(), column_map);
	}
	auto original_group_index = aggregate.group_index;
	auto original_aggregate_index = aggregate.aggregate_index;
	aggregate.groups = std::move(global_groups);
	aggregate.expressions = std::move(global_aggregates);

	if (!needs_fixup) {
		// Every function combined by itself, so the aggregation still exposes exactly
		// the columns it did before - nothing above it has to be repointed.
		return op;
	}
	// count and avg do not come back as the column the parent was reading, so a
	// projection above the global aggregation puts it back - one output per original
	// expression, so that an entry's position is the position it stands for. That
	// projection renames every binding it passes through, which is what the export
	// tells whoever read the aggregation.
	auto projection_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> select_list;
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		select_list.push_back(make_uniq<BoundColumnRefExpression>(
		    aggregate.groups[i]->GetReturnType(), ColumnBinding(original_group_index, ProjectionIndex(i))));
		exports.emplace_back(ColumnBinding(original_group_index, ProjectionIndex(i)),
		                     ColumnBinding(projection_index, ProjectionIndex(i)));
	}
	for (idx_t i = 0; i < final_values.size(); i++) {
		auto position = select_list.size();
		select_list.push_back(std::move(final_values[i]));
		exports.emplace_back(ColumnBinding(original_aggregate_index, ProjectionIndex(i)),
		                     ColumnBinding(projection_index, ProjectionIndex(position)));
	}
	auto fixup = make_uniq<LogicalProjection>(projection_index, std::move(select_list));
	fixup->children.push_back(std::move(op));
	return std::move(fixup);
}

unique_ptr<LogicalOperator> LocalAggregatePusher::Push(unique_ptr<LogicalOperator> plan) {
	vector<std::pair<ColumnBinding, ColumnBinding>> exports;
	return PushNode(std::move(plan), exports);
}

} // namespace duckdb
