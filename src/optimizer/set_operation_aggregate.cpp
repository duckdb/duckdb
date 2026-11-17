// EXCEPT ALL and INTERSECT ALL reach the optimizer as a LogicalSetOperation. Once the filters have been moved around it
// (FilterPullup / FilterPushdown: a filter on the result goes into the left child, what it implies for the other
// child goes there, see PushdownSetOperation and PullupSetOperation), this pass chooses how to run it.
//
// The general plan numbers the rows that are equal in all columns on both sides and joins on the columns and the
// number (LogicalSetOperation::CreateJoin): two windows that partition by every column, and a hash join on every
// column. But a row that occurs n times in l and m times in r occurs max(n - m, 0) times in l EXCEPT ALL r and
// min(n, m) times in l INTERSECT ALL r, so counting the rows of every group per side is enough:
//
//     PROJECTION <the columns>
//       UNNEST <list of block_size entries>         -- one row per copy
//         UNNEST <list of block_count entries>      -- one row per block of copies
//           FILTER n > m                            -- INTERSECT ALL: n > 0 AND m > 0
//             AGGREGATE count(in_l) AS n, count(in_r) AS m GROUP BY <all columns>
//               UNION ALL
//                 PROJECTION <the columns of l>, true AS in_l, NULL AS in_r
//                 PROJECTION <the columns of r>, NULL AS in_l, true AS in_r
//
// A group is emitted c = n - m times (INTERSECT ALL: c = min(n, m) times). Its copies are generated in blocks of
// COPY_BLOCK_SIZE rows, so that a group with very many copies never needs one list with an entry per copy: the
// first UNNEST yields a row per block, the second one the rows of the block. The lists come from list_resize of a
// constant list, which like every function used here is part of the core library; a group with a single copy, the
// common case, unnests the constant list itself in both steps.
//
// The aggregate returns one row of a group where the join returns the left rows, so it is only used if rows that
// compare equal are identical: no column is compared under a collation, none is floating point (0.0 and -0.0) or of
// a type whose comparison normalizes (INTERVAL, TIME WITH TIME ZONE), also not nested. Every other EXCEPT ALL /
// INTERSECT ALL becomes the join.
//
// The join is also kept where the left child is estimated to have few distinct rows with many copies each: the few
// groups of the aggregate cannot feed all threads, so the expansion and what follows it would run on one thread.
//
// The pass runs before the join order, statistics propagation and compressed materialization, which then see an
// ordinary aggregate or join.
#include "duckdb/optimizer/set_operation_aggregate.hpp"

#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"

namespace duckdb {

namespace {

//! The copies of a group are generated in blocks of this many rows
constexpr int64_t COPY_BLOCK_SIZE = STANDARD_VECTOR_SIZE;

//! The aggregate plan is not used for fewer groups than this per thread: the scan of the aggregate's result is the
//! source of everything above the set operation, and with few groups it cannot keep the threads busy
constexpr idx_t MIN_GROUPS_PER_THREAD = STANDARD_VECTOR_SIZE;
//! .. if every group has at least this many rows on average, so that there is work above to be spread
constexpr idx_t MIN_COPIES_PER_GROUP = 128;
//! How deep the estimate of the distinct values of a column follows the plan
constexpr idx_t MAX_DISTINCT_COUNT_DEPTH = 64;

optional_idx MultiplyCounts(optional_idx left, optional_idx right) {
	if (!left.IsValid() || !right.IsValid()) {
		return optional_idx();
	}
	const idx_t limit = NumericLimits<uint32_t>::Maximum();
	if (left.GetIndex() > limit || right.GetIndex() > limit) {
		// as good as unknown
		return optional_idx();
	}
	return left.GetIndex() * right.GetIndex();
}

optional_idx ColumnDistinctCount(ClientContext &context, LogicalOperator &op, const ColumnBinding &binding,
                                 idx_t depth);

//! An estimate of the number of distinct values of an expression over the columns of op, if there is one: a constant
//! has one value, a column what the scan it comes from says, and another expression at most as many as the
//! combinations of the columns it reads
optional_idx ExpressionDistinctCount(ClientContext &context, LogicalOperator &op, const Expression &expr, idx_t depth) {
	if (expr.IsFoldable()) {
		return 1;
	}
	if (expr.IsVolatile()) {
		return optional_idx();
	}
	optional_idx result = 1;
	bool has_column = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		has_column = true;
		result = colref.Depth() == 0
		             ? MultiplyCounts(result, ColumnDistinctCount(context, op, colref.Binding(), depth + 1))
		             : optional_idx();
	});
	if (!has_column) {
		return optional_idx();
	}
	// few values by type
	optional_idx domain;
	switch (expr.GetReturnType().id()) {
	case LogicalTypeId::BOOLEAN:
		domain = 3;
		break;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::UTINYINT:
		domain = 257;
		break;
	case LogicalTypeId::ENUM:
		domain = EnumType::GetSize(expr.GetReturnType()) + 1;
		break;
	default:
		break;
	}
	if (domain.IsValid() && (!result.IsValid() || result.GetIndex() > domain.GetIndex())) {
		return domain;
	}
	return result;
}

//! An estimate of the number of distinct values of a column that op produces, if there is one
optional_idx ColumnDistinctCount(ClientContext &context, LogicalOperator &op, const ColumnBinding &binding,
                                 idx_t depth) {
	if (depth > MAX_DISTINCT_COUNT_DEPTH) {
		return optional_idx();
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (binding.table_index != get.table_index || !get.children.empty()) {
			return optional_idx();
		}
		auto &column_index = get.GetColumnIndex(binding);
		if (column_index.IsVirtualColumn()) {
			return optional_idx();
		}
		unique_ptr<BaseStatistics> stats;
		if (get.bind_data && get.function.statistics_extended) {
			TableFunctionGetStatisticsInput input(get.bind_data.get(), column_index);
			stats = get.function.statistics_extended(context, input);
		} else if (get.bind_data && get.function.statistics) {
			stats = get.function.statistics(context, get.bind_data.get(), column_index.GetPrimaryIndex());
		}
		if (!stats || stats->GetDistinctCount() == 0) {
			return optional_idx();
		}
		// one more for NULL
		return stats->GetDistinctCount() + 1;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		if (binding.table_index != projection.table_index || binding.column_index >= op.expressions.size()) {
			return optional_idx();
		}
		return ExpressionDistinctCount(context, *op.children[0], *op.expressions[binding.column_index], depth);
	}
	case LogicalOperatorType::LOGICAL_UNION: {
		auto &setop = op.Cast<LogicalSetOperation>();
		if (binding.table_index != setop.table_index) {
			return optional_idx();
		}
		idx_t result = 0;
		for (auto &child : op.children) {
			auto child_bindings = child->GetColumnBindings();
			if (binding.column_index >= child_bindings.size()) {
				return optional_idx();
			}
			auto child_count = ColumnDistinctCount(context, *child, child_bindings[binding.column_index], depth + 1);
			if (!child_count.IsValid()) {
				return optional_idx();
			}
			result += child_count.GetIndex();
		}
		return result;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// the values of a column come from the child that has it (an outer join adds NULL, which is counted)
		for (auto &child : op.children) {
			for (auto &child_binding : child->GetColumnBindings()) {
				if (child_binding == binding) {
					return ColumnDistinctCount(context, *child, binding, depth + 1);
				}
			}
		}
		return optional_idx();
	}
	default:
		return optional_idx();
	}
}

} // namespace

SetOperationAggregate::SetOperationAggregate(Optimizer &optimizer, bool use_aggregate)
    : optimizer(optimizer), use_aggregate(use_aggregate) {
}

bool SetOperationAggregate::CanUseAggregate(LogicalSetOperation &setop) {
	if (!use_aggregate || !setop.setop_all) {
		return false;
	}
	for (auto &comparison : setop.GetComparisons(optimizer.context)) {
		// a column that is compared under a collation is not compared as a plain column
		if (comparison.GetLHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    comparison.GetRHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    comparison.GetLHS().GetReturnType() != comparison.GetRHS().GetReturnType() ||
		    !comparison.GetLHS().GetReturnType().EqualValuesAreIdentical()) {
			return false;
		}
	}
	return !FewGroupsOfManyRows(setop);
}

//! Whether the left child, whose rows the set operation returns, is estimated to have few distinct rows with many
//! copies each. The aggregate plan then expands a few groups into many rows, and since the scan of its few groups
//! is the source of that pipeline, the expansion and everything above it up to the next pipeline breaker run on
//! one thread, where the rows of the join plan come from all threads. Without an estimate the aggregate is used.
bool SetOperationAggregate::FewGroupsOfManyRows(LogicalSetOperation &setop) {
	auto &context = optimizer.context;
	auto threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	if (threads <= 1) {
		return false;
	}
	auto &left = *setop.children[0];
	optional_idx groups = 1;
	for (auto &binding : left.GetColumnBindings()) {
		groups = MultiplyCounts(groups, ColumnDistinctCount(context, left, binding, 0));
	}
	if (!groups.IsValid()) {
		return false;
	}
	auto rows = left.EstimateCardinality(context);
	return groups.GetIndex() < threads * MIN_GROUPS_PER_THREAD && groups.GetIndex() * MIN_COPIES_PER_GROUP <= rows;
}

//! The columns of a child followed by its side markers: in_left is TRUE for the rows of the left child and NULL for
//! the rows of the right one, in_right the reverse
unique_ptr<LogicalOperator> SetOperationAggregate::AddSideMarkers(unique_ptr<LogicalOperator> child, bool is_left) {
	child->ResolveOperatorTypes();
	auto bindings = child->GetColumnBindings();
	vector<unique_ptr<Expression>> select_list;
	for (idx_t i = 0; i < bindings.size(); i++) {
		select_list.push_back(make_uniq<BoundColumnRefExpression>(child->types[i], bindings[i]));
	}
	for (auto set : {is_left, !is_left}) {
		select_list.push_back(
		    make_uniq<BoundConstantExpression>(set ? Value::BOOLEAN(true) : Value(LogicalType::BOOLEAN)));
	}
	auto projection = make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(select_list));
	projection->AddChild(std::move(child));
	return std::move(projection);
}

//! The aggregate over the union of the children of an EXCEPT ALL / INTERSECT ALL
unique_ptr<LogicalOperator> SetOperationAggregate::CreateAggregate(LogicalSetOperation &setop) {
	auto &context = optimizer.context;
	auto &binder = optimizer.binder;
	bool is_except = setop.type == LogicalOperatorType::LOGICAL_EXCEPT;
	auto types = setop.GetComparisonTypes(context);
	auto left = AddSideMarkers(std::move(setop.children[0]), true);
	auto right = AddSideMarkers(std::move(setop.children[1]), false);

	auto column_count = types.size();
	auto union_index = binder.GenerateTableIndex();
	auto union_all = make_uniq<LogicalSetOperation>(union_index, column_count + 2, std::move(left), std::move(right),
	                                                LogicalOperatorType::LOGICAL_UNION, true);

	// count(in_left), count(in_right) GROUP BY all columns
	vector<unique_ptr<Expression>> counts;
	FunctionBinder function_binder(context);
	for (idx_t side = 0; side < 2; side++) {
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(make_uniq<BoundColumnRefExpression>(
		    LogicalType::BOOLEAN, ColumnBinding(union_index, ProjectionIndex(column_count + side))));
		auto function = GetBuiltinAggregateFunction(context, Identifier("count"), {LogicalType::BOOLEAN});
		counts.push_back(function_binder.BindAggregateFunction(function, std::move(arguments)));
	}
	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();
	auto aggregate = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(counts));
	aggregate->groupings_index = binder.GenerateTableIndex();
	GroupingSet every_column;
	for (idx_t i = 0; i < column_count; i++) {
		aggregate->groups.push_back(
		    make_uniq<BoundColumnRefExpression>(types[i], ColumnBinding(union_index, ProjectionIndex(i))));
		every_column.insert(ProjectionIndex(i));
	}
	aggregate->grouping_sets.push_back(std::move(every_column));
	aggregate->AddChild(std::move(union_all));

	auto left_count = [&]() -> unique_ptr<Expression> {
		return make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT,
		                                           ColumnBinding(aggregate_index, ProjectionIndex(0)));
	};
	auto right_count = [&]() -> unique_ptr<Expression> {
		return make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT,
		                                           ColumnBinding(aggregate_index, ProjectionIndex(1)));
	};
	auto bigint = [&](int64_t value) -> unique_ptr<Expression> {
		return make_uniq<BoundConstantExpression>(Value::BIGINT(value));
	};
	auto call = [&](const char *name, unique_ptr<Expression> first,
	                unique_ptr<Expression> second) -> unique_ptr<Expression> {
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(std::move(first));
		arguments.push_back(std::move(second));
		return BindBuiltinScalarExpression(context, Identifier(name), std::move(arguments));
	};

	// the groups that remain, and how often each of them is emitted
	auto filter = make_uniq<LogicalFilter>();
	unique_ptr<Expression> copies;
	if (is_except) {
		filter->expressions.push_back(
		    BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, left_count(), right_count()));
		copies = call("-", left_count(), right_count());
	} else {
		filter->expressions.push_back(
		    BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, left_count(), bigint(0)));
		filter->expressions.push_back(
		    BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, right_count(), bigint(0)));
		copies = make_uniq<BoundCaseExpression>(
		    BoundComparisonExpression::Create(ExpressionType::COMPARE_LESSTHAN, left_count(), right_count()),
		    left_count(), right_count());
	}
	filter->AddChild(std::move(aggregate));

	// one row per copy, in two steps so that no list holds more than COPY_BLOCK_SIZE entries per block of copies.
	// a group with a single block, or a single copy, unnests a constant list, which costs next to nothing
	auto list_of = [&](vector<Value> entries) -> unique_ptr<Expression> {
		return make_uniq<BoundConstantExpression>(Value::LIST(LogicalType::BOOLEAN, std::move(entries)));
	};
	auto unnest_of = [&](unique_ptr<Expression> list) -> unique_ptr<Expression> {
		auto result = make_uniq<BoundUnnestExpression>(LogicalType::BOOLEAN);
		result->ChildMutable() = std::move(list);
		return std::move(result);
	};
	auto compare = [&](ExpressionType type, unique_ptr<Expression> left, int64_t right) {
		return BoundComparisonExpression::Create(type, std::move(left), bigint(right));
	};
	// the blocks: one row per started block of copies, TRUE for the first block and NULL for the others
	auto block_count = call("+", call("//", call("-", copies->Copy(), bigint(1)), bigint(COPY_BLOCK_SIZE)), bigint(1));
	auto block_list = make_uniq<BoundCaseExpression>(
	    compare(ExpressionType::COMPARE_LESSTHANOREQUALTO, copies->Copy(), COPY_BLOCK_SIZE),
	    list_of({Value::BOOLEAN(true)}), call("list_resize", list_of({Value::BOOLEAN(true)}), std::move(block_count)));
	auto blocks_index = binder.GenerateTableIndex();
	auto blocks = make_uniq<LogicalUnnest>(blocks_index);
	blocks->expressions.push_back(unnest_of(std::move(block_list)));
	blocks->AddChild(std::move(filter));

	// a block: a list of COPY_BLOCK_SIZE entries, the first block holds the remainder
	auto first_block = call("+", call("%", call("-", copies->Copy(), bigint(1)), bigint(COPY_BLOCK_SIZE)), bigint(1));
	auto is_first_block =
	    make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, ColumnBinding(blocks_index, ProjectionIndex(0)));
	auto block_size =
	    make_uniq<BoundCaseExpression>(std::move(is_first_block), std::move(first_block), bigint(COPY_BLOCK_SIZE));
	auto copy_list = make_uniq<BoundCaseExpression>(compare(ExpressionType::COMPARE_EQUAL, std::move(copies), 1),
	                                                list_of({Value::BOOLEAN(true)}),
	                                                call("list_resize", list_of({}), std::move(block_size)));
	auto unnest = make_uniq<LogicalUnnest>(binder.GenerateTableIndex());
	unnest->expressions.push_back(unnest_of(std::move(copy_list)));
	unnest->AddChild(std::move(blocks));

	// the columns of the set operation, under its table index
	vector<unique_ptr<Expression>> select_list;
	for (idx_t i = 0; i < column_count; i++) {
		select_list.push_back(
		    make_uniq<BoundColumnRefExpression>(types[i], ColumnBinding(group_index, ProjectionIndex(i))));
	}
	auto projection = make_uniq<LogicalProjection>(setop.table_index, std::move(select_list));
	projection->AddChild(std::move(unnest));
	return std::move(projection);
}

unique_ptr<LogicalOperator> SetOperationAggregate::Optimize(unique_ptr<LogicalOperator> op) {
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	if (op->type != LogicalOperatorType::LOGICAL_EXCEPT && op->type != LogicalOperatorType::LOGICAL_INTERSECT) {
		return op;
	}
	auto &setop = op->Cast<LogicalSetOperation>();
	unique_ptr<LogicalOperator> result;
	if (CanUseAggregate(setop)) {
		result = CreateAggregate(setop);
	} else {
		auto types = setop.GetComparisonTypes(optimizer.context);
		result =
		    LogicalSetOperation::CreateJoin(optimizer.binder, types, setop.table_index, setop.type, setop.setop_all,
		                                    std::move(setop.children[0]), std::move(setop.children[1]));
	}
	result->ResolveOperatorTypes();
	return result;
}

} // namespace duckdb
