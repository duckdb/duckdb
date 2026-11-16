// `l EXCEPT ALL r` is planned as row_number() OVER (PARTITION BY <all columns>) on both inputs and an ANTI join on all
// columns and the row number (INTERSECT ALL: a SEMI join), see CreateSetOperationJoin in plan_setop.cpp: two windows
// that partition by every column, and a hash join on every column. A row that occurs n times in l and m times in r
// occurs max(n - m, 0) times in the result (min(n, m) times for INTERSECT ALL), so counting the rows of every group
// per side is enough:
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
// The rewrite runs after filter pushdown and matches the plan as the binder built it. By then a filter above the set
// operation and a filter inside one input have reached the other input through the join conditions, which a plan
// that never had the join could not do for them, and a set operation with a provably empty input is gone.
//
// What must hold:
// - a projection of plain left columns over a SEMI or ANTI comparison join whose children are windows with the single
//   expression row_number() OVER (PARTITION BY <every column of the child>);
// - the join conditions are IS NOT DISTINCT FROM between plain columns and pair every left column with one right
//   column, and the two row numbers with each other. A collated column is compared through its collation, not as a
//   plain column, so it does not match;
// - rows that compare equal are identical, because the aggregate yields one row of a group where the join yields the
//   left rows: no floating point column (0.0 and -0.0), no type whose comparison normalizes (INTERVAL, TIME WITH TIME
//   ZONE), also not nested.
#include "duckdb/optimizer/set_operation_aggregate.hpp"

#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

namespace {

//! The copies of a group are generated in blocks of this many rows
constexpr int64_t COPY_BLOCK_SIZE = STANDARD_VECTOR_SIZE;

//! Whether two values of the type that compare equal are the same value
bool EqualValuesAreIdentical(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::UUID:
	case LogicalTypeId::ENUM:
		return true;
	case LogicalTypeId::STRUCT:
		for (auto &child : StructType::GetChildTypes(type)) {
			if (!EqualValuesAreIdentical(child.second)) {
				return false;
			}
		}
		return true;
	case LogicalTypeId::LIST:
		return EqualValuesAreIdentical(ListType::GetChildType(type));
	case LogicalTypeId::ARRAY:
		return EqualValuesAreIdentical(ArrayType::GetChildType(type));
	default:
		return false;
	}
}

//! The binding of a plain column reference of this query
bool PlainColumn(const Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	if (colref.Depth() != 0) {
		return false;
	}
	binding = colref.Binding();
	return true;
}

optional_idx Position(const vector<ColumnBinding> &bindings, const ColumnBinding &binding) {
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (bindings[i] == binding) {
			return i;
		}
	}
	return optional_idx();
}

//! The binding of the row number, if op is a window with the single expression
//! row_number() OVER (PARTITION BY <every column of its child>)
bool AllColumnRowNumber(LogicalOperator &op, ColumnBinding &row_number_binding) {
	if (op.type != LogicalOperatorType::LOGICAL_WINDOW || op.children.size() != 1 || op.expressions.size() != 1) {
		return false;
	}
	auto &expr = *op.expressions[0];
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_WINDOW ||
	    expr.GetExpressionType() != ExpressionType::WINDOW_ROW_NUMBER) {
		return false;
	}
	auto &row_number = expr.Cast<BoundWindowExpression>();
	if (!row_number.OrderBy().empty() || !row_number.ArgOrders().empty() || row_number.Filter()) {
		return false;
	}
	auto child_bindings = op.children[0]->GetColumnBindings();
	if (row_number.Partitions().size() != child_bindings.size()) {
		return false;
	}
	vector<bool> partitioned(child_bindings.size(), false);
	for (auto &partition : row_number.Partitions()) {
		ColumnBinding binding;
		if (!PlainColumn(*partition, binding)) {
			return false;
		}
		auto position = Position(child_bindings, binding);
		if (!position.IsValid() || partitioned[position.GetIndex()]) {
			return false;
		}
		partitioned[position.GetIndex()] = true;
	}
	row_number_binding = ColumnBinding(op.Cast<LogicalWindow>().window_index, ProjectionIndex(0));
	return true;
}

} // namespace

SetOperationAggregate::SetOperationAggregate(Optimizer &optimizer) : optimizer(optimizer) {
}

//! The columns of the set operation in the order of the left input, if op is the projection over the join of the two
//! row number windows and the rows of a group are identical
bool SetOperationAggregate::Match(LogicalOperator &op, vector<SetOperationColumn> &columns, bool &is_except) const {
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION || op.children.size() != 1 ||
	    op.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = op.children[0]->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::ANTI && join.join_type != JoinType::SEMI) {
		return false;
	}
	is_except = join.join_type == JoinType::ANTI;
	if (join.HasProjectionMap() || !join.duplicate_eliminated_columns.empty() || join.filter_pushdown ||
	    join.children.size() != 2 || !join.expressions.empty()) {
		return false;
	}
	ColumnBinding left_row_number;
	ColumnBinding right_row_number;
	if (!AllColumnRowNumber(*join.children[0], left_row_number) ||
	    !AllColumnRowNumber(*join.children[1], right_row_number)) {
		return false;
	}
	auto left_bindings = join.children[0]->children[0]->GetColumnBindings();
	auto right_bindings = join.children[1]->children[0]->GetColumnBindings();
	if (left_bindings.size() != right_bindings.size() || join.conditions.size() != left_bindings.size() + 1) {
		return false;
	}

	// every left column is compared with one right column, and the row numbers with each other
	columns.clear();
	columns.resize(left_bindings.size());
	vector<bool> left_seen(left_bindings.size(), false);
	vector<bool> right_seen(right_bindings.size(), false);
	bool row_numbers_compared = false;
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			return false;
		}
		ColumnBinding left;
		ColumnBinding right;
		if (!PlainColumn(condition.GetLHS(), left) || !PlainColumn(condition.GetRHS(), right)) {
			return false;
		}
		if (left == left_row_number && right == right_row_number && !row_numbers_compared) {
			row_numbers_compared = true;
			continue;
		}
		auto left_position = Position(left_bindings, left);
		auto right_position = Position(right_bindings, right);
		if (!left_position.IsValid() || !right_position.IsValid()) {
			return false;
		}
		auto &type = condition.GetLHS().GetReturnType();
		if (left_seen[left_position.GetIndex()] || right_seen[right_position.GetIndex()] ||
		    type != condition.GetRHS().GetReturnType() || !EqualValuesAreIdentical(type)) {
			return false;
		}
		left_seen[left_position.GetIndex()] = true;
		right_seen[right_position.GetIndex()] = true;
		columns[left_position.GetIndex()] = SetOperationColumn {type, left, right};
	}
	if (!row_numbers_compared) {
		return false;
	}
	// with one condition per left column, none compared twice, and the row numbers, every column is compared

	// the projection reads columns of the left input only, never the row number
	for (auto &expr : op.expressions) {
		ColumnBinding binding;
		if (!PlainColumn(*expr, binding) || !Position(left_bindings, binding).IsValid()) {
			return false;
		}
	}
	return true;
}

//! The columns of an input followed by its side markers: in_left is TRUE for the rows of the left input and NULL for
//! the rows of the right one, in_right the reverse
unique_ptr<LogicalOperator> SetOperationAggregate::AddSideMarkers(unique_ptr<LogicalOperator> child,
                                                                  const vector<SetOperationColumn> &columns,
                                                                  bool is_left) {
	vector<unique_ptr<Expression>> select_list;
	for (auto &column : columns) {
		select_list.push_back(make_uniq<BoundColumnRefExpression>(column.type, is_left ? column.left : column.right));
	}
	for (auto set : {is_left, !is_left}) {
		select_list.push_back(
		    make_uniq<BoundConstantExpression>(set ? Value::BOOLEAN(true) : Value(LogicalType::BOOLEAN)));
	}
	auto projection = make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(select_list));
	projection->AddChild(std::move(child));
	return std::move(projection);
}

//! Replaces the join below the projection op by the aggregate over the union of the inputs of the two windows
void SetOperationAggregate::Rewrite(LogicalOperator &op, const vector<SetOperationColumn> &columns, bool is_except) {
	auto &context = optimizer.context;
	auto &binder = optimizer.binder;
	auto &join = *op.children[0];
	auto left = AddSideMarkers(std::move(join.children[0]->children[0]), columns, true);
	auto right = AddSideMarkers(std::move(join.children[1]->children[0]), columns, false);

	auto column_count = columns.size();
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
		    make_uniq<BoundColumnRefExpression>(columns[i].type, ColumnBinding(union_index, ProjectionIndex(i))));
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

	// the projection reads the groups instead of the left input
	for (auto &expr : op.expressions) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		optional_idx position;
		for (idx_t i = 0; i < column_count; i++) {
			if (columns[i].left == colref.Binding()) {
				position = i;
				break;
			}
		}
		if (!position.IsValid()) {
			throw InternalException("Set operation projection reads a column that is not compared");
		}
		expr = make_uniq<BoundColumnRefExpression>(expr->GetAlias(), expr->GetReturnType(),
		                                           ColumnBinding(group_index, ProjectionIndex(position.GetIndex())));
	}
	// drops the join and the windows
	op.children[0] = std::move(unnest);
	op.ResolveOperatorTypes();
}

unique_ptr<LogicalOperator> SetOperationAggregate::Optimize(unique_ptr<LogicalOperator> op) {
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	vector<SetOperationColumn> columns;
	bool is_except = false;
	if (Match(*op, columns, is_except)) {
		Rewrite(*op, columns, is_except);
	}
	return op;
}

} // namespace duckdb
