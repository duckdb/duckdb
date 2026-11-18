//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ClientContext;
class Expression;
struct JoinCondition;
class JoinToSemiJoin;
class LogicalAggregate;
class LogicalComparisonJoin;
class LogicalFilter;
class LogicalJoin;
class LogicalOrder;
class LogicalProjection;
class LogicalTopN;

//! The two points of the optimizer pipeline at which joins are simplified
enum class JoinSimplificationPhase : uint8_t {
	//! On the plan as bound, before filter pushdown: an INNER join against a relation that only filters another one
	//! becomes a SEMI join (JoinToSemiJoin). The predicates still sit where the query put them, in filters above
	//! cross products and in join conditions, which is what that rewrite collects
	BEFORE_FILTER_PUSHDOWN,
	//! After filter pushdown, before join ordering: outer joins whose NULL-extended rows are filtered become LEFT,
	//! RIGHT, INNER or ANTI joins. The filters have reached the joins they constrain by then
	AFTER_FILTER_PUSHDOWN
};

//! Simplifies joins from what the operators above them need: one traversal that carries, down the plan, the columns
//! that have to stay available, the columns whose NULLs are filtered or required, whether repeated rows matter and
//! which tables are read. Each phase applies the rewrites that belong to it:
//! - outer joins are simplified if NULL-extended rows are filtered in a way that changes the join semantics;
//! - INNER joins against relations that only filter become SEMI joins if repeated rows do not matter.
class JoinSimplification {
public:
	JoinSimplification(ClientContext &context, JoinSimplificationPhase phase);
	~JoinSimplification();

public:
	void Optimize(unique_ptr<LogicalOperator> &plan);

private:
	//! What is known about the operators above the operator being visited, apart from the column sets below.
	//! Unlike those, it differs between the children of one operator
	struct ParentInfo {
		//! Whether the operators above give the same result over the rows of this operator with or without repeats
		bool ignores_duplicates = false;
		//! Whether `read_tables` is known: every operator above reads the columns of this one through expressions
		bool reads_known = false;
		//! The tables the operators above read through expressions, up to the closest projection or aggregate
		unordered_set<TableIndex> read_tables;
		//! Whether the parent is a node of the same region of filters, cross products and INNER joins
		bool in_region = false;
	};

	//! A context below an operator that does not pass the column sets on
	JoinSimplification(JoinSimplification &parent, column_binding_set_t required_columns);

	void VisitOperator(unique_ptr<LogicalOperator> &op_ptr);
	//! Visit child `child_index` of `op` with the column sets of this context / of a new context that requires
	//! `required_columns`. A projection map `op` holds for the child follows the child's bindings, which a rewrite
	//! below may change
	void VisitChild(LogicalOperator &op, idx_t child_index, ParentInfo info);
	void VisitChildInNewContext(LogicalOperator &op, idx_t child_index, column_binding_set_t required_columns,
	                            ParentInfo info);
	//! The ParentInfo of the children of `op`, which passes its rows on (a filter, a join): it reads what the
	//! operators above read and what its own expressions read
	ParentInfo PassThroughInfo(const LogicalOperator &op, bool ignores_duplicates) const;
	//! The ParentInfo below an operator that hides what is above it
	static ParentInfo UnknownInfo();
	bool SimplifiesOuterJoins() const {
		return phase == JoinSimplificationPhase::AFTER_FILTER_PUSHDOWN;
	}

	//! Extract and propagate column references through expressions and join conditions
	void AddColumnReferences(const Expression &expr, column_binding_set_t &bindings);
	void AddRequiredColumns(const Expression &expr);
	void AddRequiredColumns(const JoinCondition &condition);
	void AddRequiredColumns(const vector<JoinCondition> &conditions);
	bool GetColumnBinding(const Expression &expr, ColumnBinding &binding);
	bool GetNullPreservingColumnBinding(const Expression &expr, ColumnBinding &binding);

	//! Track predicates that require columns to be NULL or reject NULL values
	void HandleExpression(const Expression &expr);
	void HandleFilterExpression(const Expression &expr);
	bool HandleIsNullExpression(const Expression &expr);
	bool IsNullFilter(const Expression &expr, ColumnBinding &binding);
	void InitializeRequiredColumns(LogicalOperator &op);
	bool FiltersNulls(ExpressionType comparison_type);

	//! Rewrite joins based on the NULL constraints collected from operators above them
	bool TryConvertLeftToAntiJoin(LogicalComparisonJoin &join);
	bool HasNullRequiredColumns(const vector<ColumnBinding> &bindings);
	bool HasRequiredColumns(const vector<ColumnBinding> &bindings);
	void MarkEliminatedNullColumns(const vector<ColumnBinding> &bindings);
	void EraseNullRequiredColumns(LogicalOperator &child);
	vector<ColumnBinding> GetRightBindings(LogicalJoin &join);
	void SimplifyOuterJoinType(LogicalComparisonJoin &join);

	//! Visit operators while preserving only the constraints that can safely pass through each operator type
	void VisitComparisonJoin(LogicalComparisonJoin &join, unique_ptr<LogicalOperator> &op_ptr);
	void VisitInnerOrSemiJoin(LogicalComparisonJoin &join, LogicalOperator &op);
	void VisitOuterJoin(LogicalComparisonJoin &join, unique_ptr<LogicalOperator> &op_ptr);
	void VisitProjection(LogicalProjection &projection, LogicalOperator &op);
	void VisitFilter(LogicalFilter &filter, LogicalOperator &op);
	void VisitOrder(LogicalOrder &order, LogicalOperator &op);
	void VisitTopN(LogicalTopN &top_n, LogicalOperator &op);
	void VisitAggregate(LogicalAggregate &aggregate, LogicalOperator &op);
	void VisitUnsupportedOperator(LogicalOperator &op);
	//! The children of a join that the column sets do not pass through: `left` / `right` say whether the side
	//! may see repeated rows of its child without a change of the result
	void VisitJoinChildren(LogicalOperator &op, bool left_ignores_duplicates, bool right_ignores_duplicates,
	                       bool keep_context);

private:
	JoinSimplificationPhase phase;
	//! The region rewrite of the first phase; shared by the contexts of one traversal
	optional_ptr<JoinToSemiJoin> join_to_semi_join;
	unique_ptr<JoinToSemiJoin> owned_join_to_semi_join;
	//! What is known about the operators above the operator being visited
	ParentInfo parent;

	//! Columns that have their NULL values filtered
	column_binding_set_t null_filtered_columns;
	//! Columns that are required to be NULL
	column_binding_set_t null_required_columns;
	//! Columns that are known to be NULL after rewriting a LEFT join to an ANTI join
	column_binding_set_t eliminated_null_columns;
	//! Columns that must remain available to operators above the current point in the plan
	column_binding_set_t required_columns;
	bool initialized_required_columns = false;
};

} // namespace duckdb
