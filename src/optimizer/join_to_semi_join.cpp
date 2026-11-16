// An INNER join against a relation that is only read by predicates becomes a SEMI join on the one relation those
// predicates compare it with.
//
//     SELECT DISTINCT f.k FROM f, d, p WHERE f.dk = d.dk AND d.x BETWEEN p.lo AND p.hi
//
// `p` contributes no column: it only says which rows of `d` qualify, and it repeats every qualifying row once per
// matching row of `p`. The join order keeps a relation without an equality predicate for last, so the predicate runs
// over (f JOIN d) x p. As `d SEMI JOIN p ON d.x BETWEEN p.lo AND p.hi` it filters `d` before the join with `f` and
// repeats nothing.
//
// JoinSimplification calls this for every region it reaches in its first phase, with what it knows about the operators
// above: whether they ignore duplicates and which tables they read.
//
// What must hold, in the plan as bound (the rewrite runs before filter pushdown, which then pushes the remaining
// filters below the new join): a region of filters, cross products and INNER joins below a projection or an
// aggregate, and in it a relation R such that
// - no operator above the region reads a column of R: its columns occur in predicates of the region only;
// - every predicate that reads R reads, besides R, columns of one other relation L of the region, and at least one
//   does; none of them holds an equality (an equality join is a hash join the join order places well, and its build
//   side feeds runtime filters; an OR of equalities is the disjunctive join rewrite's);
// - no expression of the region is volatile or can throw, neither a predicate nor an expression inside one of its
//   relations (or the definition of a CTE read there): a volatile one would be evaluated on fewer rows, and one
//   that can throw (a cast that can fail, arithmetic that can overflow) on other rows than before, because the
//   SEMI join changes which joins of the region run first and which relations are read to the end. That is within
//   what the join order may do anyway, but a query that ran must not start to fail because of this rewrite, nor
//   the other way round;
// - repeats of the region's rows do not matter: the parent chain ignores duplicates (a GROUP BY whose aggregates
//   ignore them, LogicalAggregate::IgnoresDuplicates: min, max, bool_and, bool_or and DISTINCT ones; a DISTINCT; the
//   right side of a SEMI / ANTI / MARK join, through projections, filters and joins without volatile expressions), or R
//   holds exactly one row (a projection over a dummy scan) and the region has a third relation (with L and R alone the
//   SEMI join would sit where the INNER join sits);
// - the SEMI join is not slower than the join it replaces. What the two are depends on the predicates between L and
//   R, split into their conjuncts (a BETWEEN is two range comparisons):
//   * none is a comparison of an expression over L with one over R: the INNER join is a cross product with a filter
//     or a blockwise nested-loop join, and so is the SEMI join, which emits less;
//   * one comparison: the same range or nested-loop join either way;
//   * two comparisons, or comparisons next to other predicates: the INNER join is a range join whose build side
//     feeds runtime filters into the scan of L. The SEMI join is an IE join only for two range comparisons in a
//     plan without materialized or recursive CTEs (which later optimizers may still add); otherwise it is a
//     blockwise or nested-loop join that compares every row of L with every row of R, without runtime filters.
//     That is cheap only on a small L: L must be a scan of at most MAX_UNFILTERED_SEMI_ROWS rows;
//   * three or more comparisons, or comparisons against an R known to hold at most one row (an ungrouped
//     aggregate, a LIMIT 1): no rewrite, the INNER join is a nested-loop join with runtime filters.
//
// Then L becomes `L SEMI JOIN R ON <the predicates that read R>` and R leaves the region. The rows of the region
// are, as a set, the rows of the region without R whose L row has a match in R, which is what the SEMI join keeps;
// with a one-row R every such row occurs exactly once in both.
#include "duckdb/optimizer/join_to_semi_join.hpp"

#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

class JoinRegionRewriter {
public:
	using table_set_t = unordered_set<TableIndex>;

	explicit JoinRegionRewriter(ClientContext &context) : context(context) {
	}

	void Initialize(LogicalOperator &plan) {
		// the CTEs whose definition holds an expression that is volatile or can throw, directly or through another
		// CTE: a fixpoint, a definition may read a CTE that is found later
		vector<reference<LogicalOperator>> ctes;
		CollectCTEs(plan, ctes);
		for (bool changed = true; changed;) {
			changed = false;
			for (auto &cte : ctes) {
				auto &op = cte.get();
				auto index = op.Cast<LogicalCTE>().table_index;
				if (unstable_ctes.count(index) > 0) {
					continue;
				}
				// a recursive CTE runs both of its children once per iteration
				const bool recursive = op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE;
				if (IsUnstable(*op.children[0]) || (recursive && IsUnstable(*op.children[1]))) {
					unstable_ctes.insert(index);
					changed = true;
				}
			}
		}
	}

	//! Notes a materialized CTE whose definition holds at most one row; the traversal calls this before it visits
	//! the references
	void VisitMaterializedCTE(const LogicalOperator &op) {
		if (HoldsAtMostOneRow(*op.children[0])) {
			single_row_ctes.insert(op.Cast<LogicalMaterializedCTE>().table_index);
		}
	}

	//! The largest scan that gets a SEMI join which may be planned as a blockwise nested-loop join
	static constexpr idx_t MAX_UNFILTERED_SEMI_ROWS = 100000;

	//! A relation of the region
	struct Leaf {
		//! The child indexes that lead from the root of the region to the relation
		vector<idx_t> path;
		table_set_t tables;
		bool one_row;
		bool at_most_one_row;
		//! The rows of the scan the relation reads, when it is a scan below projections and filters
		optional_idx scan_cardinality;
		//! Whether an expression below the relation is volatile or can throw
		bool unstable;
	};

	//! Where a predicate of the region lives in its operator
	enum class PredicateSlot : uint8_t { FILTER_EXPRESSION, JOIN_CONDITION, ANY_JOIN_CONDITION };

	//! A predicate of the region
	struct Predicate {
		//! The child indexes that lead from the root of the region to the operator that holds the predicate
		vector<idx_t> path;
		PredicateSlot slot;
		//! The position among the expressions of the filter / the conditions of the join
		idx_t index;
		table_set_t tables;
		bool is_volatile;
		bool can_throw;
		bool equality;
		//! The predicate, or for a comparison condition of a join its two sides
		optional_ptr<const Expression> expression;
		optional_ptr<const Expression> lhs;
		optional_ptr<const Expression> rhs;
		ExpressionType comparison_type = ExpressionType::INVALID;
	};

	//! The conjuncts of the predicates between two relations, by what a join can do with them
	struct PredicateShape {
		//! <, <=, >, >= between an expression over one relation and an expression over the other
		idx_t range_comparisons = 0;
		//! <>, IS [NOT] DISTINCT FROM between an expression over one relation and an expression over the other
		idx_t other_comparisons = 0;
		//! Anything else
		idx_t other_predicates = 0;
	};

	//! `semi_leaf` gets a SEMI join against `removed_leaf` on `predicates`
	struct Rewrite {
		idx_t removed_leaf;
		idx_t semi_leaf;
		vector<idx_t> predicates;
	};

public:
	static void ExpressionTables(const Expression &expr, table_set_t &tables) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    expr, [&](const BoundColumnRefExpression &colref) { tables.insert(colref.Binding().table_index); });
	}

	//! The tables the expressions of `op` read
	static void OperatorTables(const LogicalOperator &op, table_set_t &tables) {
		LogicalOperatorVisitor::EnumerateExpressions(
		    op, [&](const unique_ptr<Expression> *expr) { ExpressionTables(**expr, tables); });
	}

	static bool HasVolatileExpression(const LogicalOperator &op) {
		bool result = false;
		LogicalOperatorVisitor::EnumerateExpressions(
		    op, [&](const unique_ptr<Expression> *expr) { result = result || (*expr)->IsVolatile(); });
		return result;
	}

	static bool IsEquality(ExpressionType type) {
		return type == ExpressionType::COMPARE_EQUAL || type == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	}

	static bool Intersects(const table_set_t &a, const table_set_t &b) {
		for (auto &table : a) {
			if (b.count(table) > 0) {
				return true;
			}
		}
		return false;
	}

	static bool Subset(const table_set_t &a, const table_set_t &b) {
		for (auto &table : a) {
			if (b.count(table) == 0) {
				return false;
			}
		}
		return true;
	}

	static bool IsRangeComparison(ExpressionType type) {
		switch (type) {
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return true;
		default:
			return false;
		}
	}

	//! Whether one of the two expressions reads `tables` only and the other does not read them: the two sides of a
	//! join condition
	static bool OnOppositeSides(const Expression &a, const Expression &b, const table_set_t &tables) {
		table_set_t a_tables;
		table_set_t b_tables;
		ExpressionTables(a, a_tables);
		ExpressionTables(b, b_tables);
		if (a_tables.empty() || b_tables.empty()) {
			return false;
		}
		const bool a_inside = Subset(a_tables, tables);
		const bool b_inside = Subset(b_tables, tables);
		return (a_inside && !Intersects(b_tables, tables)) || (b_inside && !Intersects(a_tables, tables));
	}

	static void AddComparison(ExpressionType type, const Expression &lhs, const Expression &rhs,
	                          const table_set_t &tables, PredicateShape &shape) {
		if (!OnOppositeSides(lhs, rhs, tables)) {
			shape.other_predicates++;
		} else if (IsRangeComparison(type)) {
			shape.range_comparisons++;
		} else {
			shape.other_comparisons++;
		}
	}

	//! Adds the conjuncts of `expr`, a predicate between the relation of `tables` and another one, to `shape`
	static void AddPredicate(const Expression &expr, const table_set_t &tables, PredicateShape &shape) {
		switch (expr.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
				AddPredicate(*child, tables, shape);
			}
			return;
		case ExpressionType::COMPARE_BETWEEN: {
			auto &between = expr.Cast<BoundFunctionExpression>();
			auto &input = BoundBetweenExpression::Input(between);
			AddComparison(ExpressionType::COMPARE_GREATERTHANOREQUALTO, input,
			              BoundBetweenExpression::LowerBound(between), tables, shape);
			AddComparison(ExpressionType::COMPARE_LESSTHANOREQUALTO, input, BoundBetweenExpression::UpperBound(between),
			              tables, shape);
			return;
		}
		default:
			break;
		}
		if (BoundComparisonExpression::IsComparison(expr)) {
			auto &comparison = expr.Cast<BoundFunctionExpression>();
			AddComparison(expr.GetExpressionType(), BoundComparisonExpression::Left(comparison),
			              BoundComparisonExpression::Right(comparison), tables, shape);
			return;
		}
		shape.other_predicates++;
	}

	//! Whether `expr` holds an equality anywhere
	static bool HasEquality(const Expression &expr) {
		if (IsEquality(expr.GetExpressionType())) {
			return true;
		}
		bool result = false;
		ExpressionIterator::EnumerateChildren(expr,
		                                      [&](const Expression &child) { result = result || HasEquality(child); });
		return result;
	}

	//! A node of a join region: a filter, a cross product or a plain INNER join
	static bool IsRegionNode(const LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_FILTER:
			return !op.HasProjectionMap();
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
			return true;
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op.Cast<LogicalComparisonJoin>();
			return join.join_type == JoinType::INNER && !join.HasProjectionMap() && !join.filter_pushdown &&
			       join.duplicate_eliminated_columns.empty();
		}
		case LogicalOperatorType::LOGICAL_ANY_JOIN: {
			auto &join = op.Cast<LogicalAnyJoin>();
			return join.join_type == JoinType::INNER && !join.HasProjectionMap();
		}
		default:
			return false;
		}
	}

	//! Exactly one row: projections over a dummy scan
	static bool HoldsOneRow(const LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
			return true;
		case LogicalOperatorType::LOGICAL_PROJECTION:
			return op.children.size() == 1 && HoldsOneRow(*op.children[0]);
		default:
			return false;
		}
	}

	//! At most one row: exactly one, an ungrouped aggregate, a LIMIT 1, a reference to such a CTE; through
	//! projections and filters
	bool HoldsAtMostOneRow(const LogicalOperator &op) const {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
			return true;
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggregate = op.Cast<LogicalAggregate>();
			return aggregate.groups.empty() && aggregate.grouping_sets.size() <= 1;
		}
		case LogicalOperatorType::LOGICAL_LIMIT: {
			auto &limit = op.Cast<LogicalLimit>().limit_val;
			return limit.Type() == LimitNodeType::CONSTANT_VALUE && limit.GetConstantValue() <= 1;
		}
		case LogicalOperatorType::LOGICAL_TOP_N:
			return op.Cast<LogicalTopN>().limit <= 1;
		case LogicalOperatorType::LOGICAL_CTE_REF:
			return single_row_ctes.count(op.Cast<LogicalCTERef>().cte_index) > 0;
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_FILTER:
			return op.children.size() == 1 && HoldsAtMostOneRow(*op.children[0]);
		default:
			return false;
		}
	}

	static void CollectCTEs(LogicalOperator &op, vector<reference<LogicalOperator>> &ctes) {
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
		    op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			ctes.push_back(op);
		}
		for (auto &child : op.children) {
			CollectCTEs(*child, ctes);
		}
	}

	//! Whether an expression of `op` or below it, or of the definition of a CTE read there, is volatile or can throw
	bool IsUnstable(const LogicalOperator &op) const {
		if (op.type == LogicalOperatorType::LOGICAL_CTE_REF &&
		    unstable_ctes.count(op.Cast<LogicalCTERef>().cte_index) > 0) {
			return true;
		}
		bool result = false;
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expr) {
			result = result || (*expr)->IsVolatile() || (*expr)->CanThrow();
		});
		for (idx_t i = 0; i < op.children.size() && !result; i++) {
			result = IsUnstable(*op.children[i]);
		}
		return result;
	}

	//! The rows of the scan below projections and filters, when the scan says how many it reads
	optional_idx ScanCardinality(const LogicalOperator &op) const {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET: {
			auto &get = op.Cast<LogicalGet>();
			if (!get.children.empty() || !get.function.cardinality || !get.bind_data) {
				return optional_idx();
			}
			auto node_statistics = get.function.cardinality(context, get.bind_data.get());
			if (!node_statistics || !node_statistics->has_estimated_cardinality) {
				return optional_idx();
			}
			return node_statistics->estimated_cardinality;
		}
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_FILTER:
			return op.children.size() == 1 ? ScanCardinality(*op.children[0]) : optional_idx();
		default:
			return optional_idx();
		}
	}

	static unique_ptr<LogicalOperator> &SlotAt(unique_ptr<LogicalOperator> &op, const vector<idx_t> &path) {
		reference<unique_ptr<LogicalOperator>> slot = op;
		for (auto child : path) {
			slot = slot.get()->children[child];
		}
		return slot.get();
	}

	//! The relations and the predicates of the region rooted at `op`
	void CollectRegion(LogicalOperator &op, vector<idx_t> &path, vector<Leaf> &leaves, vector<Predicate> &predicates) {
		if (!IsRegionNode(op)) {
			Leaf leaf;
			leaf.path = path;
			for (auto &binding : op.GetColumnBindings()) {
				leaf.tables.insert(binding.table_index);
			}
			leaf.one_row = HoldsOneRow(op);
			leaf.at_most_one_row = HoldsAtMostOneRow(op);
			leaf.scan_cardinality = ScanCardinality(op);
			leaf.unstable = IsUnstable(op);
			leaves.push_back(std::move(leaf));
			return;
		}
		auto add = [&](PredicateSlot slot, idx_t index, const Expression &expr) {
			Predicate predicate;
			predicate.path = path;
			predicate.slot = slot;
			predicate.index = index;
			ExpressionTables(expr, predicate.tables);
			predicate.is_volatile = expr.IsVolatile();
			predicate.can_throw = expr.CanThrow();
			predicate.equality = HasEquality(expr);
			predicate.expression = expr;
			predicates.push_back(std::move(predicate));
		};
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_FILTER:
			for (idx_t i = 0; i < op.expressions.size(); i++) {
				add(PredicateSlot::FILTER_EXPRESSION, i, *op.expressions[i]);
			}
			break;
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &conditions = op.Cast<LogicalComparisonJoin>().conditions;
			for (idx_t i = 0; i < conditions.size(); i++) {
				auto &condition = conditions[i];
				if (!condition.IsComparison()) {
					add(PredicateSlot::JOIN_CONDITION, i, condition.GetJoinExpression());
					continue;
				}
				Predicate predicate;
				predicate.path = path;
				predicate.slot = PredicateSlot::JOIN_CONDITION;
				predicate.index = i;
				ExpressionTables(condition.GetLHS(), predicate.tables);
				ExpressionTables(condition.GetRHS(), predicate.tables);
				predicate.is_volatile = condition.GetLHS().IsVolatile() || condition.GetRHS().IsVolatile();
				predicate.can_throw = condition.GetLHS().CanThrow() || condition.GetRHS().CanThrow();
				predicate.equality = IsEquality(condition.GetComparisonType()) || HasEquality(condition.GetLHS()) ||
				                     HasEquality(condition.GetRHS());
				predicate.lhs = condition.GetLHS();
				predicate.rhs = condition.GetRHS();
				predicate.comparison_type = condition.GetComparisonType();
				predicates.push_back(std::move(predicate));
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_ANY_JOIN: {
			add(PredicateSlot::ANY_JOIN_CONDITION, 0, *op.Cast<LogicalAnyJoin>().condition);
			break;
		}
		default:
			break;
		}
		for (idx_t i = 0; i < op.children.size(); i++) {
			path.push_back(i);
			CollectRegion(*op.children[i], path, leaves, predicates);
			path.pop_back();
		}
	}

	//! A relation of the region that only filters one other relation of it
	bool FindRewrite(unique_ptr<LogicalOperator> &op, bool ignores, const table_set_t &used, const vector<Leaf> &leaves,
	                 const vector<Predicate> &predicates, Rewrite &rewrite) const {
		if (leaves.size() < 2) {
			return false;
		}
		for (auto &predicate : predicates) {
			// a volatile predicate anywhere in the region is evaluated once per row of the region: fewer rows, fewer
			// evaluations. One that can throw is evaluated on the rows the join order leaves it, and the SEMI join
			// changes that order: for the predicates it takes with it and for the ones that stay
			if (predicate.is_volatile || predicate.can_throw) {
				return false;
			}
		}
		for (auto &leaf : leaves) {
			// the same for the expressions inside the relations: which of them run to the end, and which stop
			// early because another one turned out empty, depends on how the region is joined
			if (leaf.unstable) {
				return false;
			}
		}
		for (idx_t r = 0; r < leaves.size(); r++) {
			auto &removed = leaves[r];
			if (!(ignores || (removed.one_row && leaves.size() >= 3)) || Intersects(removed.tables, used)) {
				continue;
			}
			// its parent goes with it: a cross product, or a join all of whose conditions read it
			if (removed.path.empty()) {
				continue;
			}
			vector<idx_t> parent_path(removed.path.begin(), removed.path.end() - 1);
			if (SlotAt(op, parent_path)->type == LogicalOperatorType::LOGICAL_FILTER) {
				continue;
			}
			vector<idx_t> reading;
			vector<idx_t> cross;
			optional_idx semi;
			bool valid = true;
			for (idx_t p = 0; p < predicates.size() && valid; p++) {
				auto &predicate = predicates[p];
				if (!Intersects(predicate.tables, removed.tables)) {
					// a condition of the parent that does not read the relation would lose its join
					valid = predicate.path != parent_path;
					continue;
				}
				reading.push_back(p);
				bool reads_other = false;
				for (auto &table : predicate.tables) {
					if (removed.tables.count(table) > 0) {
						continue;
					}
					optional_idx other;
					for (idx_t l = 0; l < leaves.size(); l++) {
						if (leaves[l].tables.count(table) > 0) {
							other = l;
							break;
						}
					}
					if (!other.IsValid()) {
						// a column of no relation of the region
						return false;
					}
					if (semi.IsValid() && semi.GetIndex() != other.GetIndex()) {
						valid = false;
						break;
					}
					semi = other;
					reads_other = true;
				}
				if (!valid) {
					break;
				}
				if (reads_other) {
					if (predicate.equality) {
						valid = false;
						break;
					}
					cross.push_back(p);
				}
			}
			if (!valid || !semi.IsValid() || semi.GetIndex() == r) {
				continue;
			}
			PredicateShape shape;
			for (auto p : cross) {
				auto &predicate = predicates[p];
				if (predicate.expression) {
					AddPredicate(*predicate.expression, removed.tables, shape);
				} else {
					AddComparison(predicate.comparison_type, *predicate.lhs, *predicate.rhs, removed.tables, shape);
				}
			}
			if (!SemiJoinIsNoSlower(shape, leaves, r, semi.GetIndex())) {
				continue;
			}
			rewrite.removed_leaf = r;
			rewrite.semi_leaf = semi.GetIndex();
			rewrite.predicates = std::move(reading);
			return true;
		}
		return false;
	}

	//! Whether `semi` SEMI JOIN `removed` on predicates of `shape` is planned as a join that is no slower than the
	//! INNER join of the two (see the top of this file)
	bool SemiJoinIsNoSlower(const PredicateShape &shape, const vector<Leaf> &leaves, idx_t removed, idx_t semi) const {
		const auto comparisons = shape.range_comparisons + shape.other_comparisons;
		if (comparisons == 0) {
			// a cross product with a filter or a blockwise nested-loop join either way
			return true;
		}
		if (comparisons > 2 || leaves[removed].at_most_one_row) {
			// the INNER join is a nested-loop join with runtime filters, the SEMI join would be a blockwise one
			return false;
		}
		if (comparisons == 1 && shape.other_predicates == 0) {
			// the same range or nested-loop join either way
			return true;
		}
		// the INNER join is a range join with runtime filters into the scan of L; the SEMI join may compare every
		// row of L with every row of R
		auto &semi_cardinality = leaves[semi].scan_cardinality;
		return semi_cardinality.IsValid() && semi_cardinality.GetIndex() <= MAX_UNFILTERED_SEMI_ROWS;
	}

	//! A filter of the region that lost every predicate goes
	static void RemoveEmptyFilters(unique_ptr<LogicalOperator> &op) {
		while (op->type == LogicalOperatorType::LOGICAL_FILTER && IsRegionNode(*op) && op->expressions.empty()) {
			auto child = std::move(op->children[0]);
			op = std::move(child);
		}
		if (IsRegionNode(*op)) {
			for (auto &child : op->children) {
				RemoveEmptyFilters(child);
			}
		}
	}

	//! Applies one rewrite to the region rooted at `op`; returns whether it did
	bool RewriteRegion(unique_ptr<LogicalOperator> &op, bool ignores, const table_set_t &used) {
		vector<Leaf> leaves;
		vector<Predicate> predicates;
		vector<idx_t> path;
		CollectRegion(*op, path, leaves, predicates);
		Rewrite rewrite;
		if (!FindRewrite(op, ignores, used, leaves, predicates, rewrite)) {
			return false;
		}
		// the predicates leave their operators, last position first so that the earlier positions stay valid
		vector<reference<const Predicate>> moved;
		for (auto p : rewrite.predicates) {
			moved.push_back(predicates[p]);
		}
		std::sort(moved.begin(), moved.end(), [](const Predicate &a, const Predicate &b) { return a.index > b.index; });
		vector<unique_ptr<Expression>> expressions;
		for (auto &entry : moved) {
			auto &predicate = entry.get();
			auto &node = SlotAt(op, predicate.path);
			switch (predicate.slot) {
			case PredicateSlot::FILTER_EXPRESSION:
				expressions.push_back(std::move(node->expressions[predicate.index]));
				node->expressions.erase_at(predicate.index);
				break;
			case PredicateSlot::JOIN_CONDITION: {
				auto &conditions = node->Cast<LogicalComparisonJoin>().conditions;
				expressions.push_back(JoinCondition::CreateExpression(std::move(conditions[predicate.index])));
				conditions.erase_at(predicate.index);
				if (conditions.empty()) {
					node = make_uniq<LogicalCrossProduct>(std::move(node->children[0]), std::move(node->children[1]));
				}
				break;
			}
			case PredicateSlot::ANY_JOIN_CONDITION:
				expressions.push_back(std::move(node->Cast<LogicalAnyJoin>().condition));
				node = make_uniq<LogicalCrossProduct>(std::move(node->children[0]), std::move(node->children[1]));
				break;
			}
		}
		// L SEMI JOIN R takes the place of L
		auto &removed = leaves[rewrite.removed_leaf];
		auto &semi = leaves[rewrite.semi_leaf];
		auto &semi_slot = SlotAt(op, semi.path);
		auto left = std::move(semi_slot);
		auto right = std::move(SlotAt(op, removed.path));
		vector<JoinCondition> conditions;
		LogicalComparisonJoin::ExtractJoinConditions(context, JoinType::SEMI, JoinRefType::REGULAR, left, right,
		                                             semi.tables, removed.tables, expressions, conditions);
		semi_slot = LogicalComparisonJoin::CreateJoin(JoinType::SEMI, JoinRefType::REGULAR, std::move(left),
		                                              std::move(right), std::move(conditions));
		// the cross product that held R is its other child now
		vector<idx_t> parent_path(removed.path.begin(), removed.path.end() - 1);
		auto &parent = SlotAt(op, parent_path);
		auto other = std::move(parent->children[1 - removed.path.back()]);
		parent = std::move(other);
		RemoveEmptyFilters(op);
		return true;
	}

public:
	ClientContext &context;
	//! The materialized CTEs seen so far whose definition holds at most one row
	table_set_t single_row_ctes;
	//! The CTEs whose definition holds an expression that is volatile or can throw
	table_set_t unstable_ctes;
};

JoinToSemiJoin::JoinToSemiJoin(ClientContext &context) : rewriter(make_uniq<JoinRegionRewriter>(context)) {
}

JoinToSemiJoin::~JoinToSemiJoin() {
}

void JoinToSemiJoin::Initialize(LogicalOperator &plan) {
	rewriter->Initialize(plan);
}

bool JoinToSemiJoin::IsRegionNode(const LogicalOperator &op) {
	return JoinRegionRewriter::IsRegionNode(op);
}

bool JoinToSemiJoin::RewriteRegion(unique_ptr<LogicalOperator> &op, bool ignores_duplicates,
                                   const unordered_set<TableIndex> &read_tables) {
	return rewriter->RewriteRegion(op, ignores_duplicates, read_tables);
}

void JoinToSemiJoin::VisitMaterializedCTE(const LogicalOperator &op) {
	rewriter->VisitMaterializedCTE(op);
}

void JoinToSemiJoin::AddReadTables(const LogicalOperator &op, unordered_set<TableIndex> &tables) {
	JoinRegionRewriter::OperatorTables(op, tables);
}

bool JoinToSemiJoin::HasVolatileExpression(const LogicalOperator &op) {
	return JoinRegionRewriter::HasVolatileExpression(op);
}

} // namespace duckdb
