//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/type_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class TypePushdown {
public:
	explicit TypePushdown(ClientContext &ctx);
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	ClientContext &context;
};

struct GetAnalysis {
	LogicalGet &get;
	/**
	 * for CAST(col), mapping of "col scan index" -> "CAST expression".
	 * "CAST expression" is nullptr iff column is used with a different function
	 * or without function application in the query plan.
	 */
	unordered_map<ProjectionIndex, const BoundCastExpression *> col_to_cast;
};

using Analyses = unordered_map<TableIndex, GetAnalysis>;

/*
 * SELECT CAST(col AS T) FROM '*.csv' yields a PROJECTION CAST(col) -> GET (csv)
 * plan. PROJECTION's "col" table_index is 1, GET's table_index is 0.
 * So we want to track original table_index for GET in case column is found
 * in filter we failed to push down (i.e. WHERE CAST(col AS T) > 0) as well as
 * projection's table_index.
 *
 * So we keep a mapping of
 *
 * "projection table index" to "projection operator".
 *
 * to resolve this.
 * For simplicity, current implementation is limited to one level i.e.
 * PROJECTION -> GET (i.e. read from VIEW) is pushed down but VIEW->VIEW->GET
 * or VIEW->CTE->GET is not.
 *
 * Storing a reference is fine because the plan outlives the optimizer pass.
 */
using Projections = unordered_map<TableIndex, LogicalProjection &>;

/**
 * Collect CAST(col) expressions. If "col" is used without CAST in "plan",
 * record in "analyses.conflicts"
 */
struct CastCollect final : LogicalOperatorVisitor {
	Analyses &analyses;
	const Projections &projections;
	// Casts that are direct outputs of a clean projection over a GET. Only these
	// start a pushdown candidate; a nested cast may push down a different value.
	// See test/sql/copy/csv/auto/test_large_integer_detection.test
	unordered_set<const Expression *> top_level_casts;

	CastCollect(Analyses &analyses, const Projections &projections);
	void VisitOperator(LogicalOperator &op) override;
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *ptr) override;
	unique_ptr<Expression> VisitReplace(BoundCastExpression &expr, unique_ptr<Expression> *ptr) override;
};

/*
 * For "col" in columns collected by ScalarFnCollect, replace CAST(col) to "col"
 * if "col" doesn't have conflicting usage. Update return types for bound
 * columns and logical projections referencing this column.
 */
struct CastReplace final : LogicalOperatorVisitor {
	Analyses &analyses;
	const Projections &projections;

	CastReplace(Analyses &analyses, const Projections &aliases);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *ptr) override;
	unique_ptr<Expression> VisitReplace(BoundCastExpression &expr, unique_ptr<Expression> *ptr) override;
};

void FindGetsAndProjections(LogicalOperator &op, Analyses &analyses, Projections &aliases);

struct GetBinding {
	GetAnalysis &analysis;
	ProjectionIndex column_index;
	// If column binding was part of a projection, this is non-nullptr
	LogicalProjection *projection;
};

/*
 * Given a column binding, resolve it to a GET and a GET's column scan index.
 * Returns nullopt for virtual columns and columns which are neither part of
 * GET nor part of PROJECTION wrapping a GET.
 */
std::optional<GetBinding> Resolve(ColumnBinding binding, Analyses &analyses, const Projections &projections);
} // namespace duckdb
