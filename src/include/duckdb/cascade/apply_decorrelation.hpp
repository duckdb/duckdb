//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/apply_decorrelation.hpp
//
// Apply elimination: the classic rule set that rewrites the Apply operator
// (LogicalDependentJoin) into ordinary joins, run as an optimizer pass.
//
// DuckDB decorrelates during planning, in FlattenDependentJoins. Keeping the
// same work here instead lets one query be planned both ways and compared.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class ClientContext;
class Expression;
class LogicalAggregate;
class LogicalOperator;

//! A rewrite can change the bindings a node exposes. The caller gets the old ->
//! new mapping back so it can repoint its own expressions; without it, replacing
//! a sub-tree with an aggregate (which re-binds its groups) would strand every
//! reference the parent holds.
using BindingExport = vector<std::pair<ColumnBinding, ColumnBinding>>;

//! Rewrite the marker joins that Apply elimination produces into the semi/anti
//! joins they actually mean, whenever the marker is consumed only as a
//! predicate. A marker is dead weight the executor has to carry and that keeps
//! the join off the semi-join fast path; DuckDB's Deliminator does the same
//! cleanup after FlattenDependentJoins.
unique_ptr<LogicalOperator> SimplifyMarkerJoins(unique_ptr<LogicalOperator> plan);

class ApplyDecorrelator {
public:
	ApplyDecorrelator(Binder &binder, ClientContext &context);

	//! Eliminate every Apply in the plan.
	unique_ptr<LogicalOperator> Decorrelate(unique_ptr<LogicalOperator> plan);

private:
	unique_ptr<LogicalOperator> DecorrelateNode(unique_ptr<LogicalOperator> op, BindingExport &exports);
	unique_ptr<LogicalOperator> DecorrelateApply(unique_ptr<LogicalOperator> op, BindingExport &exports);
	//! Correlated scalar subquery, by identity (9) of Galindo-Legaria & Joshi:
	//! group by the outer columns over a left outer join, so an outer row with no
	//! match still has a group and the aggregate sees a NULL-padded row.
	unique_ptr<LogicalOperator> DecorrelateScalar(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
	                                              const CorrelatedColumns &correlated, BindingExport &exports);
	//! Correlated scalar sub-query whose correlation sits below a GroupBy of its own,
	//! by identity (8): the Apply moves below that GroupBy (over a deduplicated outer
	//! side) and the outer columns join its grouping, with the outer rows' multiplicity
	//! restored afterwards.
	unique_ptr<LogicalOperator> DecorrelateNestedScalar(unique_ptr<LogicalOperator> left,
	                                                    unique_ptr<LogicalOperator> right,
	                                                    const vector<LogicalOperator *> &projections,
	                                                    LogicalAggregate &top, LogicalAggregate &nested,
	                                                    vector<unique_ptr<Expression>> &extracted,
	                                                    const CorrelatedColumns &correlated, BindingExport &exports,
	                                                    const ColumnBinding &value_binding);

private:
	Binder &binder;
	ClientContext &context;
};

} // namespace duckdb
