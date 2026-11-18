//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {
class Optimizer;
class LogicalAggregate;
class LogicalMaterializedCTE;

//! The producers a restriction to the outer keys keeps running, as far as they are proven to stop with its removal
struct ReleasedDomainWork {
	//! The producer of the key domain, when nothing else reads it
	optional_ptr<LogicalOperator> production;
	//! Whether the source is no longer materialized
	bool source = false;
};

//! Restrict local grouped aggregates to their consumer's demand keys.
class DuplicateEliminatedDomainOptimizer : public LogicalOperatorVisitor {
public:
	explicit DuplicateEliminatedDomainOptimizer(Optimizer &optimizer);

public:
	void Optimize(unique_ptr<LogicalOperator> &op);

private:
	void VisitOperator(unique_ptr<LogicalOperator> &op) override;
	bool TryOptimizeDomain(unique_ptr<LogicalOperator> &op, idx_t aggregate_side, idx_t condition_index);
	bool TryOptimizeRuntimeDomain(unique_ptr<LogicalOperator> &op, LogicalAggregate &aggregate, idx_t aggregate_side,
	                              idx_t demand_index, ExpressionType comparison,
	                              optional_ptr<const ReleasedDomainWork> released = nullptr);
	bool TryPullUpOuterDomainJoin(unique_ptr<LogicalOperator> &op);

private:
	Optimizer &optimizer;
	BindingReplacementGraph replacements;
	vector<reference<LogicalMaterializedCTE>> cte_scopes;
};
} // namespace duckdb
