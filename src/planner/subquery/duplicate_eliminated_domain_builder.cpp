//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/duplicate_eliminated_domain_builder.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/subquery/duplicate_eliminated_domain_builder.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> DuplicateEliminatedDomainBuilder::TryBuild(Binder &binder,
                                                                       unique_ptr<LogicalOperator> source,
                                                                       const vector<idx_t> &key_indices,
                                                                       const vector<LogicalType> &key_types) {
	if (key_indices.size() != key_types.size()) {
		return nullptr;
	}
	source->ResolveOperatorTypes();
	auto source_bindings = source->GetColumnBindings();

	auto domain = make_uniq<LogicalAggregate>(binder.GenerateTableIndex(), binder.GenerateTableIndex(),
	                                          vector<unique_ptr<Expression>>());
	for (idx_t key_idx = 0; key_idx < key_indices.size(); key_idx++) {
		auto source_idx = key_indices[key_idx];
		if (source_idx >= source_bindings.size() || source_idx >= source->types.size() ||
		    source->types[source_idx] != key_types[key_idx]) {
			return nullptr;
		}
		ColumnBinding::PushExpression(
		    domain->groups, make_uniq<BoundColumnRefExpression>(key_types[key_idx], source_bindings[source_idx]));
	}
	domain->children.push_back(std::move(source));
	domain->ResolveOperatorTypes();
	return domain;
}

} // namespace duckdb
