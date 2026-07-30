//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain_factorer.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain_factorer.hpp"

#include "duckdb/optimizer/duplicate_eliminated_domain_candidate.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain_safety.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

static vector<Identifier> GenerateColumnNames(idx_t column_count) {
	vector<Identifier> names;
	names.reserve(column_count);
	for (idx_t column_idx = 0; column_idx < column_count; column_idx++) {
		names.emplace_back("__duckdb_duplicate_eliminated_factor_col_" + to_string(column_idx));
	}
	return names;
}

unique_ptr<FactoredDuplicateEliminatedDomain>
DuplicateEliminatedDomainFactorer::TryFactor(Binder &binder, unique_ptr<LogicalOperator> &join_op,
                                             TableIndex domain_cte_index,
                                             const DuplicateEliminatedDomainCandidate &candidate) {
	auto &join = join_op->Cast<LogicalComparisonJoin>();
	if (join.children.size() != 2 || join.duplicate_eliminated_columns.empty()) {
		return nullptr;
	}
	D_ASSERT(candidate.key_indices.size() == join.duplicate_eliminated_columns.size());
	D_ASSERT(candidate.coverage == DuplicateEliminatedDomainCoverage::EXACT ||
	         DuplicateEliminatedDomainSafety::CanEvaluateAdditionalGroups(*join.children[1], domain_cte_index));

	auto &source_location = candidate.source.get();
	auto old_bindings = source_location->GetColumnBindings();
	auto source_types = source_location->types;

	auto factor = make_uniq<FactoredDuplicateEliminatedDomain>();
	factor->cte_index = binder.GenerateTableIndex();
	factor->cte_name = Identifier("__duckdb_duplicate_eliminated_factor_" + to_string(factor->cte_index.index));
	factor->column_count = old_bindings.size();
	factor->source = std::move(source_location);

	auto payload_ref_index = binder.GenerateTableIndex();
	auto payload_ref = make_uniq<LogicalCTERef>(payload_ref_index, factor->cte_index, source_types,
	                                            GenerateColumnNames(factor->column_count));
	auto payload_bindings = payload_ref->GetColumnBindings();
	for (idx_t binding_idx = 0; binding_idx < old_bindings.size(); binding_idx++) {
		if (old_bindings[binding_idx] != payload_bindings[binding_idx]) {
			factor->output_replacements.Add(old_bindings[binding_idx], payload_bindings[binding_idx]);
		}
	}
	source_location = std::move(payload_ref);

	CorrelatedColumnBindingReplacer replacer;
	factor->output_replacements.AddTo(replacer);
	replacer.stop_operator = join.children[1];
	replacer.VisitOperator(*join_op);

	auto domain_ref_index = binder.GenerateTableIndex();
	auto domain_ref = make_uniq<LogicalCTERef>(domain_ref_index, factor->cte_index, source_types,
	                                           GenerateColumnNames(factor->column_count));
	auto domain_bindings = domain_ref->GetColumnBindings();
	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> aggregates;
	auto domain = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	for (idx_t key_idx = 0; key_idx < candidate.key_indices.size(); key_idx++) {
		auto source_idx = candidate.key_indices[key_idx];
		D_ASSERT(source_idx < domain_bindings.size());
		auto &key = join.duplicate_eliminated_columns[key_idx];
		auto column =
		    make_uniq<BoundColumnRefExpression>(key->GetName(), key->GetReturnType(), domain_bindings[source_idx]);
		auto new_group = ColumnBinding::PushExpression(domain->groups, std::move(column));
		for (auto &grouping_set : domain->grouping_sets) {
			grouping_set.insert(new_group);
		}
	}
	domain->children.push_back(std::move(domain_ref));
	factor->domain = std::move(domain);
	return factor;
}

} // namespace duckdb
