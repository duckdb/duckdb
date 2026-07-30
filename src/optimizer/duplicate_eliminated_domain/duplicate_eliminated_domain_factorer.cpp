//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_factorer.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_factorer.hpp"

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_builder.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.hpp"
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
                                             const DuplicateEliminatedDomainCandidate &candidate) {
	auto &join = join_op->Cast<LogicalComparisonJoin>();
	if (join.children.size() != 2 || join.duplicate_eliminated_columns.empty()) {
		return nullptr;
	}
	D_ASSERT(candidate.KeyIndices().size() == join.duplicate_eliminated_columns.size());

	auto &source_location = candidate.Source();
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
	vector<LogicalType> key_types;
	key_types.reserve(join.duplicate_eliminated_columns.size());
	for (idx_t key_idx = 0; key_idx < join.duplicate_eliminated_columns.size(); key_idx++) {
		auto &key = join.duplicate_eliminated_columns[key_idx];
		key_types.push_back(key->GetReturnType());
	}
	factor->domain =
	    DuplicateEliminatedDomainBuilder::TryBuild(binder, std::move(domain_ref), candidate.KeyIndices(), key_types);
	if (!factor->domain) {
		throw InternalException("Failed to construct factored duplicate-eliminated domain");
	}
	return factor;
}

} // namespace duckdb
