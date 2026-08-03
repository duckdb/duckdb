//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_factorer.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_factorer.hpp"

#include "duckdb/planner/subquery/duplicate_eliminated_domain_builder.hpp"
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
	auto &live_join = join_op->Cast<LogicalComparisonJoin>();
	if (live_join.children.size() != 2 || live_join.duplicate_eliminated_columns.empty()) {
		return nullptr;
	}
	D_ASSERT(candidate.KeyIndices().size() == live_join.duplicate_eliminated_columns.size());
	join_op->ResolveOperatorTypes();
	auto live_output_bindings = join_op->GetColumnBindings();
	auto live_output_types = join_op->types;

	unique_ptr<LogicalOperator> alternative;
	try {
		alternative = join_op->Copy(binder.context);
	} catch (NotImplementedException &) {
		return nullptr;
	}
	alternative->ResolveOperatorTypes();
	auto alternative_output_bindings = alternative->GetColumnBindings();
	if (live_output_bindings.size() != alternative_output_bindings.size() ||
	    live_output_types != alternative->types) {
		return nullptr;
	}

	auto factor = make_uniq<FactoredDuplicateEliminatedDomain>();
	for (idx_t output_idx = 0; output_idx < live_output_bindings.size(); output_idx++) {
		if (!factor->output_replacements.TryAdd(
		        ReplacementBinding(live_output_bindings[output_idx], alternative_output_bindings[output_idx]))) {
			return nullptr;
		}
	}

	auto &join = alternative->Cast<LogicalComparisonJoin>();
	auto source_location = &join.children[0];
	for (auto child_idx : candidate.SourcePath()) {
		if (child_idx >= (*source_location)->children.size()) {
			return nullptr;
		}
		source_location = &(*source_location)->children[child_idx];
	}

	auto old_bindings = (*source_location)->GetColumnBindings();
	auto source_types = (*source_location)->types;
	if ((*source_location)->type != candidate.SourceType() || source_types != candidate.SourceTypes() ||
	    old_bindings.size() != source_types.size()) {
		return nullptr;
	}
	for (auto key_idx : candidate.KeyIndices()) {
		if (key_idx >= source_types.size()) {
			return nullptr;
		}
	}

	factor->cte_index = binder.GenerateTableIndex();
	factor->cte_name = Identifier("__duckdb_duplicate_eliminated_factor_" + to_string(factor->cte_index.index));
	factor->column_count = old_bindings.size();

	auto payload_ref_index = binder.GenerateTableIndex();
	auto payload_ref = make_uniq<LogicalCTERef>(payload_ref_index, factor->cte_index, source_types,
	                                            GenerateColumnNames(factor->column_count));
	auto payload_bindings = payload_ref->GetColumnBindings();
	for (idx_t binding_idx = 0; binding_idx < old_bindings.size(); binding_idx++) {
		if (old_bindings[binding_idx] != payload_bindings[binding_idx]) {
			if (!factor->output_replacements.TryAdd(
			        ReplacementBinding(old_bindings[binding_idx], payload_bindings[binding_idx]))) {
				return nullptr;
			}
		}
	}

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
		return nullptr;
	}

	// Install the factor only after every replacement subplan has been constructed successfully.
	factor->source = std::move(*source_location);
	*source_location = std::move(payload_ref);
	CorrelatedColumnBindingReplacer replacer;
	factor->output_replacements.AddTo(replacer);
	replacer.stop_operator = join.children[1];
	replacer.VisitOperator(*alternative);
	factor->child = std::move(alternative);
	return factor;
}

} // namespace duckdb
