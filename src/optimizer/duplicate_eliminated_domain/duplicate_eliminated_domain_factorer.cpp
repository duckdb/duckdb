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
#include "duckdb/planner/logical_operator_deep_copy.hpp"
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

static bool TryAddLayoutReplacements(const vector<ColumnBinding> &old_bindings,
                                     const vector<ColumnBinding> &new_bindings,
                                     BindingReplacementGraph &output_replacements) {
	if (old_bindings.size() != new_bindings.size()) {
		return false;
	}
	for (idx_t binding_idx = 0; binding_idx < old_bindings.size(); binding_idx++) {
		if (old_bindings[binding_idx] == new_bindings[binding_idx]) {
			continue;
		}
		if (!output_replacements.TryAdd(ReplacementBinding(old_bindings[binding_idx], new_bindings[binding_idx]))) {
			return false;
		}
	}
	return true;
}

static unique_ptr<LogicalOperator> TryCopyAlternative(Binder &binder, LogicalOperator &join,
                                                      const vector<ColumnBinding> &live_output_bindings,
                                                      const vector<LogicalType> &live_output_types,
                                                      BindingReplacementGraph &output_replacements) {
	unique_ptr<LogicalOperator> alternative;
	try {
		alternative = join.Copy(binder.context);
	} catch (NotImplementedException &) {
		return nullptr;
	}
	auto parameters = binder.GetParameters();
	unordered_map<TableIndex, TableIndex> table_idx_replacements;
	TableBindingReplacer replacer(table_idx_replacements, parameters ? parameters->GetParametersPtr() : nullptr);
	replacer.VisitOperator(*alternative);
	alternative->ResolveOperatorTypes();
	if (live_output_types != alternative->types ||
	    !TryAddLayoutReplacements(live_output_bindings, alternative->GetColumnBindings(), output_replacements)) {
		return nullptr;
	}
	return alternative;
}

struct FactoredCandidateSource {
public:
	TableIndex cte_index;
	Identifier cte_name;
	idx_t column_count;
	unique_ptr<LogicalOperator> source;
	unique_ptr<LogicalOperator> domain;
	vector<LogicalType> key_types;
};

static optional<FactoredCandidateSource> TryFactorCandidateSource(Binder &binder, LogicalComparisonJoin &join,
                                                                  const DuplicateEliminatedDomainCandidate &candidate,
                                                                  BindingReplacementGraph &output_replacements) {
	auto source_location = candidate.TryResolveSource(join.children[0]);
	if (!source_location) {
		return {};
	}
	auto old_bindings = (*source_location)->GetColumnBindings();
	auto source_types = (*source_location)->types;
	if (old_bindings.size() != source_types.size()) {
		return {};
	}

	FactoredCandidateSource result;
	result.cte_index = binder.GenerateTableIndex();
	result.cte_name = Identifier("__duckdb_duplicate_eliminated_factor_" + to_string(result.cte_index.index));
	result.column_count = old_bindings.size();

	auto payload_ref = make_uniq<LogicalCTERef>(binder.GenerateTableIndex(), result.cte_index, source_types,
	                                            GenerateColumnNames(result.column_count));
	if (!TryAddLayoutReplacements(old_bindings, payload_ref->GetColumnBindings(), output_replacements)) {
		return {};
	}

	auto domain_ref = make_uniq<LogicalCTERef>(binder.GenerateTableIndex(), result.cte_index, source_types,
	                                           GenerateColumnNames(result.column_count));
	result.key_types.reserve(join.duplicate_eliminated_columns.size());
	for (auto &key : join.duplicate_eliminated_columns) {
		result.key_types.push_back(key->GetReturnType());
	}
	result.domain = DuplicateEliminatedDomainBuilder::TryBuild(binder, std::move(domain_ref), candidate.KeyIndices(),
	                                                           result.key_types);
	if (!result.domain) {
		return {};
	}

	// Install the factor only after every replacement subplan has been constructed successfully.
	result.source = std::move(*source_location);
	*source_location = std::move(payload_ref);
	CorrelatedColumnBindingReplacer replacer;
	output_replacements.AddTo(replacer);
	replacer.stop_operator = join.children[1];
	replacer.VisitOperator(join);
	return result;
}

static unique_ptr<LogicalOperator> TryBuildFactoredPlan(Binder &binder, unique_ptr<LogicalOperator> alternative,
                                                        TableIndex domain_cte_index,
                                                        FactoredCandidateSource factored_source,
                                                        BindingReplacementGraph &output_replacements) {
	auto &join = alternative->Cast<LogicalComparisonJoin>();
	join.duplicate_eliminated_columns.clear();

	BindingReplacementGraph domain_output_replacements;
	auto domain_cte_child = ColumnBindingRewrite::CreateIdentityProjection(
	    binder.GenerateTableIndex(), std::move(alternative), domain_output_replacements);
	if (!output_replacements.TryMerge(domain_output_replacements)) {
		return nullptr;
	}
	auto domain_cte_name = Identifier("__duckdb_delim_dedup_" + to_string(domain_cte_index.index));
	auto domain_cte = make_uniq<LogicalMaterializedCTE>(
	    domain_cte_name, domain_cte_index, factored_source.key_types.size(), std::move(factored_source.domain),
	    std::move(domain_cte_child), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);

	BindingReplacementGraph factor_output_replacements;
	auto factor_child = ColumnBindingRewrite::CreateIdentityProjection(
	    binder.GenerateTableIndex(), std::move(domain_cte), factor_output_replacements);
	if (!output_replacements.TryMerge(factor_output_replacements)) {
		return nullptr;
	}
	return make_uniq<LogicalMaterializedCTE>(factored_source.cte_name, factored_source.cte_index,
	                                         factored_source.column_count, std::move(factored_source.source),
	                                         std::move(factor_child), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
}

unique_ptr<DelimJoinCTEOptimizationAlternative>
DuplicateEliminatedDomainFactorer::TryFactor(Binder &binder, unique_ptr<LogicalOperator> &join_op,
                                             TableIndex domain_cte_index,
                                             const DuplicateEliminatedDomainCandidate &candidate) {
	auto &live_join = join_op->Cast<LogicalComparisonJoin>();
	if (live_join.children.size() != 2 || live_join.duplicate_eliminated_columns.empty()) {
		return nullptr;
	}
	D_ASSERT(candidate.KeyIndices().size() == live_join.duplicate_eliminated_columns.size());
	join_op->ResolveOperatorTypes();
	auto live_output_bindings = join_op->GetColumnBindings();
	auto live_output_types = join_op->types;

	auto result = make_uniq<DelimJoinCTEOptimizationAlternative>();
	auto alternative =
	    TryCopyAlternative(binder, *join_op, live_output_bindings, live_output_types, result->output_replacements);
	if (!alternative) {
		return nullptr;
	}

	auto factored_source = TryFactorCandidateSource(binder, alternative->Cast<LogicalComparisonJoin>(), candidate,
	                                                result->output_replacements);
	if (!factored_source) {
		return nullptr;
	}
	result->plan = TryBuildFactoredPlan(binder, std::move(alternative), domain_cte_index, std::move(*factored_source),
	                                    result->output_replacements);
	if (!result->plan) {
		return nullptr;
	}
	result->plan->ResolveOperatorTypes();
	if (!ColumnBindingRewrite::TryValidateOutputLayout(live_output_bindings, live_output_types,
	                                                   result->plan->GetColumnBindings(), result->plan->types,
	                                                   result->output_replacements)) {
		return nullptr;
	}
	return result;
}

} // namespace duckdb
