//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_inliner.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_inliner.hpp"

#include "duckdb/planner/subquery/duplicate_eliminated_domain_builder.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator_deep_copy.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

using operator_location_list_t = vector<reference<unique_ptr<LogicalOperator>>>;

struct DomainAggregateUse {
	bool contains_domain = false;
	bool feeds_aggregate = false;
};

static DomainAggregateUse FindDomainAggregateUse(const LogicalOperator &op, TableIndex domain_cte_index) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF && op.Cast<LogicalCTERef>().cte_index == domain_cte_index) {
		return {true, false};
	}
	DomainAggregateUse result;
	for (auto &child : op.children) {
		auto child_use = FindDomainAggregateUse(*child, domain_cte_index);
		result.contains_domain |= child_use.contains_domain;
		result.feeds_aggregate |= child_use.feeds_aggregate;
	}
	if (result.contains_domain && op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		result.feeds_aggregate = true;
	}
	return result;
}

static optional_ptr<TableCatalogEntry> GetScannedTable(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return op.Cast<LogicalGet>().GetTable();
	}
	if (op.children.size() != 1) {
		return nullptr;
	}
	return GetScannedTable(*op.children[0]);
}

static bool ScansTable(const LogicalOperator &op, const TableCatalogEntry &table, TableIndex domain_cte_index) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF && op.Cast<LogicalCTERef>().cte_index == domain_cte_index) {
		return false;
	}
	if (op.type == LogicalOperatorType::LOGICAL_GET && op.Cast<LogicalGet>().GetTable().get() == &table) {
		return true;
	}
	for (auto &child : op.children) {
		if (ScansTable(*child, table, domain_cte_index)) {
			return true;
		}
	}
	return false;
}

static void FindDomainReferences(unique_ptr<LogicalOperator> &op, TableIndex domain_cte_index,
                                 operator_location_list_t &locations) {
	if (op->type == LogicalOperatorType::LOGICAL_CTE_REF && op->Cast<LogicalCTERef>().cte_index == domain_cte_index) {
		locations.push_back(op);
		return;
	}
	for (auto &child : op->children) {
		FindDomainReferences(child, domain_cte_index, locations);
	}
}

static unique_ptr<LogicalOperator> CreateDuplicateFreeDomain(Binder &binder, unique_ptr<LogicalOperator> &source,
                                                             const DuplicateEliminatedDomainCandidate &candidate,
                                                             LogicalCTERef &domain_ref) {
	LogicalOperatorDeepCopy deep_copy(binder, nullptr);
	auto source_copy = deep_copy.DeepCopy(source);
	auto distinct_domain = DuplicateEliminatedDomainBuilder::TryBuild(binder, std::move(source_copy),
	                                                                  candidate.KeyIndices(), domain_ref.chunk_types);
	if (!distinct_domain) {
		return nullptr;
	}

	vector<unique_ptr<Expression>> expressions;
	auto domain_bindings = distinct_domain->GetColumnBindings();
	expressions.reserve(domain_bindings.size());
	for (idx_t key_idx = 0; key_idx < domain_bindings.size(); key_idx++) {
		expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(domain_ref.chunk_types[key_idx], domain_bindings[key_idx]));
	}
	auto projection = make_uniq<LogicalProjection>(domain_ref.table_index, std::move(expressions));
	projection->children.push_back(std::move(distinct_domain));
	projection->ResolveOperatorTypes();
	if (projection->GetColumnBindings() != domain_ref.GetColumnBindings()) {
		throw InternalException("Inlined duplicate-eliminated domain changed its output bindings");
	}
	return projection;
}

bool DuplicateEliminatedDomainInliner::TryInline(Binder &binder, unique_ptr<LogicalOperator> &rhs,
                                                 unique_ptr<LogicalOperator> &source, TableIndex domain_cte_index,
                                                 idx_t domain_ref_count,
                                                 const DuplicateEliminatedDomainCandidate &candidate) {
	if (domain_ref_count != 1 || !FindDomainAggregateUse(*rhs, domain_cte_index).feeds_aggregate ||
	    !DuplicateEliminatedDomainSafety::CanDuplicateSource(binder.context, *source)) {
		return false;
	}
	auto scanned_table = GetScannedTable(*source);
	if (scanned_table && ScansTable(*rhs, *scanned_table, domain_cte_index)) {
		return false;
	}

	operator_location_list_t locations;
	FindDomainReferences(rhs, domain_cte_index, locations);
	if (locations.size() != domain_ref_count) {
		return false;
	}

	vector<unique_ptr<LogicalOperator>> replacements;
	replacements.reserve(locations.size());
	try {
		for (auto &location : locations) {
			auto &domain_ref = location.get()->Cast<LogicalCTERef>();
			auto replacement = CreateDuplicateFreeDomain(binder, source, candidate, domain_ref);
			if (!replacement) {
				return false;
			}
			replacements.push_back(std::move(replacement));
		}
	} catch (NotImplementedException &) {
		return false;
	}

	for (idx_t location_idx = 0; location_idx < locations.size(); location_idx++) {
		locations[location_idx].get() = std::move(replacements[location_idx]);
	}
	return true;
}

} // namespace duckdb
