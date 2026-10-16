#include "duckdb/optimizer/constraint_propagation/queries.hpp"

#include "duckdb/optimizer/constraint_propagation/helpers.hpp"
#include "duckdb/optimizer/constraint_propagation/constraint_propagator.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

bool IsUniqueOn(const ConstraintPropagator &p, const LogicalOperator &scope, const ColumnMask &cols,
                bool require_null_safe) {
	if (cols.IsEmpty()) {
		return false;
	}
	return p.Facts(scope).IsUniqueOn(cols, require_null_safe);
}

bool IsNotNullOn(const ConstraintPropagator &p, const LogicalOperator &scope, const ColumnMask &cols) {
	if (cols.IsEmpty()) {
		return false;
	}
	return cols.IsSubsetOf(p.Facts(scope).NotNull());
}

bool IsForeignKeyTo(const ConstraintPropagator &p, const LogicalOperator &scope, const ColumnMask &cols,
                    const TableCatalogEntry &target) {
	const auto &f = p.Facts(scope);
	Identifier target_schema(target.schema.name);
	Identifier target_name(target.name);
	for (auto &fk : f.FKs()) {
		if (!fk.IsValid()) {
			continue;
		}
		if (fk.target_schema != target_schema || fk.target_name != target_name) {
			continue;
		}
		if (fk.PositionsAllIn(cols)) {
			return true;
		}
	}
	return false;
}

bool EquiKeys(const ConstraintPropagator &p, const LogicalComparisonJoin &join, ColumnMask &left_keys,
              ColumnMask &right_keys) {
	idx_t matched = 0;
	if (!CollectEquiKeys(p.Store(), join, left_keys, right_keys, &matched)) {
		return false;
	}
	return matched == join.conditions.size();
}

//! Physical columns that are guaranteed non-null by the schema (NOT NULL constraint or PRIMARY KEY).
static unordered_set<idx_t> ComputeCatalogNotNullColumns(const TableCatalogEntry &table) {
	unordered_set<idx_t> result;
	auto &columns = table.GetColumns();
	for (auto &constraint : table.GetConstraints()) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
			result.insert(columns.GetColumn(constraint->Cast<NotNullConstraint>().index).Physical().index);
			break;
		case ConstraintType::UNIQUE: {
			auto &unique = constraint->Cast<UniqueConstraint>();
			if (!unique.is_primary_key) {
				break;
			}
			for (auto &log_idx : unique.GetLogicalIndexes(columns)) {
				result.insert(columns.GetColumn(log_idx).Physical().index);
			}
			break;
		}
		default:
			break;
		}
	}
	return result;
}

//! Did a filter touch this position?
static bool PositionHasFilterEffect(const ValueDomain &d, idx_t phys, const unordered_set<idx_t> &catalog_not_null) {
	if (d.HasValueConstraint()) {
		return true;
	}
	if (d.null_possible || phys == DConstants::INVALID_INDEX) {
		return false;
	}
	return catalog_not_null.find(phys) == catalog_not_null.end();
}

//! Position on the probe side paired with `ref_phys`.
static idx_t FindPairedProbePosition(const FKFact &fk, idx_t ref_phys) {
	for (idx_t j = 0; j < fk.referenced_keys.size(); j++) {
		if (fk.referenced_keys[j] == ref_phys) {
			return fk.cols[j];
		}
	}
	return DConstants::INVALID_INDEX;
}

//! Every filter effect on the ref side must be FK-paired
//! And the probe's domain on the pair must be a subset of the ref's.
static bool RefSideFiltersSatisfied(const ScopeFacts &other_facts, const ScopeFacts &side_facts, const FKFact &fk) {
	auto catalog_not_null = ComputeCatalogNotNullColumns(*other_facts.base_table);

	for (idx_t pos = 0; pos < other_facts.domains.size(); pos++) {
		idx_t phys = other_facts.base_column[pos];
		const auto &d = other_facts.domains[pos];
		if (!PositionHasFilterEffect(d, phys, catalog_not_null)) {
			continue;
		}
		if (phys == DConstants::INVALID_INDEX) {
			return false; // filter on a computed output
		}
		idx_t paired = FindPairedProbePosition(fk, phys);
		if (paired == DConstants::INVALID_INDEX) {
			return false; // filter on a non-paired column
		}
		if (!side_facts.Domain(paired).IsSubsetOf(d)) {
			return false; // probe's domain not covered by the ref filter
		}
	}
	return true;
}

bool JoinCoverage(const ConstraintPropagator &p, const LogicalComparisonJoin &join, idx_t side) {
	if (side > 1 || join.children.size() != 2) {
		return false;
	}

	vector<std::pair<idx_t, idx_t>> pairs;
	idx_t matched = 0;
	if (!CollectEquiKeyPairs(p.Store(), join, pairs, &matched)) {
		return false;
	}
	if (join.conditions.empty() || matched != join.conditions.size() || pairs.empty()) {
		return false;
	}

	const LogicalOperator &side_op = *join.children[side];
	const LogicalOperator &other_op = *join.children[1 - side];
	const ScopeFacts &side_facts = p.Facts(side_op);
	const ScopeFacts &other_facts = p.Facts(other_op);

	// The other side must be a single-table pipeline
	if (!other_facts.base_table || other_facts.rows_dropped_below) {
		return false;
	}

	// Orient the pairs
	vector<std::pair<idx_t, idx_t>> side_pairs;
	side_pairs.reserve(pairs.size());
	for (auto &kv : pairs) {
		idx_t side_pos = side == 0 ? kv.first : kv.second;
		idx_t other_pos = side == 0 ? kv.second : kv.first;
		if (other_pos >= other_facts.base_column.size() ||
		    other_facts.base_column[other_pos] == DConstants::INVALID_INDEX) {
			return false;
		}
		side_pairs.emplace_back(side_pos, other_facts.base_column[other_pos]);
	}

	const Identifier other_schema(other_facts.base_table->schema.name);
	const Identifier other_name(other_facts.base_table->name);

	for (auto &f : side_facts.FKs()) {
		D_ASSERT(f.IsValid());
		if (f.target_schema != other_schema || f.target_name != other_name) {
			continue;
		}

		// Every join pair must be an FK pair.
		bool all_pairs_covered = true;
		for (auto &sp : side_pairs) {
			bool pair_covered = false;
			for (idx_t j = 0; j < f.cols.size(); j++) {
				if (f.cols[j] == sp.first && f.referenced_keys[j] == sp.second) {
					pair_covered = true;
					break;
				}
			}
			if (!pair_covered) {
				all_pairs_covered = false;
				break;
			}
		}
		if (!all_pairs_covered) {
			continue;
		}

		// FK guarantee requires every FK column non-null.
		if (!f.PositionsAllIn(side_facts.NotNull())) {
			continue;
		}

		// Every filter effect on the ref side must be provably satisfied by the FK witness row.
		if (!RefSideFiltersSatisfied(other_facts, side_facts, f)) {
			continue;
		}

		return true;
	}
	return false;
}

//! Self-join drop-side filter elimination
bool DropFilterEffectsAreRedundant(const ScopeFacts &keep_facts, const ScopeFacts &drop_facts) {
	auto catalog_nn = ComputeCatalogNotNullColumns(*drop_facts.base_table);

	for (idx_t pos_d = 0; pos_d < drop_facts.domains.size(); pos_d++) {
		D_ASSERT(pos_d < drop_facts.base_column.size());
		idx_t phys = drop_facts.base_column[pos_d];
		const auto &drop_d = drop_facts.domains[pos_d];
		if (!PositionHasFilterEffect(drop_d, phys, catalog_nn)) {
			continue; // no filter touched this position
		}
		if (phys == DConstants::INVALID_INDEX) {
			return false; // filter on a computed output
		}
		// Find keep's position at the same physical column.
		idx_t pos_k = DConstants::INVALID_INDEX;
		for (idx_t k = 0; k < keep_facts.base_column.size(); k++) {
			if (keep_facts.base_column[k] == phys) {
				pos_k = k;
				break;
			}
		}
		if (pos_k == DConstants::INVALID_INDEX) {
			return false; // keep doesn't project this column
		}
		if (!keep_facts.Domain(pos_k).IsSubsetOf(drop_d)) {
			return false; // keep has values drop doesn't allow
		}
	}
	return true;
}

SideMultiplicity MultiplicityOf(const ConstraintPropagator &p, const LogicalComparisonJoin &join, idx_t side) {
	if (side > 1 || join.children.size() != 2) {
		return SideMultiplicity::UNKNOWN;
	}

	if (join.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		switch (join.join_type) {
		case JoinType::LEFT:
			if (side == 0) {
				return SideMultiplicity::EXACTLY_ONE;
			}
			return SideMultiplicity::UNKNOWN;
		case JoinType::INNER:
		case JoinType::SEMI:
		case JoinType::ANTI:
			if (side == 0) {
				return SideMultiplicity::AT_MOST_ONE;
			}
			return SideMultiplicity::UNKNOWN;
		case JoinType::RIGHT:
			if (side == 1) {
				return SideMultiplicity::EXACTLY_ONE;
			}
			return SideMultiplicity::UNKNOWN;
		case JoinType::OUTER:
			if (side == 0) {
				return SideMultiplicity::AT_MOST_ONE;
			}
			return SideMultiplicity::UNKNOWN;
		default:
			return SideMultiplicity::UNKNOWN;
		}
	}

	idx_t matched = 0;
	ColumnMask key0, key1;
	CollectEquiKeys(p.Store(), join, key0, key1, &matched);
	const ColumnMask &other_key = (side == 0) ? key1 : key0;
	const LogicalOperator &other_op = *join.children[1 - side];
	bool other_unique = p.Facts(other_op).IsUniqueOn(other_key, false);

	switch (join.join_type) {
	case JoinType::INNER:
		if (other_unique) {
			if (JoinCoverage(p, join, side)) {
				return SideMultiplicity::EXACTLY_ONE;
			}
			return SideMultiplicity::AT_MOST_ONE;
		}
		return SideMultiplicity::UNKNOWN;
	case JoinType::LEFT:
		if (!other_unique) {
			return SideMultiplicity::UNKNOWN;
		}
		if (side == 0) {
			return SideMultiplicity::EXACTLY_ONE;
		}
		if (JoinCoverage(p, join, side)) {
			return SideMultiplicity::EXACTLY_ONE;
		}
		return SideMultiplicity::AT_MOST_ONE;
	case JoinType::RIGHT:
		if (!other_unique) {
			return SideMultiplicity::UNKNOWN;
		}
		if (side == 1) {
			return SideMultiplicity::EXACTLY_ONE;
		}
		if (JoinCoverage(p, join, side)) {
			return SideMultiplicity::EXACTLY_ONE;
		}
		return SideMultiplicity::AT_MOST_ONE;
	case JoinType::SEMI:
		if (side == 0) {
			return JoinCoverage(p, join, 0) ? SideMultiplicity::EXACTLY_ONE : SideMultiplicity::AT_MOST_ONE;
		}
		return SideMultiplicity::UNKNOWN;
	case JoinType::ANTI:
		return side == 0 ? SideMultiplicity::AT_MOST_ONE : SideMultiplicity::UNKNOWN;
	case JoinType::MARK:
	case JoinType::SINGLE:
		return side == 0 ? SideMultiplicity::EXACTLY_ONE : SideMultiplicity::UNKNOWN;
	default:
		return SideMultiplicity::UNKNOWN;
	}
}

} // namespace duckdb
