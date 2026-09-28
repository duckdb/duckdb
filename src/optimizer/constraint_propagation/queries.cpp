#include "duckdb/optimizer/constraint_propagation/queries.hpp"

#include "duckdb/optimizer/constraint_propagation/helpers.hpp"
#include "duckdb/optimizer/constraint_propagation/constraint_propagator.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"
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
		if (fk.cols.IsSubsetOf(cols) && fk.target_schema == target_schema && fk.target_name == target_name) {
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

bool JoinCoverage(const ConstraintPropagator &p, const LogicalComparisonJoin &join, idx_t side) {
	if (side > 1 || join.children.size() != 2) {
		return false;
	}
	idx_t matched = 0;
	ColumnMask key0, key1;
	if (!CollectEquiKeys(p.Store(), join, key0, key1, &matched)) {
		return false;
	}
	if (join.conditions.empty() || matched != join.conditions.size()) {
		return false;
	}
	const ColumnMask &side_key = (side == 0) ? key0 : key1;
	const ColumnMask &other_key = (side == 0) ? key1 : key0;
	if (side_key.IsEmpty()) {
		return false;
	}

	const LogicalOperator &side_op = *join.children[side];
	const LogicalOperator &other_op = *join.children[1 - side];
	const ScopeFacts &side_facts = p.Facts(side_op);
	const ScopeFacts &other_facts = p.Facts(other_op);

	// A NULL FK value legally matches nothing.
	if (!side_key.IsSubsetOf(side_facts.NotNull())) {
		return false;
	}
	// The other side must be a filterless single-table pipeline.
	if (!other_facts.base_table || other_facts.filter_below) {
		return false;
	}

	// Map other_key positions to physical columns of the other side's base table.
	unordered_set<idx_t> key_phys;
	bool traceable = true;
	other_key.ForEachPosition([&](idx_t pos) -> bool {
		if (pos >= other_facts.base_column.size() || other_facts.base_column[pos] == DConstants::INVALID_INDEX) {
			traceable = false;
			return false;
		}
		key_phys.insert(other_facts.base_column[pos]);
		return true;
	});
	if (!traceable || key_phys.size() != other_key.PopCount()) {
		return false;
	}

	const Identifier other_schema(other_facts.base_table->schema.name);
	const Identifier other_name(other_facts.base_table->name);

	// Find an FK on the side such that:
	//   1. the join key on the side is a subset of the FK's columns,
	//   2. the FK targets the other side's base table,
	//   3. the join key on the other side is a subset of the FK's referenced keys.
	const FKFact *fk = nullptr;
	for (auto &f : side_facts.FKs()) {
		if (!side_key.IsSubsetOf(f.cols)) {
			continue;
		}
		if (f.target_schema != other_schema || f.target_name != other_name) {
			continue;
		}
		unordered_set ref_phys(f.referenced_keys.begin(), f.referenced_keys.end());
		bool ref_covers = true;
		for (auto k : key_phys) {
			if (ref_phys.find(k) == ref_phys.end()) {
				ref_covers = false;
				break;
			}
		}
		if (!ref_covers) {
			continue;
		}
		fk = &f;
		break;
	}
	if (!fk) {
		return false;
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
