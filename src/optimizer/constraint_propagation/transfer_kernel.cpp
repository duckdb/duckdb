#include "duckdb/optimizer/constraint_propagation/transfer_kernel.hpp"

#include "duckdb/optimizer/constraint_propagation/constraint_propagator.hpp"
#include "duckdb/optimizer/constraint_propagation/fact_store.hpp"
#include "duckdb/optimizer/constraint_propagation/helpers.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// File-local helpers
//===----------------------------------------------------------------------===//
//! The fact dies unless every column survives (tuple facts: unique, FK).
static bool RemapMaskStrict(const ColumnMask &m, const vector<idx_t> &map, idx_t width, ColumnMask &out) {
	out = ColumnMask(width);
	bool ok = true;
	m.ForEachPosition([&](idx_t p) -> bool {
		if (p >= map.size() || map[p] == DConstants::INVALID_INDEX) {
			ok = false;
			return false;
		}
		out.Set(map[p]);
		return true;
	});
	if (!ok) {
		out = ColumnMask::Empty();
	}
	return ok;
}

//! Per-column facts (not_null) — map what survives, skip the rest.
static ColumnMask RemapMaskPartial(const ColumnMask &m, const vector<idx_t> &map, idx_t width) {
	ColumnMask out(width);
	m.ForEachPosition([&](idx_t p) -> bool {
		if (p < map.size() && map[p] != DConstants::INVALID_INDEX) {
			out.Set(map[p]);
		}
		return true;
	});
	return out;
}

static void RemapUniqueFacts(const vector<UniqueFact> &facts, const vector<idx_t> &map, idx_t width,
                             ScopeFacts &props) {
	for (auto &uf : facts) {
		ColumnMask c;
		if (RemapMaskStrict(uf.cols, map, width, c)) {
			props.AddUniqueFact(UniqueFact {c, uf.null_distinct});
		}
	}
}

static void RemapFKFacts(const vector<FKFact> &fks, const vector<idx_t> &map, idx_t width, ScopeFacts &props) {
	for (auto &fk : fks) {
		ColumnMask c;
		if (!RemapMaskStrict(fk.cols, map, width, c)) {
			continue;
		}
		FKFact nf;
		nf.target_schema = fk.target_schema;
		nf.target_name = fk.target_name;
		nf.cols = c;
		nf.referenced_keys = fk.referenced_keys;
		props.AddFKFact(std::move(nf));
	}
}

static optional_idx FindPositionOfPhysical(const vector<idx_t> &base_column, idx_t phys_index) {
	for (idx_t i = 0; i < base_column.size(); i++) {
		if (base_column[i] == phys_index) {
			return i;
		}
	}
	return optional_idx();
}

static vector<idx_t> BuildChildToOutputMap(FactStore &store, LogicalOperator &child,
                                           const vector<ColumnBinding> &parent_bindings) {
	const auto &child_bindings = store.OutputBindings(child);
	vector<idx_t> map(child_bindings.size(), DConstants::INVALID_INDEX);
	column_binding_map_t<idx_t> parent_map;
	for (idx_t j = 0; j < parent_bindings.size(); j++) {
		parent_map[parent_bindings[j]] = j;
	}
	for (idx_t i = 0; i < child_bindings.size(); i++) {
		auto it = parent_map.find(child_bindings[i]);
		if (it != parent_map.end()) {
			map[i] = it->second;
		}
	}
	return map;
}

static vector<idx_t> BuildExprPositionMap(const vector<unique_ptr<Expression>> &exprs,
                                          const vector<ColumnBinding> &child_bindings, idx_t max_positions) {
	column_binding_map_t<idx_t> first_occurrence;
	idx_t n = MinValue<idx_t>(exprs.size(), max_positions);
	for (idx_t i = 0; i < n; i++) {
		auto &expr = exprs[i];
		if (expr && expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			auto b = expr->Cast<BoundColumnRefExpression>().Binding();
			if (first_occurrence.find(b) == first_occurrence.end()) {
				first_occurrence[b] = i;
			}
		}
	}
	vector<idx_t> map(child_bindings.size(), DConstants::INVALID_INDEX);
	for (idx_t p = 0; p < child_bindings.size(); p++) {
		auto it = first_occurrence.find(child_bindings[p]);
		if (it != first_occurrence.end()) {
			map[p] = it->second;
		}
	}
	return map;
}

static void TransferJoinUniqueFacts(FactStore &store, LogicalOperator &op, ScopeFacts &props,
                                    const vector<UniqueFact> &f0_unique, const vector<UniqueFact> &f1_unique,
                                    bool side0_rowset, bool side1_rowset) {
	const auto &out_bindings = store.OutputBindings(op);
	idx_t width = out_bindings.size();
	if (side0_rowset) {
		auto map0 = BuildChildToOutputMap(store, *op.children[0], out_bindings);
		RemapUniqueFacts(f0_unique, map0, width, props);
	}
	if (side1_rowset) {
		auto map1 = BuildChildToOutputMap(store, *op.children[1], out_bindings);
		RemapUniqueFacts(f1_unique, map1, width, props);
	}
}

//===----------------------------------------------------------------------===//
// Kernel
//===----------------------------------------------------------------------===//
TransferKernel::TransferKernel(ConstraintPropagator &owner) : owner_(owner) {
}

void TransferKernel::Walk(LogicalOperator &root) {
	Visit(root);
}

void TransferKernel::Visit(LogicalOperator &op) {
	// Post-order: children first.
	owner_.store_.OutputBindings(op);
	for (auto &child : op.children) {
		Visit(*child);
	}

	auto &props = owner_.store_.GetOrCreate(&op);
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		VisitGet(op, props);
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		VisitProjection(op, props);
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_SAMPLE:
		VisitPassthrough(op, props);
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		VisitAggregate(op, props);
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		VisitDistinct(op, props);
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		VisitComparisonJoin(op, props);
		break;
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		VisitAsofJoin(op, props);
		break;
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		VisitDelimJoin(op, props);
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		VisitAnyJoin(op, props);
		break;
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		VisitCrossProduct(op, props);
		break;
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
		VisitSetOperation(op, props);
		break;
	default:
		break;
	}
}

void TransferKernel::VisitGet(LogicalOperator &op, ScopeFacts &props) {
	auto &get = op.Cast<LogicalGet>();
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		return;
	}
	auto &table = *table_ptr;
	auto &columns = table.GetColumns();
	auto &column_ids = get.GetColumnIds();

	idx_t width = owner_.store_.OutputBindings(op).size();
	props.base_table = &table;
	props.filter_below = get.table_filters.HasFilters();
	props.base_column.assign(width, DConstants::INVALID_INDEX);
	for (idx_t i = 0; i < width && i < column_ids.size(); i++) {
		if (column_ids[i].HasPrimaryIndex()) {
			props.base_column[i] = column_ids[i].GetPrimaryIndex();
		}
	}

	// Collect NOT NULL logical indexes AND record positions, in one pass.
	unordered_set<idx_t> not_null_logical;
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::NOT_NULL) {
			continue;
		}
		auto &not_null = constraint->Cast<NotNullConstraint>();
		not_null_logical.insert(not_null.index.index);

		auto phy = columns.GetColumn(not_null.index).Physical();
		auto pos = FindPositionOfPhysical(props.base_column, phy.index);
		if (pos.IsValid()) {
			props.AddNotNullBit(pos.GetIndex());
		}
	}

	// UNIQUE / PRIMARY KEY and FOREIGN KEY.
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type == ConstraintType::UNIQUE) {
			auto &unique = constraint->Cast<UniqueConstraint>();
			auto logical_indexes = unique.GetLogicalIndexes(columns);
			UniqueFact fact;
			bool all_found = true;
			for (auto &log_idx : logical_indexes) {
				auto phys = columns.GetColumn(log_idx).Physical();
				auto pos = FindPositionOfPhysical(props.base_column, phys.index);
				if (!pos.IsValid()) {
					all_found = false;
					break;
				}
				fact.cols.Set(pos.GetIndex());
			}
			if (!all_found || logical_indexes.empty()) {
				continue; // key not fully in the output
			}
			bool key_not_null = true;
			for (auto &log_idx : logical_indexes) {
				if (!not_null_logical.count(log_idx.index)) {
					key_not_null = false;
					break;
				}
			}
			fact.null_distinct = !(unique.is_primary_key || key_not_null);
			props.AddUniqueFact(std::move(fact));
			if (unique.is_primary_key) {
				// PK implies NOT NULL on the key columns.
				for (auto &log_idx : logical_indexes) {
					auto phys = columns.GetColumn(log_idx).Physical();
					auto pos = FindPositionOfPhysical(props.base_column, phys.index);
					if (pos.IsValid()) {
						props.AddNotNullBit(pos.GetIndex());
					}
				}
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				FKFact fact;
				fact.target_schema =
				    fk.info.schema.empty() ? Identifier(table.schema.name) : Identifier(fk.info.schema);
				fact.target_name = Identifier(fk.info.table);
				bool all_found = true;
				for (auto &phys : fk.info.fk_keys) {
					auto pos = FindPositionOfPhysical(props.base_column, phys.index);
					if (!pos.IsValid()) {
						all_found = false;
						break;
					}
					fact.cols.Set(pos.GetIndex());
				}
				for (auto &phys : fk.info.pk_keys) {
					fact.referenced_keys.push_back(phys.index);
				}
				if (all_found && !fact.cols.IsEmpty()) {
					props.AddFKFact(std::move(fact));
				}
			}
		}
	}
}

void TransferKernel::VisitProjection(LogicalOperator &op, ScopeFacts &props) {
	auto &proj = op.Cast<LogicalProjection>();
	auto &child = *op.children[0];
	const auto &child_facts = owner_.store_.Get(child);
	const auto &out_bindings = owner_.store_.OutputBindings(op);
	const auto &child_bindings = owner_.store_.OutputBindings(child);
	idx_t width = out_bindings.size();

	auto map = BuildExprPositionMap(proj.expressions, child_bindings, width);

	RemapUniqueFacts(child_facts.Unique(), map, width, props);
	props.SetNotNull(RemapMaskPartial(child_facts.NotNull(), map, width));
	RemapFKFacts(child_facts.FKs(), map, width, props);

	props.base_table = child_facts.base_table;
	props.base_column.assign(width, DConstants::INVALID_INDEX);
	for (idx_t p = 0; p < map.size(); p++) {
		if (map[p] != DConstants::INVALID_INDEX && p < child_facts.base_column.size() &&
		    child_facts.base_column[p] != DConstants::INVALID_INDEX) {
			props.base_column[map[p]] = child_facts.base_column[p];
		}
	}
	props.filter_below = child_facts.filter_below;
}

void TransferKernel::VisitPassthrough(LogicalOperator &op, ScopeFacts &props) {
	auto &child = *op.children[0];
	const auto &child_facts = owner_.store_.Get(child);
	props.SetUniqueFacts(child_facts.Unique());
	props.SetNotNull(child_facts.NotNull());
	props.SetFKFacts(child_facts.FKs());
	props.base_table = child_facts.base_table;
	props.base_column = child_facts.base_column;
	props.filter_below = child_facts.filter_below;
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_SAMPLE:
		props.filter_below = true;
		break;
	default:
		break;
	}
	// TODO (filter-derived not_null): a FILTER can ADD not_null facts from
	// IS NOT NULL conjuncts and comparisons with non-null constants
}

void TransferKernel::VisitAggregate(LogicalOperator &op, ScopeFacts &props) {
	auto &aggr = op.Cast<LogicalAggregate>();
	if (aggr.grouping_sets.size() > 1) {
		return;
	}
	auto &child = *op.children[0];
	const auto &child_facts = owner_.store_.Get(child);
	const auto &child_bindings = owner_.store_.OutputBindings(child);
	idx_t width = owner_.store_.OutputBindings(op).size();

	auto map = BuildExprPositionMap(aggr.groups, child_bindings, width);

	// A group set is a null-safe unique key
	UniqueFact group_fact;
	group_fact.cols = ColumnMask(width);
	for (idx_t i = 0; i < aggr.groups.size() && i < width; i++) {
		group_fact.cols.Set(i);
	}
	group_fact.null_distinct = false;
	props.AddUniqueFact(std::move(group_fact));

	props.SetNotNull(RemapMaskPartial(child_facts.NotNull(), map, width));
	RemapFKFacts(child_facts.FKs(), map, width, props);

	props.base_table = nullptr;
	props.filter_below = child_facts.filter_below;
}

void TransferKernel::VisitDistinct(LogicalOperator &op, ScopeFacts &props) {
	auto &distinct = op.Cast<LogicalDistinct>();
	auto &child = *op.children[0];
	const auto &child_facts = owner_.store_.Get(child);
	const auto &out_bindings = owner_.store_.OutputBindings(op);
	const auto &child_bindings = owner_.store_.OutputBindings(child);
	idx_t width = out_bindings.size();

	D_ASSERT(out_bindings.size() == child_bindings.size());
	for (idx_t i = 0; i < width; i++) {
		D_ASSERT(out_bindings[i] == child_bindings[i]);
	}

	props.SetNotNull(child_facts.NotNull());
	props.SetFKFacts(child_facts.FKs());
	props.base_table = child_facts.base_table;
	props.base_column = child_facts.base_column;
	props.filter_below = child_facts.filter_below;

	if (distinct.distinct_type == DistinctType::DISTINCT_ON) {
		props.filter_below = true;
	}

	// Uniqueness is always null-safe here
	if (distinct.distinct_targets.empty()) {
		UniqueFact f;
		f.cols = ColumnMask(width);
		for (idx_t i = 0; i < width; i++) {
			f.cols.Set(i);
		}
		f.null_distinct = false;
		props.AddUniqueFact(std::move(f));
	} else {
		vector<idx_t> positions;
		bool ok = true;
		for (auto &target : distinct.distinct_targets) {
			if (target->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				ok = false;
				break;
			}
			auto b = target->Cast<BoundColumnRefExpression>().Binding();
			auto pos = PositionIn(out_bindings, b);
			if (!pos.IsValid()) {
				ok = false;
				break;
			}
			positions.push_back(pos.GetIndex());
		}
		if (ok && !positions.empty()) {
			UniqueFact f;
			f.cols = ColumnMask::FromPositions(positions, width);
			f.null_distinct = false;
			props.AddUniqueFact(std::move(f));
		}
	}
}

void TransferKernel::VisitSetOperation(LogicalOperator &op, ScopeFacts &props) {
	if (op.children.size() != 2) {
		return;
	}
	idx_t width = owner_.store_.OutputBindings(op).size();
	const auto &a = owner_.store_.Get(*op.children[0]);
	const auto &b = owner_.store_.Get(*op.children[1]);
	D_ASSERT(owner_.store_.OutputBindings(*op.children[0]).size() == width &&
	         owner_.store_.OutputBindings(*op.children[1]).size() == width);

	auto same_fk = [](const FKFact &fa, const FKFact &fb) {
		return fa.cols == fb.cols && fa.target_schema == fb.target_schema && fa.target_name == fb.target_name &&
		       fa.referenced_keys == fb.referenced_keys;
	};

	if (op.type == LogicalOperatorType::LOGICAL_UNION) {
		// A value fact must hold in BOTH.
		props.SetNotNull(a.NotNull().Intersection(b.NotNull()));
		for (auto &fa : a.FKs()) {
			for (auto &fb : b.FKs()) {
				if (same_fk(fa, fb)) {
					props.AddFKFact(fa);
					break;
				}
			}
		}
	} else if (op.type == LogicalOperatorType::LOGICAL_INTERSECT) {
		props.SetNotNull(a.NotNull().Union(b.NotNull()));
		for (auto &fa : a.FKs()) {
			props.AddFKFact(fa);
		}
		for (auto &fb : b.FKs()) {
			props.AddFKFact(fb);
		}
	} else {
		props.SetNotNull(a.NotNull());
		for (auto &fa : a.FKs()) {
			props.AddFKFact(fa);
		}
	}
	// INTERSECT / EXCEPT have set semantics, output rows are pairwise distinct.
	if (op.type != LogicalOperatorType::LOGICAL_UNION) {
		UniqueFact f;
		f.cols = ColumnMask(width);
		for (idx_t i = 0; i < width; i++) {
			f.cols.Set(i);
		}
		f.null_distinct = false;
		props.AddUniqueFact(std::move(f));
	}
	props.base_table = nullptr;
	props.filter_below = true;
}

void TransferKernel::TransferJoinValueFacts(LogicalOperator &op, JoinType join_type, ScopeFacts &props,
                                            bool conservative_not_null) {
	const auto &f0 = owner_.store_.Get(*op.children[0]);
	const auto &f1 = owner_.store_.Get(*op.children[1]);
	const auto &out_bindings = owner_.store_.OutputBindings(op);
	idx_t width = out_bindings.size();
	auto map0 = BuildChildToOutputMap(owner_.store_, *op.children[0], out_bindings);
	auto map1 = BuildChildToOutputMap(owner_.store_, *op.children[1], out_bindings);

	bool nn0 = false, nn1 = false;
	switch (join_type) {
	case JoinType::INNER:
		nn0 = nn1 = true;
		break;
	case JoinType::LEFT:
		nn0 = true;
		break;
	case JoinType::RIGHT:
		nn1 = true;
		break;
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		nn0 = nn1 = true;
		break;
	case JoinType::SINGLE:
		nn0 = true;
		break;
	case JoinType::OUTER:
	default:
		break;
	}
	if (conservative_not_null) {
		nn0 = nn1 = false;
	}

	ColumnMask nn(width);
	if (nn0) {
		nn = nn.Union(RemapMaskPartial(f0.NotNull(), map0, width));
	}
	if (nn1) {
		nn = nn.Union(RemapMaskPartial(f1.NotNull(), map1, width));
	}
	props.SetNotNull(std::move(nn));

	RemapFKFacts(f0.FKs(), map0, width, props);
	RemapFKFacts(f1.FKs(), map1, width, props);

	props.filter_below = f0.filter_below || f1.filter_below;
}

void TransferKernel::VisitComparisonJoin(LogicalOperator &op, ScopeFacts &props) {
	auto &join = op.Cast<LogicalComparisonJoin>();
	TransferJoinValueFacts(op, join.join_type, props, false);

	const auto &f0 = owner_.store_.Get(*op.children[0]);
	const auto &f1 = owner_.store_.Get(*op.children[1]);
	const auto &out_bindings = owner_.store_.OutputBindings(op);
	auto map0 = BuildChildToOutputMap(owner_.store_, *op.children[0], out_bindings);
	auto map1 = BuildChildToOutputMap(owner_.store_, *op.children[1], out_bindings);

	idx_t matched = 0;
	ColumnMask key0, key1;
	CollectEquiKeys(owner_.store_, join, key0, key1, &matched);

	// side X's rows are unique iff the OTHER side is unique on its equi key
	bool side0_rowset = false, side1_rowset = false;
	switch (join.join_type) {
	case JoinType::INNER:
	case JoinType::LEFT:
	case JoinType::RIGHT:
		side0_rowset = f1.IsUniqueOn(key1, false);
		side1_rowset = f0.IsUniqueOn(key0, false);
		break;
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
	case JoinType::SINGLE:
		side0_rowset = true;
		break;
	default:
		break;
	}

	TransferJoinUniqueFacts(owner_.store_, op, props, f0.Unique(), f1.Unique(), side0_rowset, side1_rowset);
}

void TransferKernel::VisitAsofJoin(LogicalOperator &op, ScopeFacts &props) {
	auto &join = op.Cast<LogicalComparisonJoin>();
	TransferJoinValueFacts(op, join.join_type, props, false);

	const auto &f0 = owner_.store_.Get(*op.children[0]);
	const auto &f1 = owner_.store_.Get(*op.children[1]);

	bool side0_rowset = false, side1_rowset = false;
	switch (join.join_type) {
	case JoinType::INNER:
	case JoinType::LEFT:
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::OUTER:
		side0_rowset = true;
		break;
	case JoinType::RIGHT:
		side1_rowset = true;
		break;
	default:
		break;
	}

	TransferJoinUniqueFacts(owner_.store_, op, props, f0.Unique(), f1.Unique(), side0_rowset, side1_rowset);
}

void TransferKernel::VisitDelimJoin(LogicalOperator &op, ScopeFacts &props) {
	auto &join = op.Cast<LogicalComparisonJoin>();
	TransferJoinValueFacts(op, join.join_type, props, true);
}

void TransferKernel::VisitAnyJoin(LogicalOperator &op, ScopeFacts &props) {
	auto &any_join = op.Cast<LogicalAnyJoin>();
	TransferJoinValueFacts(op, any_join.join_type, props, false);
}

void TransferKernel::VisitCrossProduct(LogicalOperator &op, ScopeFacts &props) {
	TransferJoinValueFacts(op, JoinType::INNER, props, false);
}

} // namespace duckdb
