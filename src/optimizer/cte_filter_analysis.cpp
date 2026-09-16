#include "duckdb/optimizer/cte_filter_analysis.hpp"

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {
namespace {

// ROWS preserves the bag of rows in each required partition. KEYS only preserves the
// existence of each key, and is introduced exclusively by duplicate elimination.
enum class CTEDemand : uint8_t { ROWS, KEYS };
struct DemandKey {
	ColumnBinding binding;
	ProjectionIndex source_column;
};
using DemandKeys = vector<DemandKey>;

class ConsumerAnalysis {
public:
	ConsumerAnalysis(LogicalOperator &root, TableIndex target, const vector<reference<LogicalOperator>> &filters)
	    : target(target) {
		Index(root);
		for (auto &filter : filters) {
			row_consumers.insert(&filter.get().children[0]->Cast<LogicalCTERef>());
		}
	}

	// Every reference needs a certificate; an unobserved or unsupported consumer prevents pruning.
	bool Analyze(LogicalOperator &root) {
		// Restricting the producer must not change values of retained rows through side effects.
		if (!Stable(*definitions.at(target)->children[0])) {
			return false;
		}
		FindDemands(root);
		for (auto ref : references[target]) {
			if (!row_consumers.count(ref) && !key_consumers.count(ref)) {
				return false;
			}
		}
		return !row_consumers.empty();
	}

private:
	TableIndex target;
	unordered_map<TableIndex, LogicalMaterializedCTE *> definitions;
	unordered_map<TableIndex, vector<LogicalCTERef *>> references;
	unordered_set<LogicalCTERef *> row_consumers;
	unordered_set<LogicalCTERef *> key_consumers;
	unordered_map<LogicalOperator *, bool> stable_cache;
	unordered_map<LogicalOperator *, bool> dependency_cache;

	// Index definitions and all their uses before following any demand across a CTE boundary.
	void Index(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			auto &cte = op.Cast<LogicalMaterializedCTE>();
			definitions.emplace(cte.table_index, &cte);
		} else if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto &ref = op.Cast<LogicalCTERef>();
			references[ref.cte_index].push_back(&ref);
		}
		for (auto &child : op.children) {
			Index(*child);
		}
	}

	// Removing unused partitions must not change values in the retained partitions through
	// volatile expressions or ordered effects. Unknown table functions are not assumed pure.
	bool Stable(LogicalOperator &op) {
		auto entry = stable_cache.find(&op);
		if (entry != stable_cache.end()) {
			return entry->second;
		}
		stable_cache[&op] = false;
		bool stable = true;
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
			stable &= !(*expr)->IsVolatile() && !ExpressionBarrier::Contains(**expr);
			ExpressionIterator::VisitExpression<BoundFunctionExpression>(
			    **expr,
			    [&](const BoundFunctionExpression &func) { stable &= !func.Function().RequiresOrderedExecution(); });
		});
		if (op.type == LogicalOperatorType::LOGICAL_SAMPLE ||
		    op.type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
			stable = false;
		}
		if (op.type == LogicalOperatorType::LOGICAL_GET && !op.Cast<LogicalGet>().GetTable()) {
			stable = false;
		}
		if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto def = definitions.find(op.Cast<LogicalCTERef>().cte_index);
			if (def != definitions.end() && def->first != target) {
				stable &= Stable(*def->second->children[0]);
			}
		}
		for (auto &child : op.children) {
			stable &= Stable(*child);
		}
		stable_cache[&op] = stable;
		return stable;
	}

	// Materialized references are dependencies too. Mark an in-progress lookup conservatively;
	// ordinary CTE dependencies are acyclic, while unknown cycles must prevent pruning.
	bool DependsOnTarget(LogicalOperator &op) {
		auto entry = dependency_cache.find(&op);
		if (entry != dependency_cache.end()) {
			return entry->second;
		}
		dependency_cache[&op] = true;
		bool result = false;
		if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto &ref = op.Cast<LogicalCTERef>();
			result = ref.cte_index == target;
			auto def = definitions.find(ref.cte_index);
			if (def != definitions.end()) {
				result |= DependsOnTarget(*def->second->children[0]);
			}
		}
		for (auto &child : op.children) {
			result |= DependsOnTarget(*child);
		}
		dependency_cache[&op] = result;
		return result;
	}

	static optional_idx Position(const vector<ColumnBinding> &bindings, ColumnBinding binding) {
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (bindings[i] == binding) {
				return i;
			}
		}
		return optional_idx();
	}

	static bool ContainsKeys(LogicalOperator &op, const DemandKeys &keys) {
		auto bindings = op.GetColumnBindings();
		for (auto &key : keys) {
			if (!Position(bindings, key.binding).IsValid()) {
				return false;
			}
		}
		return true;
	}

	// Only direct column mappings establish key identity. Arithmetic, casts and computed
	// aliases do not silently become equality or functional-dependency proofs.
	static bool ThroughProjection(LogicalProjection &proj, DemandKeys &keys) {
		auto bindings = proj.GetColumnBindings();
		for (auto &key : keys) {
			auto pos = Position(bindings, key.binding);
			if (!pos.IsValid()) {
				return false;
			}
			auto &expr = *proj.expressions[pos.GetIndex()];
			if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				return false;
			}
			auto &col = expr.Cast<BoundColumnRefExpression>();
			if (col.Depth() != 0) {
				return false;
			}
			key.binding = col.Binding();
		}
		return true;
	}

	// CTE outputs are positional: translate only after verifying the complete output contract.
	static bool Remap(DemandKeys &keys, LogicalOperator &from, LogicalOperator &to) {
		auto source = from.GetColumnBindings();
		auto dest = to.GetColumnBindings();
		if (source.size() != dest.size()) {
			return false;
		}
		for (auto &key : keys) {
			auto pos = Position(source, key.binding);
			if (!pos.IsValid()) {
				return false;
			}
			key.binding = dest[pos.GetIndex()];
		}
		return true;
	}

	// Trace a required key set to an existing, filtered row consumer. The original filter
	// remains in the plan, and is included in the OR predicate used to restrict the producer.
	bool RowSource(LogicalOperator &op, DemandKeys &keys) {
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			return ThroughProjection(op.Cast<LogicalProjection>(), keys) && RowSource(*op.children[0], keys);
		}
		if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			return RowSource(*op.children[0], keys);
		}
		if (op.type != LogicalOperatorType::LOGICAL_CTE_REF) {
			return false;
		}
		auto &ref = op.Cast<LogicalCTERef>();
		if (!row_consumers.count(&ref)) {
			return false;
		}
		for (auto &key : keys) {
			if (key.binding.table_index != ref.table_index) {
				return false;
			}
			key.source_column = key.binding.column_index;
		}
		return true;
	}

	// Prove a recursive transition keeps each demanded key unchanged. There must be exactly
	// one self-reference, and no operation that combines partitions or limits them globally.
	bool PreservesPartition(LogicalOperator &op, DemandKeys keys, TableIndex recursive_index) {
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			return ThroughProjection(op.Cast<LogicalProjection>(), keys) &&
			       PreservesPartition(*op.children[0], std::move(keys), recursive_index);
		}
		if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			return PreservesPartition(*op.children[0], std::move(keys), recursive_index);
		}
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		    op.Cast<LogicalComparisonJoin>().join_type == JoinType::INNER) {
			for (idx_t i = 0; i < 2; i++) {
				if (ContainsKeys(*op.children[i], keys)) {
					return PreservesPartition(*op.children[i], std::move(keys), recursive_index);
				}
			}
			return false;
		}
		if (op.type != LogicalOperatorType::LOGICAL_CTE_REF) {
			return false;
		}
		auto &ref = op.Cast<LogicalCTERef>();
		if (ref.cte_index != recursive_index || ref.is_recurring) {
			return false;
		}
		for (auto &key : keys) {
			if (key.binding != ColumnBinding(ref.table_index, key.source_column)) {
				return false;
			}
		}
		return true;
	}

	// Transfer a partition demand through GROUP BY. Only complete, aggregate-free duplicate
	// elimination changes ROWS to KEYS; SUM/COUNT still require all contributing rows.
	static bool ThroughAggregate(LogicalAggregate &aggr, DemandKeys &keys, CTEDemand &demand) {
		if (aggr.groups.empty() || aggr.grouping_sets.size() > 1 || !aggr.grouping_functions.empty()) {
			return false;
		}
		unordered_set<ProjectionIndex> covered;
		for (auto &key : keys) {
			if (key.binding.table_index != aggr.group_index ||
			    key.binding.column_index.GetIndex() >= aggr.groups.size()) {
				return false;
			}
			auto idx = key.binding.column_index;
			if (!aggr.grouping_sets.empty() && !aggr.grouping_sets[0].count(idx)) {
				return false;
			}
			auto &expr = *aggr.groups[idx.GetIndex()];
			if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
			    expr.Cast<BoundColumnRefExpression>().Depth() != 0) {
				return false;
			}
			covered.insert(idx);
			key.binding = expr.Cast<BoundColumnRefExpression>().Binding();
		}
		demand = aggr.expressions.empty() && covered.size() == aggr.groups.size() ? CTEDemand::KEYS : CTEDemand::ROWS;
		return true;
	}

	// All is the implicit fallback: unsuccessful analysis certifies no consumers. A caller
	// commits the local certificates only when every relevant branch of this demand succeeds.
	bool Demand(LogicalOperator &op, DemandKeys keys, CTEDemand demand, vector<LogicalCTERef *> &covered) {
		if (!Stable(op)) {
			return false;
		}
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
			return ThroughProjection(op.Cast<LogicalProjection>(), keys) &&
			       Demand(*op.children[0], std::move(keys), demand, covered);
		case LogicalOperatorType::LOGICAL_FILTER:
			// A filtered key witness may not be supplied by the selected row consumer.
			return demand == CTEDemand::ROWS && Demand(*op.children[0], std::move(keys), demand, covered);
		case LogicalOperatorType::LOGICAL_DISTINCT: {
			auto &distinct = op.Cast<LogicalDistinct>();
			if (distinct.distinct_type != DistinctType::DISTINCT || distinct.order_by) {
				return false;
			}
			// Only a complete key-only output removes the need to preserve multiplicity.
			// DISTINCT ON and payload columns require a different witness and are not inferred.
			auto bindings = distinct.GetColumnBindings();
			for (auto &binding : bindings) {
				bool found = false;
				for (auto &key : keys) {
					found |= key.binding == binding;
				}
				if (!found) {
					return false;
				}
			}
			return Demand(*op.children[0], std::move(keys), CTEDemand::KEYS, covered);
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
			return ThroughAggregate(op.Cast<LogicalAggregate>(), keys, demand) &&
			       Demand(*op.children[0], std::move(keys), demand, covered);
		case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
			auto &rec = op.Cast<LogicalRecursiveCTE>();
			if (!rec.union_all || !rec.key_targets.empty() || !rec.payload_aggregates.empty() || rec.ref_recurring ||
			    references[rec.table_index].size() != 1) {
				return false;
			}
			auto recursive_keys = keys;
			for (auto &key : recursive_keys) {
				if (key.binding.table_index != rec.table_index) {
					return false;
				}
				key.source_column = key.binding.column_index;
			}
			if (!Remap(recursive_keys, rec, *rec.children[1]) ||
			    !PreservesPartition(*rec.children[1], std::move(recursive_keys), rec.table_index) ||
			    DependsOnTarget(*rec.children[1])) {
				return false;
			}
			return Remap(keys, rec, *rec.children[0]) &&
			       Demand(*rec.children[0], std::move(keys), CTEDemand::ROWS, covered);
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			if (demand != CTEDemand::ROWS || op.Cast<LogicalComparisonJoin>().join_type != JoinType::INNER) {
				return false;
			}
			for (idx_t i = 0; i < 2; i++) {
				if (ContainsKeys(*op.children[i], keys) && !DependsOnTarget(*op.children[1 - i])) {
					return Demand(*op.children[i], std::move(keys), demand, covered);
				}
			}
			return false;
		}
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			return Demand(*op.children[1], std::move(keys), demand, covered);
		case LogicalOperatorType::LOGICAL_CTE_REF: {
			auto &ref = op.Cast<LogicalCTERef>();
			if (ref.cte_index == target) {
				if (demand != CTEDemand::KEYS) {
					return false;
				}
				for (auto &key : keys) {
					if (key.binding != ColumnBinding(ref.table_index, key.source_column)) {
						return false;
					}
				}
				covered.push_back(&ref);
				return true;
			}
			auto def = definitions.find(ref.cte_index);
			// Shared intermediate results require merging every demand, not following one use.
			if (def == definitions.end() || references[ref.cte_index].size() != 1) {
				return false;
			}
			return Remap(keys, ref, *def->second->children[0]) &&
			       Demand(*def->second->children[0], std::move(keys), demand, covered);
		}
		default:
			return false;
		}
	}

	// Equality joins supply symbolic key sets from their filtered row input. The key set is
	// never evaluated or materialized: matching source columns prove that retained rows are
	// witnesses for every required key, including NULL under null-safe comparisons.
	void FindDemands(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		    op.Cast<LogicalComparisonJoin>().join_type == JoinType::INNER && Stable(op)) {
			auto &join = op.Cast<LogicalComparisonJoin>();
			for (idx_t side = 0; side < 2; side++) {
				DemandKeys source_keys, required_keys;
				auto source_bindings = join.children[side]->GetColumnBindings();
				auto required_bindings = join.children[1 - side]->GetColumnBindings();
				for (auto &cond : join.conditions) {
					if (!cond.IsComparison() ||
					    (cond.GetComparisonType() != ExpressionType::COMPARE_EQUAL &&
					     cond.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
						continue;
					}
					auto &lhs = cond.GetLHS();
					auto &rhs = cond.GetRHS();
					if (lhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
					    rhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
						continue;
					}
					if (lhs.Cast<BoundColumnRefExpression>().Depth() != 0 ||
					    rhs.Cast<BoundColumnRefExpression>().Depth() != 0) {
						continue;
					}
					auto a = lhs.Cast<BoundColumnRefExpression>().Binding();
					auto b = rhs.Cast<BoundColumnRefExpression>().Binding();
					if (side == 1) {
						std::swap(a, b);
					}
					if (Position(source_bindings, a).IsValid() && Position(required_bindings, b).IsValid()) {
						source_keys.push_back({a, a.column_index});
						required_keys.push_back({b, a.column_index});
					}
				}
				if (source_keys.empty() || !RowSource(*join.children[side], source_keys)) {
					continue;
				}
				for (idx_t i = 0; i < required_keys.size(); i++) {
					required_keys[i].source_column = source_keys[i].source_column;
				}
				vector<LogicalCTERef *> covered;
				if (Demand(*join.children[1 - side], std::move(required_keys), CTEDemand::ROWS, covered)) {
					key_consumers.insert(covered.begin(), covered.end());
				}
			}
		}
		for (auto &child : op.children) {
			FindDemands(*child);
		}
	}
};
} // namespace

bool CTEFilterAnalysis::CanRestrict(LogicalOperator &root, LogicalMaterializedCTE &cte,
                                    const vector<reference<LogicalOperator>> &filters) {
	ConsumerAnalysis analysis(root, cte.table_index, filters);
	return analysis.Analyze(root);
}
} // namespace duckdb
