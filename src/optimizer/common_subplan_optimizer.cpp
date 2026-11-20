#include "duckdb/optimizer/common_subplan_optimizer.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/cte_inlining.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_extractor.hpp"
#include "duckdb/optimizer/remove_redundant_order.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/arena_containers/arena_unordered_map.hpp"
#include "duckdb/common/arena_containers/arena_vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/logical_operator_repeatability.hpp"

#include <algorithm>
#include <cmath>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Subplan Signature/Info
//===--------------------------------------------------------------------===//
static vector<ColumnBinding>
GetExpandedColumnBindings(LogicalOperator &op, optional_ptr<vector<reference<LogicalGet>>> scans = nullptr,
                          optional_ptr<vector<reference<LogicalOperator>>> expression_sources = nullptr) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
		return GetExpandedColumnBindings(*op.children[0], scans, expression_sources);
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_UNNEST: {
		auto bindings = GetExpandedColumnBindings(*op.children[0], scans, expression_sources);
		const auto table_index = op.GetTableIndex()[0];
		for (auto col_idx : ProjectionIndex::GetIndexes(op.expressions.size())) {
			bindings.emplace_back(table_index, col_idx);
		}
		return bindings;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalJoin>();
		if (join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI) {
			return GetExpandedColumnBindings(*op.children[1], scans, expression_sources);
		}
		auto left_bindings = GetExpandedColumnBindings(*op.children[0], scans, expression_sources);
		if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI) {
			return left_bindings;
		}
		if (join.join_type == JoinType::MARK) {
			left_bindings.emplace_back(join.mark_index, ProjectionIndex(0));
			return left_bindings;
		}
		auto right_bindings = GetExpandedColumnBindings(*op.children[1], scans, expression_sources);
		left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
		return left_bindings;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN: {
		auto left_bindings = GetExpandedColumnBindings(*op.children[0], scans, expression_sources);
		auto right_bindings = GetExpandedColumnBindings(*op.children[1], scans, expression_sources);
		left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
		return left_bindings;
	}
	case LogicalOperatorType::LOGICAL_GET:
		if (scans) {
			scans->emplace_back(op.Cast<LogicalGet>());
		}
		return op.GetColumnBindings();
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_PROJECTION:
		if (expression_sources) {
			expression_sources->emplace_back(op);
		}
		return op.GetColumnBindings();
	default:
		return op.GetColumnBindings();
	}
}

static TableIndex GetExpressionOutputTableIndex(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return op.Cast<LogicalAggregate>().aggregate_index;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return op.Cast<LogicalProjection>().table_index;
	default:
		throw InternalException("Operator has no expandable expression outputs");
	}
}

static bool CanShareSubplanExpression(const Expression &expr) {
	if (expr.IsVolatile() || expr.CanThrow() || ExpressionBarrier::Contains(expr)) {
		return false;
	}
	bool unsafe = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &ref) { unsafe |= ref.Depth() != 0; });
	ExpressionIterator::VisitExpression<BoundFunctionExpression>(expr, [&](const BoundFunctionExpression &function) {
		unsafe |= ExpressionFilter::IsOptionalExpression(function);
	});
	return !unsafe;
}

static bool HasFixedSizeState(const LogicalType &type, const AggregateStateField &field) {
	switch (field.kind) {
	case AggregateFieldKind::PRIMITIVE:
		return TypeIsConstantSize(type.InternalType());
	case AggregateFieldKind::OPTIONAL_VALUE:
		return field.children.size() == 1 && HasFixedSizeState(type, field.children[0]);
	case AggregateFieldKind::STRUCT: {
		if (type.id() != LogicalTypeId::STRUCT) {
			return false;
		}
		const auto &types = StructType::GetChildTypes(type);
		if (types.size() != field.children.size()) {
			return false;
		}
		for (idx_t i = 0; i < types.size(); i++) {
			if (!HasFixedSizeState(types[i].second, field.children[i])) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

static bool CanCombineAggregateState(const BoundAggregateExpression &aggregate) {
	const auto &function = aggregate.Function();
	if (aggregate.IsDistinct() || aggregate.GetOrderBys() ||
	    aggregate.StateExportMode() != AggregateStateExportMode::NONE || !function.HasGetStateTypeCallback() ||
	    function.HasExportAggregateStateCallback() || function.HasImportAggregateStateCallback()) {
		return false;
	}
	// Combining growing states can use more memory than evaluating the original aggregates separately.
	const auto layout = function.GetStateType(aggregate.BindInfo().get());
	return HasFixedSizeState(layout.type, layout.field);
}

enum class ConversionType {
	TO_CANONICAL,
	RESTORE_ORIGINAL,
};

class PlanSignatureColumnIndexMap {
public:
	explicit PlanSignatureColumnIndexMap(ArenaAllocator &allocator)
	    : to_canonical(allocator), restore_original(allocator) {
	}

public:
	template <ConversionType TYPE>
	bool Empty() const {
		return GetMap<TYPE>().empty();
	}

	void Insert(const ProjectionIndex original, const ProjectionIndex canonical) {
		D_ASSERT(to_canonical.find(original) == to_canonical.end());
		D_ASSERT(restore_original.find(canonical) == restore_original.end());
		to_canonical.emplace(make_pair(original, canonical));
		restore_original.emplace(make_pair(canonical, original));
	}

	template <ConversionType TYPE>
	ProjectionIndex Get(const ProjectionIndex index) const {
		D_ASSERT(!Empty<TYPE>());
		return GetMap<TYPE>().at(index);
	}

private:
	template <ConversionType TYPE>
	const arena_unordered_map<ProjectionIndex, ProjectionIndex> &GetMap() const {
		return TYPE == ConversionType::TO_CANONICAL ? to_canonical : restore_original;
	}

private:
	//! Map from original column index to canonical column index (and reverse)
	arena_unordered_map<ProjectionIndex, ProjectionIndex> to_canonical;
	arena_unordered_map<ProjectionIndex, ProjectionIndex> restore_original;
};

class PlanSignatureTableIndexMap {
public:
	explicit PlanSignatureTableIndexMap(ArenaAllocator &allocator_p)
	    : allocator(allocator_p), table_index_map(allocator), to_canonical_table_index(allocator),
	      restore_original_table_index(allocator), restore_original_table_filter_index(allocator) {
	}

public:
	const arena_unordered_map<TableIndex, PlanSignatureColumnIndexMap> &GetMap() const {
		return table_index_map;
	}

	void AddColumnMapping(TableIndex table_index, ProjectionIndex original, ProjectionIndex canonical) {
		table_index_map.at(table_index).Insert(original, canonical);
	}

	template <ConversionType TYPE>
	bool Convert(LogicalOperator &op) {
		Initialize<TYPE>(op);
		ConvertTableIndices<TYPE>(op);
		ConvertProjectionMaps<TYPE>(op);
		auto can_materialize = ConvertColumnIndices<TYPE>(op);
		ConvertChildren<TYPE>(op);
		return ConvertExpressions<TYPE>(op) && can_materialize;
	}

private:
	template <ConversionType TYPE>
	void Initialize(LogicalOperator &op) {
		if (TYPE == ConversionType::TO_CANONICAL) {
			// Clear temporary data structures
			to_canonical_table_index.clear();
			restore_original_table_index.clear();
			restore_original_table_filter_index.clear();
			column_ids.clear();
			chunk_column_ids.clear();
			projection_ids.clear();
			table_indices.clear();
			projection_maps.clear();
			expression_info.clear();

			// Store temporary mapping
			for (const auto &child_op : op.children) {
				for (const auto &child_cb : GetExpandedColumnBindings(*child_op)) {
					const auto &original = child_cb.table_index;
					auto it = to_canonical_table_index.find(original);
					if (it != to_canonical_table_index.end()) {
						continue; // We've seen this table index before
					}
					TableIndex canonical(CANONICAL_TABLE_INDEX_OFFSET + to_canonical_table_index.size());
					D_ASSERT(to_canonical_table_index.find(original) == to_canonical_table_index.end());
					D_ASSERT(restore_original_table_index.find(canonical) == restore_original_table_index.end());
					to_canonical_table_index.emplace(make_pair(original, canonical));
					restore_original_table_index.emplace(make_pair(canonical, original));
				}
			}
		}
	}

	template <ConversionType TYPE>
	void ConvertTableIndices(LogicalOperator &op) {
#ifdef D_ASSERT_IS_ENABLED
		const auto table_indices_verification = op.GetTableIndex();
#endif
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET:
			ConvertTableIndex<TYPE>(op.Cast<LogicalGet>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_CHUNK_GET:
			ConvertTableIndex<TYPE>(op.Cast<LogicalColumnDataGet>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
			ConvertTableIndex<TYPE>(op.Cast<LogicalExpressionGet>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
			ConvertTableIndex<TYPE>(op.Cast<LogicalDummyScan>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_CTE_REF:
			ConvertTableIndex<TYPE>(op.Cast<LogicalCTERef>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_PROJECTION:
			ConvertTableIndex<TYPE>(op.Cast<LogicalProjection>().table_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_PIVOT:
			ConvertTableIndex<TYPE>(op.Cast<LogicalPivot>().pivot_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_UNNEST:
			ConvertTableIndex<TYPE>(op.Cast<LogicalUnnest>().unnest_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_WINDOW:
			ConvertTableIndex<TYPE>(op.Cast<LogicalWindow>().window_index, 0);
			break;
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggr = op.Cast<LogicalAggregate>();
			ConvertTableIndex<TYPE>(aggr.group_index, 0);
			ConvertTableIndex<TYPE>(aggr.aggregate_index, 1);
			if (aggr.groupings_index.IsValid()) {
				ConvertTableIndex<TYPE>(aggr.groupings_index, 2);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op.Cast<LogicalJoin>();
			if (join.join_type == JoinType::MARK) {
				ConvertTableIndex<TYPE>(join.mark_index, 0);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT:
			ConvertTableIndex<TYPE>(op.Cast<LogicalSetOperation>().table_index, 0);
			break;
		default:
			break;
		}
#ifdef D_ASSERT_IS_ENABLED
		if (TYPE == ConversionType::TO_CANONICAL) {
			D_ASSERT(table_indices == table_indices_verification);
		}
#endif
	}

	template <ConversionType TYPE>
	void ConvertProjectionMaps(LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op.Cast<LogicalJoin>();
			if (TYPE == ConversionType::TO_CANONICAL) {
				D_ASSERT(projection_maps.empty());
				projection_maps.push_back(std::move(join.left_projection_map));
				projection_maps.push_back(std::move(join.right_projection_map));
			} else {
				D_ASSERT(TYPE == ConversionType::RESTORE_ORIGINAL);
				D_ASSERT(!projection_maps.empty());
				join.left_projection_map = std::move(projection_maps[0]);
				join.right_projection_map = std::move(projection_maps[1]);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto &filter = op.Cast<LogicalFilter>();
			if (TYPE == ConversionType::TO_CANONICAL) {
				D_ASSERT(projection_maps.empty());
				projection_maps.push_back(std::move(filter.projection_map));
			} else {
				D_ASSERT(TYPE == ConversionType::RESTORE_ORIGINAL);
				D_ASSERT(!projection_maps.empty());
				filter.projection_map = std::move(projection_maps[0]);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_ORDER_BY: {
			auto &order = op.Cast<LogicalOrder>();
			if (TYPE == ConversionType::TO_CANONICAL) {
				D_ASSERT(projection_maps.empty());
				projection_maps.push_back(std::move(order.projection_map));
			} else {
				D_ASSERT(TYPE == ConversionType::RESTORE_ORIGINAL);
				D_ASSERT(!projection_maps.empty());
				order.projection_map = std::move(projection_maps[0]);
			}
			break;
		}
		default:
			break;
		}
	}

	template <ConversionType TYPE>
	void ConvertTableIndex(TableIndex &table_index, const idx_t i) {
		switch (TYPE) {
		case ConversionType::TO_CANONICAL:
			D_ASSERT(table_indices.size() == i);
			D_ASSERT(table_index_map.find(table_index) == table_index_map.end());
			table_index_map.emplace(table_index, PlanSignatureColumnIndexMap(allocator));
			table_indices.emplace_back(table_index);
			table_index = TableIndex(CANONICAL_TABLE_INDEX_OFFSET + to_canonical_table_index.size() + i);
			break;
		case ConversionType::RESTORE_ORIGINAL:
			table_index = table_indices[i];
			break;
		}
	}

	template <ConversionType TYPE>
	bool ConvertColumnIndices(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			switch (TYPE) {
			case ConversionType::TO_CANONICAL: {
				// Source ordinality is represented by the bound operators.
				source_ordinality = get.source_ordinality;
				get.source_ordinality = OrdinalityType::WITHOUT_ORDINALITY;
				D_ASSERT(column_ids.empty());
				// Grab selected GET columns and populate with all possible columns
				column_ids = std::move(get.GetMutableColumnIds());
				for (idx_t col_idx = 0; col_idx < get.names.size(); col_idx++) {
					get.GetMutableColumnIds().push_back(ColumnIndex(col_idx));
				}
				for (const auto &vc : get.virtual_columns) {
					get.GetMutableColumnIds().push_back(ColumnIndex(vc.first));
				}

				// Also temporarily don't project any columns out
				projection_ids = std::move(get.projection_ids);

				// FIXME: can probably work for nested columns in the future
				for (const auto &col_id : column_ids) {
					if (col_id.IsVirtualColumn() || col_id.HasChildren()) {
						return false;
					}
				}

				auto register_filter_index = [&](ProjectionIndex filter_index) {
					D_ASSERT(filter_index.GetIndex() < column_ids.size());
					const auto canonical_index = ProjectionIndex(column_ids[filter_index.GetIndex()].GetPrimaryIndex());
					auto entry = restore_original_table_filter_index.emplace(canonical_index, filter_index);
					return entry.second || entry.first->second == filter_index;
				};
				for (auto &entry : get.table_filters) {
					if (!register_filter_index(entry.GetIndex())) {
						restore_original_table_filter_index.clear();
						return false;
					}
				}
				for (const auto &filter : get.table_filters.GetMultiColumnFilters()) {
					const auto &expression_filter =
					    ExpressionFilter::GetExpressionFilter(*filter, "CommonSubplanOptimizer::ConvertTableIndex");
					for (const auto &column_index : expression_filter.column_indexes) {
						if (!register_filter_index(column_index)) {
							restore_original_table_filter_index.clear();
							return false;
						}
					}
				}
				if (!restore_original_table_filter_index.empty()) {
					TableFilterSet remapped_filters;
					for (auto &entry : get.table_filters) {
						const auto canonical_index =
						    ProjectionIndex(column_ids[entry.GetIndex().GetIndex()].GetPrimaryIndex());
						remapped_filters.PushFilter(canonical_index, entry.TakeFilter());
					}
					for (const auto &filter : get.table_filters.GetMultiColumnFilters()) {
						auto remapped_filter =
						    ExpressionFilter::GetExpressionFilter(*filter, "CommonSubplanOptimizer::ConvertTableIndex")
						        .Copy();
						for (auto &column_index : remapped_filter->column_indexes) {
							column_index = ProjectionIndex(column_ids[column_index.GetIndex()].GetPrimaryIndex());
						}
						remapped_filters.PushMultiColumnFilter(std::move(remapped_filter));
					}
					get.table_filters = std::move(remapped_filters);
				}

				// Store mapping for base tables
				auto &column_index_map = table_index_map.at(table_indices[0]);
				if (projection_ids.empty()) {
					for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
						ProjectionIndex primary_index(column_ids[col_idx].GetPrimaryIndex());
						column_index_map.Insert(ProjectionIndex(col_idx), primary_index);
					}
				} else {
					for (const auto &proj_id : projection_ids) {
						ProjectionIndex primary_index(column_ids[proj_id].GetPrimaryIndex());
						column_index_map.Insert(proj_id, primary_index);
					}
				}
				break;
			}
			case ConversionType::RESTORE_ORIGINAL:
				get.source_ordinality = source_ordinality;
				D_ASSERT(!column_ids.empty());
				get.GetMutableColumnIds() = std::move(column_ids);
				D_ASSERT(get.projection_ids.empty());
				get.projection_ids = std::move(projection_ids);
				if (!restore_original_table_filter_index.empty()) {
					TableFilterSet remapped_filters;
					for (auto &entry : get.table_filters) {
						remapped_filters.PushFilter(restore_original_table_filter_index.at(entry.GetIndex()),
						                            entry.TakeFilter());
					}
					for (const auto &filter : get.table_filters.GetMultiColumnFilters()) {
						auto remapped_filter =
						    ExpressionFilter::GetExpressionFilter(*filter, "CommonSubplanOptimizer::ConvertTableIndex")
						        .Copy();
						for (auto &column_index : remapped_filter->column_indexes) {
							column_index = restore_original_table_filter_index.at(column_index);
						}
						remapped_filters.PushMultiColumnFilter(std::move(remapped_filter));
					}
					get.table_filters = std::move(remapped_filters);
				}
				break;
			}
		}
		if (op.type == LogicalOperatorType::LOGICAL_CHUNK_GET) {
			auto &get = op.Cast<LogicalColumnDataGet>();
			switch (TYPE) {
			case ConversionType::TO_CANONICAL: {
				D_ASSERT(chunk_column_ids.empty());
				chunk_column_ids = get.GetColumnIds();

				unordered_set<column_t> selected_columns;
				for (auto column_id : chunk_column_ids) {
					if (!selected_columns.insert(column_id).second) {
						return false;
					}
				}

				// Expose all collection columns so pruning does not change the scan signature.
				vector<column_t> all_column_ids;
				all_column_ids.reserve(get.chunk_types.size());
				for (idx_t column_id = 0; column_id < get.chunk_types.size(); column_id++) {
					all_column_ids.push_back(column_id);
				}
				get.SetColumnIds(std::move(all_column_ids));

				auto &column_index_map = table_index_map.at(table_indices[0]);
				for (idx_t column_idx = 0; column_idx < chunk_column_ids.size(); column_idx++) {
					column_index_map.Insert(ProjectionIndex(column_idx), ProjectionIndex(chunk_column_ids[column_idx]));
				}
				break;
			}
			case ConversionType::RESTORE_ORIGINAL:
				get.SetColumnIds(std::move(chunk_column_ids));
				break;
			}
		}
		return true;
	}

	template <ConversionType TYPE>
	void ConvertChildren(LogicalOperator &op) {
		switch (TYPE) {
		case ConversionType::TO_CANONICAL:
			D_ASSERT(children.empty());
			children = std::move(op.children);
			break;
		case ConversionType::RESTORE_ORIGINAL:
			D_ASSERT(op.children.empty());
			op.children = std::move(children);
			break;
		}
	}

	template <ConversionType TYPE>
	bool ConvertExpressions(LogicalOperator &op) {
		idx_t info_idx = 0;
		bool can_materialize = true;
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
			ExpressionIterator::EnumerateExpression(*expr, [&](unique_ptr<Expression> &child) {
				ConvertExpression<TYPE>(child, info_idx, can_materialize);
			});
		});
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			for (auto &entry : get.table_filters) {
				auto &expression_filter =
				    ExpressionFilter::GetExpressionFilter(entry.Filter(), "CommonSubplanOptimizer::ConvertExpressions");
				ConvertExpression<TYPE>(expression_filter.expr, info_idx, can_materialize);
			}
		}
		return can_materialize;
	}

	template <ConversionType TYPE>
	void ConvertExpression(unique_ptr<Expression> &expr_p, idx_t &info_idx, bool &can_materialize) {
		const auto &table_index_mapping =
		    TYPE == ConversionType::TO_CANONICAL ? to_canonical_table_index : restore_original_table_index;

		// Determine the default fields: these are cleared when converting to canonical and restored afterwards
		Identifier alias;
		QueryLocation query_location;
		switch (TYPE) {
		case ConversionType::TO_CANONICAL:
			expression_info.emplace_back(expr_p->GetAlias(), expr_p->GetQueryLocation());
			break;
		case ConversionType::RESTORE_ORIGINAL: {
			auto &info = expression_info[info_idx++];
			alias = Identifier(std::move(info.first));
			query_location = info.second;
			break;
		}
		}

		if (expr_p->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			// Replace column binding
			auto &col_ref = expr_p->Cast<BoundColumnRefExpression>();
			auto binding = col_ref.Binding();
			const auto lookup_idx = TYPE == ConversionType::TO_CANONICAL
			                            ? binding.table_index
			                            : restore_original_table_index.at(binding.table_index);
			auto &table_map = table_index_map.at(lookup_idx);
			if (!table_map.Empty<TYPE>()) {
				// Replace column index
				binding.column_index = table_map.Get<TYPE>(binding.column_index);
			}
			// Replace table index
			binding.table_index = table_index_mapping.at(binding.table_index);
			expr_p = make_uniq<BoundColumnRefExpression>(std::move(alias), col_ref.GetReturnType(), binding,
			                                             col_ref.Depth(), query_location);
		} else {
			expr_p->SetAlias(std::move(alias));
			expr_p->SetQueryLocation(query_location);
		}
		if (expr_p->IsVolatile()) {
			can_materialize = false;
		}
	}

private:
	//! Offset for table index conversion
	static constexpr idx_t CANONICAL_TABLE_INDEX_OFFSET = 10000000000000;

	//! For batching allocations
	ArenaAllocator &allocator;

	//! Map from original table index to column index map
	arena_unordered_map<TableIndex, PlanSignatureColumnIndexMap> table_index_map;

	//! Temporary map from original table index to canonical table index (and reverse)
	arena_unordered_map<TableIndex, TableIndex> to_canonical_table_index;
	arena_unordered_map<TableIndex, TableIndex> restore_original_table_index;
	//! Temporary map from canonical table filter index to original table filter index
	arena_unordered_map<ProjectionIndex, ProjectionIndex> restore_original_table_filter_index;
	//! Temporary vector to store table indices
	vector<TableIndex> table_indices;
	//! Temporary vector to store projection maps
	vector<vector<ProjectionIndex>> projection_maps;

	//! Utility to temporarily store column ids, projection_ids, table indices, expression info and children
	OrdinalityType source_ordinality = OrdinalityType::WITHOUT_ORDINALITY;
	vector<ColumnIndex> column_ids;
	vector<column_t> chunk_column_ids;
	vector<ProjectionIndex> projection_ids;
	vector<pair<string, optional_idx>> expression_info;
	vector<unique_ptr<LogicalOperator>> children;
};

class PlanSignatureCreateState {
public:
	explicit PlanSignatureCreateState(ClientContext &context)
	    : stream(DEFAULT_BLOCK_ALLOC_SIZE), serializer(stream), allocator(BufferAllocator::Get(context)),
	      table_index_map(allocator) {
	}

public:
public:
	//! For serializing operators to a binary string
	MemoryStream stream;
	BinarySerializer serializer;

	//! For batching allocations together
	ArenaAllocator allocator;

	//! Manages mappings
	PlanSignatureTableIndexMap table_index_map;
};

class PlanSignature {
public:
	PlanSignature(const MemoryStream &stream_p, idx_t offset_p, idx_t length_p,
	              vector<reference<PlanSignature>> &&child_signatures_p, idx_t operator_count_p)
	    : stream(stream_p), offset(offset_p), length(length_p),
	      signature_hash(Hash(stream_p.GetData() + offset, length)), child_signatures(std::move(child_signatures_p)),
	      operator_count(operator_count_p) {
	}

public:
	static arena_ptr<PlanSignature> Create(PlanSignatureCreateState &state, LogicalOperator &op,
	                                       vector<reference<PlanSignature>> &&child_signatures,
	                                       const vector<vector<ColumnBinding>> &positional_inputs = {}) {
		if (!OperatorIsSupported(op)) {
			return nullptr;
		}

		auto can_materialize = state.table_index_map.Convert<ConversionType::TO_CANONICAL>(op);

		// Serialize canonical representation of operator
		const auto offset = state.stream.GetPosition();
		state.serializer.Begin();
		state.serializer.WriteProperty(100, "positional_inputs", positional_inputs);
		state.serializer.End();
		state.serializer.Begin();
		try { // Operators will throw if they cannot serialize, so we need to try/catch here
			op.Serialize(state.serializer);
		} catch (std::exception &) {
			can_materialize = false;
		}
		state.serializer.End();
		const auto length = state.stream.GetPosition() - offset;

		// Convert back from canonical
		state.table_index_map.Convert<ConversionType::RESTORE_ORIGINAL>(op);

		if (!can_materialize) {
			return nullptr; // Cannot materialize, no point in adding it
		}

		// Collect some statistics so we can select a good candidate later
		idx_t operator_count = 1;
		for (auto &child_signature : child_signatures) {
			operator_count += child_signature.get().OperatorCount();
		}
		return state.allocator.MakePtr<PlanSignature>(state.stream, offset, length, std::move(child_signatures),
		                                              operator_count);
	}

	idx_t OperatorCount() const {
		return operator_count;
	}

	hash_t HashSignature() const {
		auto res = signature_hash;
		for (auto &child : child_signatures) {
			res = CombineHash(res, child.get().HashSignature());
		}
		return res;
	}

	bool Equals(const PlanSignature &other) const {
		if (this->GetSignature() != other.GetSignature()) {
			return false;
		}
		if (this->child_signatures.size() != other.child_signatures.size()) {
			return false;
		}
		for (idx_t child_idx = 0; child_idx < this->child_signatures.size(); ++child_idx) {
			if (!this->child_signatures[child_idx].get().Equals(other.child_signatures[child_idx].get())) {
				return false;
			}
		}
		return true;
	}

private:
	String GetSignature() const {
		return String(char_ptr_cast(stream.GetData() + offset), NumericCast<uint32_t>(length));
	}

	static bool OperatorIsSupported(LogicalOperator &op) {
		if (!op.SupportSerialization()) {
			return false;
		}
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		case LogicalOperatorType::LOGICAL_WINDOW:
		case LogicalOperatorType::LOGICAL_UNNEST:
		case LogicalOperatorType::LOGICAL_LIMIT:
		case LogicalOperatorType::LOGICAL_ORDER_BY:
		case LogicalOperatorType::LOGICAL_TOP_N:
		case LogicalOperatorType::LOGICAL_PIVOT:
		case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT:
			return true;
		case LogicalOperatorType::LOGICAL_GET: {
			auto &get = op.Cast<LogicalGet>();
			// A function's arguments do not describe hidden state or volatile work performed by its scan.
			// Only share scans that explicitly promise repeatable results, or scan an ordinary table.
			if (ClassifyLogicalOperatorRepeatability(get) != LogicalOperatorRepeatability::REPEATABLE) {
				return false;
			}
			if (get.bind_data && !get.function.HasSerializationCallbacks() && get.parameters.empty()) {
				// Without serialization callbacks, the serialized form carries only the call parameters
				// (see LogicalGet::Serialize). A scan without positional parameters - e.g., one created through
				// an attached catalog - keeps its identity solely in the bind data, and its options do not
				// identify what it reads, so equal serialized bytes cannot prove that two scans read the same
				// table.
				return false;
			}
			return true;
		}
		case LogicalOperatorType::LOGICAL_CHUNK_GET:
			// Avoid serializing massive amounts of data (this is here because of the "Test TPCH arrow roundtrip" test)
			return op.Cast<LogicalColumnDataGet>().collection->Count() < 1000;
		default:
			// Unsupported:
			// - case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
			// - case LogicalOperatorType::LOGICAL_SAMPLE:
			// - case LogicalOperatorType::LOGICAL_COPY_DATABASE:
			// - case LogicalOperatorType::LOGICAL_DELIM_GET:
			// - case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
			// - case LogicalOperatorType::LOGICAL_CTE_REF:
			// - case LogicalOperatorType::LOGICAL_JOIN:
			// - case LogicalOperatorType::LOGICAL_DELIM_JOIN:
			// - case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
			// - case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
			// - case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			// - case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR
			return false;
		}
	}

private:
	const MemoryStream &stream;
	const idx_t offset;
	const idx_t length;

	const hash_t signature_hash;

	const vector<reference<PlanSignature>> child_signatures;
	const idx_t operator_count;
};

struct PlanSignatureHash {
	std::size_t operator()(const PlanSignature &k) const {
		return k.HashSignature();
	}
};

struct PlanSignatureEquality {
	bool operator()(const PlanSignature &a, const PlanSignature &b) const {
		return a.Equals(b);
	}
};

struct Subplan {
	reference<unique_ptr<LogicalOperator>> op;
	arena_vector<ColumnBinding> canonical_bindings;
};

struct SubplanInfo {
	SubplanInfo(ArenaAllocator &allocator, unique_ptr<LogicalOperator> &op,
	            arena_vector<idx_t> &&canonical_table_indices_p, arena_vector<ColumnBinding> &&canonical_bindings)
	    : canonical_table_indices(std::move(canonical_table_indices_p)), subplans(allocator),
	      lowest_common_ancestor(op) {
		subplans.push_back({op, std::move(canonical_bindings)});
	}
	arena_vector<idx_t> canonical_table_indices;
	arena_vector<Subplan> subplans;
	reference<unique_ptr<LogicalOperator>> lowest_common_ancestor;
};

using subplan_map_t = unordered_map<reference<PlanSignature>, SubplanInfo, PlanSignatureHash, PlanSignatureEquality>;

//===--------------------------------------------------------------------===//
// CommonSubplanFinder
//===--------------------------------------------------------------------===//
class CommonSubplanFinder {
public:
	explicit CommonSubplanFinder(ClientContext &context) : state(context) {
	}

private:
	struct OperatorInfo {
		OperatorInfo(unique_ptr<LogicalOperator> &parent_p, const idx_t &depth_p) : parent(parent_p), depth(depth_p) {
		}

		unique_ptr<LogicalOperator> &parent;
		const idx_t depth;
		arena_ptr<PlanSignature> signature;
	};

	struct StackNode {
		explicit StackNode(unique_ptr<LogicalOperator> &op_p) : op(op_p), child_index(0) {
		}

		bool HasMoreChildren() const {
			return child_index < op->children.size();
		}

		unique_ptr<LogicalOperator> &GetNextChild() {
			D_ASSERT(child_index < op->children.size());
			return op->children[child_index++];
		};

		unique_ptr<LogicalOperator> &op;
		idx_t child_index;
	};

	struct SubplanStats {
		idx_t base_table_count;
		idx_t max_base_table_cardinality;

		void Combine(const SubplanStats &other) {
			this->base_table_count += other.base_table_count;
			this->max_base_table_cardinality =
			    MaxValue(this->max_base_table_cardinality, other.max_base_table_cardinality);
		}
	};

	struct SubplanBindings {
		struct ExpressionSource {
			ExpressionSource(LogicalOperator &op, const vector<unique_ptr<Expression>> &expressions,
			                 SubplanBindings input)
			    : op(op), expressions(expressions), input(make_uniq<SubplanBindings>(std::move(input))) {
			}

			LogicalOperator &op;
			const vector<unique_ptr<Expression>> &expressions;
			unique_ptr<SubplanBindings> input;
		};

		column_binding_map_t<ColumnBinding> bindings;
		unordered_map<TableIndex, optional_ptr<LogicalGet>> scans;
		unordered_map<TableIndex, unique_ptr<ExpressionSource>> expression_sources;

		bool Contains(const ColumnBinding &binding) const {
			auto scan = scans.find(binding.table_index);
			if (bindings.find(binding) != bindings.end() || (scan != scans.end() && scan->second)) {
				return true;
			}
			auto expression_source = expression_sources.find(binding.table_index);
			if (expression_source == expression_sources.end() || !expression_source->second) {
				return false;
			}
			auto &source = *expression_source->second;
			bool available = true;
			ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
			    *source.expressions[binding.column_index],
			    [&](const BoundColumnRefExpression &ref) { available &= source.input->Contains(ref.Binding()); });
			return available;
		}

		ColumnBinding Expose(const ColumnBinding &binding) {
			auto entry = bindings.find(binding);
			if (entry != bindings.end()) {
				return entry->second;
			}
			ColumnBinding result;
			auto scan_entry = scans.find(binding.table_index);
			if (scan_entry != scans.end() && scan_entry->second) {
				auto &scan = *scan_entry->second;
				// Appending columns preserves existing filter and expression bindings.
				auto column_index = scan.TryGetProjectionIndex(binding.column_index.GetIndex());
				if (!column_index.IsValid()) {
					column_index = scan.AddColumnId(binding.column_index.GetIndex());
				}
				// Scan projections prune columns; they cannot reorder a full-width scan.
				scan.projection_ids.clear();
				result = ColumnBinding(scan.table_index, column_index);
			} else {
				auto &source = *expression_sources.at(binding.table_index);
				auto expression = source.expressions[binding.column_index]->Copy();
				expression = ExpressionIterator::PreorderReplace<BoundColumnRefExpression>(
				    std::move(expression), [&](const BoundColumnRefExpression &ref) {
					    return ref.WithBinding(source.input->Expose(ref.Binding()));
				    });
				result = ColumnBinding(GetExpressionOutputTableIndex(source.op),
				                       ProjectionIndex(source.op.expressions.size()));
				source.op.expressions.push_back(std::move(expression));
			}
			bindings.emplace(binding, result);
			return result;
		}
	};

public:
	void FindCommonSubplans(reference<unique_ptr<LogicalOperator>> root) {
		// Find first operator with more than 1 child
		while (root.get()->children.size() == 1) {
			root = root.get()->children[0];
		}

		// Recurse through query plan using stack-based recursion
		arena_vector<StackNode> stack(state.allocator);
		stack.emplace_back(root);
		operator_infos.emplace(root, OperatorInfo(root, 0));

		while (!stack.empty()) {
			auto &current = stack.back();

			// Depth-first
			if (current.HasMoreChildren()) {
				auto &child = current.GetNextChild();
				operator_infos.emplace(child, OperatorInfo(current.op, stack.size()));
				stack.emplace_back(child);
				continue;
			}

			if (!RefersToSameObject(current.op, root.get())) {
				// We have all child information for this operator now, compute signature
				D_ASSERT(operator_infos.find(current.op) != operator_infos.end());
				auto &signature = operator_infos.find(current.op)->second.signature;
				signature = CreatePlanSignature(current.op);

				// Add to subplans (if we got actually got a signature)
				if (signature) {
					const auto current_op_table_index = current.op->GetTableIndex();
					auto it = subplans.find(*signature);
					if (it == subplans.end()) {
						// New subplan, map table indices
						arena_vector<idx_t> canonical_table_indices(state.allocator);
						for (auto &table_index : current_op_table_index) {
							const auto next_canonical_table_index = to_canonical_table_index.size();
							canonical_table_indices.push_back(next_canonical_table_index);
							D_ASSERT(to_canonical_table_index.find(table_index) == to_canonical_table_index.end());
							to_canonical_table_index.emplace(table_index, next_canonical_table_index);
						}
						RegisterExpressionBindings(current.op);

						// Add new subplan
						SubplanInfo subplan_info(state.allocator, current.op, std::move(canonical_table_indices),
						                         GetCanonicalBindings(*current.op));
						subplans.emplace(*signature, std::move(subplan_info));
					} else {
						auto &info = it->second;
						// Matches existing subplan, map already existing table indices
						D_ASSERT(current_op_table_index.size() == info.canonical_table_indices.size());
						for (idx_t i = 0; i < current_op_table_index.size(); i++) {
							to_canonical_table_index.emplace(current_op_table_index[i],
							                                 info.canonical_table_indices[i]);
						}
						RegisterExpressionBindings(current.op);

						// Add subplan to existing
						info.subplans.push_back({current.op, GetCanonicalBindings(*current.op)});
						info.lowest_common_ancestor = LowestCommonAncestor(info.lowest_common_ancestor, current.op);
					}
				}
			}

			// Done with current
			stack.pop_back();
		}
	}

	void FilterSubplans() {
		// Filter out redundant or ineligible subplans before returning
		vector<subplan_map_t::key_type> to_remove;
		for (auto &entry : subplans) {
			auto &signature = entry.first.get();
			auto &subplan_info = entry.second;
			if (signature.OperatorCount() == 1) {
				to_remove.push_back(signature); // Just one operator in this subplan
				continue;
			}

			if (subplan_info.subplans.size() == 1) {
				to_remove.push_back(signature); // No other identical subplan
				continue;
			}

			D_ASSERT(operator_infos.find(subplan_info.subplans[0].op) != operator_infos.end());
			auto &parent_op = operator_infos.find(subplan_info.subplans[0].op)->second.parent;
			D_ASSERT(operator_infos.find(parent_op) != operator_infos.end());
			auto &parent_signature = operator_infos.find(parent_op)->second.signature;
			if (parent_signature) {
				auto parent_it = subplans.find(*parent_signature);
				if (parent_it != subplans.end()) {
					const auto subplan_count = subplan_info.subplans.size() * signature.OperatorCount();
					const auto parent_subplan_count = parent_it->second.subplans.size();
					const auto parent_count = parent_subplan_count * parent_signature->OperatorCount();
					if (parent_subplan_count != 1 && parent_count >= subplan_count &&
					    ShouldMaterialize(parent_it->second)) {
						to_remove.push_back(signature); // Parent is better, this one is redundant
						continue;
					}
				}
			}

			// We can bail on a subplan for various reasons (some of which could potentially be fixed)
			bool bail = false;

			for (auto &subplan : subplan_info.subplans) {
				column_binding_set_t subplan_bindings;
				for (auto &cb : subplan.canonical_bindings) {
					if (subplan_bindings.find(cb) != subplan_bindings.end()) {
						bail =
						    true; // Subplan contains duplicate column bindings, i.e., another nested duplicate subplan
						break;
					}
					subplan_bindings.insert(cb);
				}
				if (bail) {
					break;
				}
			}

			if (bail || !SelectPrimarySubplan(subplan_info)) {
				to_remove.push_back(signature);
				continue;
			}
		}

		// Only remove them all at the end so the logic above doesn't get affected
		for (auto &signature : to_remove) {
			subplans.erase(signature);
		}
	}

	void ConvertSubplansToCTEs(Optimizer &optimizer, unique_ptr<LogicalOperator> &op) {
		const auto sorted_subplans = GetSortedSubplans();
		idx_t index = 1;
		for (auto &entry : sorted_subplans) {
			auto &subplan_info = entry.get().second;
			if (!ShouldMaterialize(subplan_info)) {
				continue; // No longer worth materializing due to other materializations
			}
			if (!SelectPrimarySubplan(subplan_info)) {
				continue; // Nested subplans were replaced by CTE refs that no longer expose the required bindings
			}

			const auto cte_index = optimizer.binder.GenerateTableIndex();
			const auto cte_name = StringUtil::Format("__common_subplan_%llu", index++);
			if (!min_cte_idx.IsValid()) {
				min_cte_idx = cte_index;
			}

			// Resolve types to be used for creating the materialized CTE and refs
			op->ResolveOperatorTypes();

			const auto &primary_subplan = subplan_info.subplans[0];

			vector<vector<ColumnBinding>> old_bindings(subplan_info.subplans.size());
			vector<vector<LogicalType>> old_types(subplan_info.subplans.size());
			arena_vector<ColumnBinding> required_canonical_bindings(state.allocator);
			column_binding_set_t required_canonical_binding_set;
			for (idx_t subplan_idx = 0; subplan_idx < subplan_info.subplans.size(); subplan_idx++) {
				auto &subplan = subplan_info.subplans[subplan_idx];
				old_bindings[subplan_idx] = subplan.op.get()->GetColumnBindings();
				old_types[subplan_idx] = subplan.op.get()->types;
				for (auto &cb : subplan.canonical_bindings) {
					if (required_canonical_binding_set.find(cb) != required_canonical_binding_set.end()) {
						continue;
					}
					required_canonical_binding_set.insert(cb);
					required_canonical_bindings.push_back(cb);
				}
			}

			auto available_bindings = GetAvailableBindings(*primary_subplan.op.get());
			vector<ColumnBinding> materialized_output_bindings;
			materialized_output_bindings.reserve(required_canonical_bindings.size());
			column_binding_map_t<idx_t> cte_binding_index;
			for (idx_t i = 0; i < required_canonical_bindings.size(); i++) {
				auto &cb = required_canonical_bindings[i];
				cte_binding_index.emplace(cb, i);
				materialized_output_bindings.push_back(available_bindings.Expose(cb));
			}
			ClearProjectionMaps(*primary_subplan.op.get());
			primary_subplan.op.get()->ResolveOperatorTypes();

			const auto materialized_bindings = primary_subplan.op.get()->GetColumnBindings();
			column_binding_map_t<idx_t> materialized_binding_index;
			for (idx_t i = 0; i < materialized_bindings.size(); i++) {
				materialized_binding_index.emplace(materialized_bindings[i], i);
			}
			vector<LogicalType> types;
			for (auto &binding : materialized_output_bindings) {
				types.push_back(primary_subplan.op.get()->types[materialized_binding_index.at(binding)]);
			}

			// Get names
			vector<Identifier> col_names;
			for (idx_t i = 0; i < types.size(); i++) {
				col_names.emplace_back(StringUtil::Format("%s_col_%llu", cte_name, i + 1));
			}
			vector<vector<idx_t>> cte_column_indexes(subplan_info.subplans.size());
			vector<bool> needs_projection(subplan_info.subplans.size(), false);
			for (idx_t subplan_idx = 0; subplan_idx < subplan_info.subplans.size(); subplan_idx++) {
				const auto &subplan = subplan_info.subplans[subplan_idx];
				const auto &canonical_bindings = subplan.canonical_bindings;
				cte_column_indexes[subplan_idx].reserve(canonical_bindings.size());
				needs_projection[subplan_idx] = canonical_bindings.size() != types.size();
				for (idx_t i = 0; i < canonical_bindings.size(); i++) {
					const auto &cb = canonical_bindings[i];
					const auto entry = cte_binding_index.find(cb);
					D_ASSERT(entry != cte_binding_index.end()); // guaranteed by FilterSubplans
					const auto cte_col_idx = entry->second;
					// Types must match: same canonical binding = same base column = same type
					D_ASSERT(old_types[subplan_idx][i] == types[cte_col_idx]);
					cte_column_indexes[subplan_idx].push_back(cte_col_idx);
					needs_projection[subplan_idx] = needs_projection[subplan_idx] || cte_col_idx != i;
				}
			}

			// Create CTE refs and figure out column binding replacements
			vector<unique_ptr<LogicalOperator>> cte_refs;
			ColumnBindingReplacer replacer;
			for (idx_t subplan_idx = 0; subplan_idx < subplan_info.subplans.size(); subplan_idx++) {
				const auto &subplan = subplan_info.subplans[subplan_idx];
				const auto cte_ref_index = optimizer.binder.GenerateTableIndex();
				cte_refs.emplace_back(make_uniq<LogicalCTERef>(cte_ref_index, cte_index, types, col_names));
				if (subplan.op.get()->has_estimated_cardinality) {
					cte_refs.back()->SetEstimatedCardinality(subplan.op.get()->estimated_cardinality);
				}
				auto new_bindings = cte_refs.back()->GetColumnBindings();
				if (needs_projection[subplan_idx]) {
					// Preserve each subplan's original output order when it differs from the
					// materialized CTE.
					vector<unique_ptr<Expression>> select_list;
					for (auto cte_col_idx : cte_column_indexes[subplan_idx]) {
						select_list.emplace_back(make_uniq<BoundColumnRefExpression>(
						    types[cte_col_idx], ColumnBinding(cte_ref_index, ProjectionIndex(cte_col_idx))));
					}

					// Place the projection on top
					auto proj =
					    make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(select_list));
					proj->children.emplace_back(std::move(cte_refs.back()));
					cte_refs.back() = std::move(proj);
					new_bindings = cte_refs.back()->GetColumnBindings();
				}
				D_ASSERT(old_bindings[subplan_idx].size() == new_bindings.size());
				D_ASSERT(subplan.canonical_bindings.size() == new_bindings.size());
				for (idx_t i = 0; i < old_bindings[subplan_idx].size(); i++) {
					replacer.replacement_bindings.emplace_back(old_bindings[subplan_idx][i], new_bindings[i]);
#ifdef D_ASSERT_IS_ENABLED
					const auto inserted = generated_binding_map.emplace(new_bindings[i], subplan.canonical_bindings[i]);
					D_ASSERT(inserted.second);
#else
					generated_binding_map.emplace(new_bindings[i], subplan.canonical_bindings[i]);
#endif
				}
			}

			// Create the materialized CTE and replace the common subplans with references to it
			auto &lowest_common_ancestor = subplan_info.lowest_common_ancestor.get();
			const auto materialized_column_count = types.size();
			auto materialized_subplan = std::move(primary_subplan.op.get());
			auto remainder = std::move(lowest_common_ancestor);
			vector<unique_ptr<Expression>> materialized_select_list;
			for (idx_t i = 0; i < materialized_output_bindings.size(); i++) {
				materialized_select_list.emplace_back(
				    make_uniq<BoundColumnRefExpression>(types[i], materialized_output_bindings[i]));
			}
			auto materialized_projection = make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(),
			                                                            std::move(materialized_select_list));
			materialized_projection->children.emplace_back(std::move(materialized_subplan));
			auto cte = make_uniq<LogicalMaterializedCTE>(Identifier(cte_name), cte_index, materialized_column_count,
			                                             std::move(materialized_projection), std::move(remainder),
			                                             CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
			for (idx_t subplan_idx = 0; subplan_idx < subplan_info.subplans.size(); subplan_idx++) {
				const auto &subplan = subplan_info.subplans[subplan_idx];
				subplan.op.get() = std::move(cte_refs[subplan_idx]);
			}
			lowest_common_ancestor = std::move(cte);

			// Replace bindings of subplans with those of the CTE refs
			replacer.stop_operator = lowest_common_ancestor.get();
			replacer.VisitOperator(*op);                                  // Replace from the root until CTE
			replacer.VisitOperator(*lowest_common_ancestor->children[1]); // Replace in CTE child

			// We have to be careful with the order in which we place the CTEs created by this optimizer
			// Pipeline dependencies cannot be set up if CTEs are in the wrong order
			// This performs "CTE pushdown" until the CTEs are in the correct order
			reference<unique_ptr<LogicalOperator>> current_op = lowest_common_ancestor;
			while (true) {
				auto &rhs_child = current_op.get()->children[1];
				if (rhs_child->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
					const auto child_table_index = rhs_child->Cast<LogicalMaterializedCTE>().table_index;
					if (child_table_index >= min_cte_idx && child_table_index < cte_index) {
						auto tmp = std::move(rhs_child->children[1]);
						rhs_child->children[1] = std::move(current_op.get());
						current_op.get() = std::move(rhs_child);
						rhs_child = std::move(tmp);
						current_op = current_op.get()->children[1];
						continue;
					}
				}
				break;
			}
		}
	}

private:
	bool CanExpandExpressionOutputs(const LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
			break;
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggregate = op.Cast<LogicalAggregate>();
			if (!aggregate.groups.empty() || !aggregate.grouping_sets.empty() ||
			    !aggregate.grouping_functions.empty()) {
				return false;
			}
			for (const auto &expression : aggregate.expressions) {
				if (!CanCombineAggregateState(expression->Cast<BoundAggregateExpression>())) {
					return false;
				}
			}
			break;
		}
		default:
			return false;
		}
		for (const auto &expression : op.expressions) {
			if (!CanShareSubplanExpression(*expression)) {
				return false;
			}
		}
		unordered_map<TableIndex, TableIndex> input_tables;
		for (const auto &binding : GetExpandedColumnBindings(*op.children[0])) {
			const auto canonical_table_index = to_canonical_table_index.at(binding.table_index);
			auto entry = input_tables.emplace(canonical_table_index, binding.table_index);
			if (!entry.second && entry.first->second != binding.table_index) {
				// Expression matching must distinguish repeated occurrences of the same input.
				return false;
			}
		}
		return true;
	}

	void RegisterExpressionBindings(unique_ptr<LogicalOperator> &op) {
		if (!CanExpandExpressionOutputs(*op)) {
			return;
		}
		const auto output_table_index = GetExpressionOutputTableIndex(*op);
		auto &expressions = canonical_expressions[to_canonical_table_index.at(output_table_index)];
		unordered_set<idx_t> used_indexes;
		for (idx_t i = 0; i < op->expressions.size(); i++) {
			auto expression = op->expressions[i]->Copy();
			expression = ExpressionIterator::PreorderReplace<BoundColumnRefExpression>(
			    std::move(expression), [&](const BoundColumnRefExpression &ref) {
				    return ref.WithBinding(GetCanonicalBinding(ref.Binding()));
			    });
			idx_t index = 0;
			for (; index < expressions.size(); index++) {
				if (used_indexes.find(index) == used_indexes.end() &&
				    expression->GetReturnType().EqualsIncludingCollation(expressions[index]->GetReturnType()) &&
				    expression->Equals(*expressions[index])) {
					break;
				}
			}
			if (index == expressions.size()) {
				expressions.push_back(std::move(expression));
			}
			used_indexes.insert(index);
			state.table_index_map.AddColumnMapping(output_table_index, ProjectionIndex(i), ProjectionIndex(index));
		}
	}

	arena_ptr<PlanSignature> CreatePlanSignature(unique_ptr<LogicalOperator> &op) {
		vector<reference<PlanSignature>> child_signatures;
		for (auto &child : op->children) {
			auto it = operator_infos.find(child);
			D_ASSERT(it != operator_infos.end());
			if (!it->second.signature) {
				return nullptr; // Failed to create signature from one of the children
			}
			child_signatures.emplace_back(*it->second.signature);
		}
		vector<vector<ColumnBinding>> positional_inputs;
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT:
		case LogicalOperatorType::LOGICAL_PIVOT:
		case LogicalOperatorType::LOGICAL_GET:
			// These operators consume child values by position rather than by bound expressions.
			for (auto &child : op->children) {
				auto bindings = child->GetColumnBindings();
				for (auto &binding : bindings) {
					binding = GetCanonicalBinding(binding);
				}
				positional_inputs.push_back(std::move(bindings));
			}
			break;
		default:
			break;
		}
		if (CanExpandExpressionOutputs(*op)) {
			// Match the row domain separately from the expandable expression outputs.
			auto expressions = std::move(op->expressions);
			auto signature = PlanSignature::Create(state, *op, std::move(child_signatures), positional_inputs);
			op->expressions = std::move(expressions);
			return signature;
		}
		return PlanSignature::Create(state, *op, std::move(child_signatures), positional_inputs);
	}

	arena_vector<ColumnBinding> GetCanonicalBindings(LogicalOperator &op) {
		// Compute the canonical column bindings coming out of this operator for convenience later
		const auto original_bindings = op.GetColumnBindings();
		return GetCanonicalBindings(original_bindings);
	}

	unique_ptr<LogicalOperator> &LowestCommonAncestor(reference<unique_ptr<LogicalOperator>> a,
	                                                  reference<unique_ptr<LogicalOperator>> b) {
		auto a_it = operator_infos.find(a);
		auto b_it = operator_infos.find(b);
		D_ASSERT(a_it != operator_infos.end() && b_it != operator_infos.end());

		// Get parents of a and b until they're at the same depth
		while (a_it->second.depth > b_it->second.depth) {
			a = a_it->second.parent;
			a_it = operator_infos.find(a);
			D_ASSERT(a_it != operator_infos.end());
		}
		while (b_it->second.depth > a_it->second.depth) {
			b = b_it->second.parent;
			b_it = operator_infos.find(b);
			D_ASSERT(b_it != operator_infos.end());
		}

		// Move up one level at a time for both until ancestor is the same
		while (!RefersToSameObject(a, b)) {
			a_it = operator_infos.find(a);
			b_it = operator_infos.find(b);
			D_ASSERT(a_it != operator_infos.end() && b_it != operator_infos.end());
			a = a_it->second.parent;
			b = b_it->second.parent;
		}

		return a.get();
	}

	ColumnBinding GetCanonicalBinding(const ColumnBinding &binding) {
		auto generated_entry = generated_binding_map.find(binding);
		if (generated_entry != generated_binding_map.end()) {
			return generated_entry->second;
		}
		const auto &table_index_map = state.table_index_map.GetMap();
		const auto canonical_table_index = to_canonical_table_index.at(binding.table_index);
		auto &table_map = table_index_map.at(binding.table_index);
		auto column_index = table_map.Empty<ConversionType::TO_CANONICAL>()
		                        ? binding.column_index
		                        : table_map.Get<ConversionType::TO_CANONICAL>(binding.column_index);
		return ColumnBinding(canonical_table_index, column_index);
	}

	arena_vector<ColumnBinding> GetCanonicalBindings(const vector<ColumnBinding> &original_bindings) {
		arena_vector<ColumnBinding> canonical_bindings(state.allocator);
		for (auto &cb : original_bindings) {
			canonical_bindings.push_back(GetCanonicalBinding(cb));
		}
		return canonical_bindings;
	}

	SubplanBindings GetAvailableBindings(LogicalOperator &op) {
		vector<reference<LogicalGet>> scans;
		vector<reference<LogicalOperator>> expression_sources;
		const auto expanded_bindings = GetExpandedColumnBindings(op, scans, expression_sources);
		const auto canonical_bindings = GetCanonicalBindings(expanded_bindings);
		SubplanBindings result;
		for (idx_t i = 0; i < expanded_bindings.size(); i++) {
			result.bindings.emplace(canonical_bindings[i], expanded_bindings[i]);
		}
		for (auto &scan : scans) {
			if (!CanExpandScan(scan)) {
				continue;
			}
			const auto table_index = to_canonical_table_index.at(scan.get().table_index);
			auto entry = result.scans.emplace(table_index, scan.get());
			if (!entry.second) {
				// Multiple occurrences of the same scan do not identify an unambiguous column source.
				entry.first->second = nullptr;
			}
		}
		for (auto &op : expression_sources) {
			auto table_index = to_canonical_table_index.find(GetExpressionOutputTableIndex(op));
			if (table_index == to_canonical_table_index.end()) {
				continue; // Generated CTE reader projections expose only their existing bindings.
			}
			auto expressions = canonical_expressions.find(table_index->second);
			if (expressions == canonical_expressions.end()) {
				continue;
			}
			auto source = make_uniq<SubplanBindings::ExpressionSource>(op, expressions->second,
			                                                           GetAvailableBindings(*op.get().children[0]));
			auto entry = result.expression_sources.emplace(table_index->second, std::move(source));
			if (!entry.second) {
				entry.first->second.reset();
			}
		}
		return result;
	}

	//! Moves a subplan that can expose every binding required by the duplicate occurrences to the front
	bool SelectPrimarySubplan(SubplanInfo &subplan_info) {
		column_binding_set_t required_bindings;
		for (auto &subplan : subplan_info.subplans) {
			required_bindings.insert(subplan.canonical_bindings.begin(), subplan.canonical_bindings.end());
		}

		optional_idx primary_subplan_idx;
		idx_t primary_subplan_binding_count = 0;
		for (idx_t subplan_idx = 0; subplan_idx < subplan_info.subplans.size(); subplan_idx++) {
			auto &subplan = subplan_info.subplans[subplan_idx];
			const auto available_bindings = GetAvailableBindings(*subplan.op.get());
			bool contains_required_bindings = true;
			for (auto &cb : required_bindings) {
				if (!available_bindings.Contains(cb)) {
					contains_required_bindings = false;
					break;
				}
			}
			if (!contains_required_bindings) {
				continue;
			}
			if (!primary_subplan_idx.IsValid() || subplan.canonical_bindings.size() > primary_subplan_binding_count) {
				primary_subplan_idx = subplan_idx;
				primary_subplan_binding_count = subplan.canonical_bindings.size();
			}
		}
		if (!primary_subplan_idx.IsValid()) {
			return false;
		}
		std::swap(subplan_info.subplans[0], subplan_info.subplans[primary_subplan_idx.GetIndex()]);
		return true;
	}

	static void ClearProjectionMaps(LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op.Cast<LogicalJoin>();
			join.left_projection_map.clear();
			join.right_projection_map.clear();
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER:
			op.Cast<LogicalFilter>().projection_map.clear();
			break;
		case LogicalOperatorType::LOGICAL_ORDER_BY:
			op.Cast<LogicalOrder>().projection_map.clear();
			break;
		default:
			break;
		}
		for (auto &child : op.children) {
			ClearProjectionMaps(*child);
		}
	}

	bool ShouldMaterialize(const SubplanInfo &subplan_info) const {
		auto &subplan = subplan_info.subplans[0].op.get();
		return CTEInlining::EndsInAggregateOrDistinct(*subplan) ||
		       (subplan->has_estimated_cardinality &&
		        IsSelectiveMultiTablePlan(*subplan, subplan->estimated_cardinality));
	}

	static bool CanExpandScan(const LogicalGet &get) {
		return get.children.empty() && !get.has_pushed_projection && !get.extra_info.sample_options &&
		       !get.dynamic_filters;
	}

	static SubplanStats GetSubplanStats(const LogicalOperator &op) {
		SubplanStats subplan_stats;
		if (op.children.empty()) {
			subplan_stats.base_table_count = 1;
			subplan_stats.max_base_table_cardinality = op.has_estimated_cardinality ? op.estimated_cardinality : 0;
		} else {
			subplan_stats = GetSubplanStats(*op.children[0]);
			for (idx_t i = 1; i < op.children.size(); i++) {
				subplan_stats.Combine(GetSubplanStats(*op.children[i]));
			}
		}
		return subplan_stats;
	}

public:
	static bool IsSelectiveMultiTablePlan(const LogicalOperator &op, idx_t cardinality) {
		static constexpr idx_t CARDINALITY_THRESHOLD = 2048;
		static constexpr idx_t CARDINALITY_RATIO = 2;

		// Must select more than 1 base table
		const auto subplan_stats = GetSubplanStats(op);
		if (subplan_stats.base_table_count < 2) {
			return false;
		}

		// If it has this cardinality or less, just materialize
		if (cardinality <= CARDINALITY_THRESHOLD) {
			return true;
		}

		// Otherwise, materialize if it is selective enough
		return cardinality < subplan_stats.max_base_table_cardinality / CARDINALITY_RATIO;
	}

private:
	vector<reference<subplan_map_t::value_type>> GetSortedSubplans() {
		// Grab entries from map, and sort by smallest plans first
		vector<reference<subplan_map_t::value_type>> subplan_infos;
		for (auto &entry : subplans) {
			subplan_infos.push_back(entry);
		}
		std::sort(subplan_infos.begin(), subplan_infos.end(),
		          [](reference<subplan_map_t::value_type> lhs, reference<subplan_map_t::value_type> rhs) {
			          return lhs.get().first.get().OperatorCount() < rhs.get().first.get().OperatorCount();
		          });
		return subplan_infos;
	}

private:
	//! State for creating PlanSignature with reusable data structures
	PlanSignatureCreateState state;
	//! Mapping from operator to info
	reference_map_t<unique_ptr<LogicalOperator>, OperatorInfo> operator_infos;
	//! Mapping from subplan signature to subplan information
	subplan_map_t subplans;
	//! Mapping from original table index to canonical table index
	unordered_map<TableIndex, TableIndex> to_canonical_table_index;
	//! Mapping from generated CTE bindings to canonical bindings
	column_binding_map_t<ColumnBinding> generated_binding_map;
	//! Output expressions in canonical input bindings, indexed by canonical output column.
	unordered_map<TableIndex, vector<unique_ptr<Expression>>> canonical_expressions;
	//! Minimum CTE index created by this optimizer
	TableIndex min_cte_idx;
};

//===--------------------------------------------------------------------===//
// Filtered join inputs
//===--------------------------------------------------------------------===//
class CommonJoinFilters {
	struct Candidate {
		explicit Candidate(unique_ptr<LogicalOperator> &op) : op(op) {
		}

		reference<unique_ptr<LogicalOperator>> op;
		vector<reference<unique_ptr<LogicalOperator>>> scans;
	};

	struct CandidateGroup {
		explicit CandidateGroup(Candidate &candidate) {
			candidates.emplace_back(candidate);
		}

		bool TryAdd(Candidate &candidate) {
			auto &primary = candidates[0].get();
			if (candidate.scans.size() != primary.scans.size()) {
				return false;
			}
			auto varying_scan = differing_scan;
			for (idx_t i = 0; i < primary.scans.size(); i++) {
				auto &left = primary.scans[i].get()->Cast<LogicalGet>();
				auto &right = candidate.scans[i].get()->Cast<LogicalGet>();
				if (left.GetColumnIds() != right.GetColumnIds()) {
					return false;
				}
				if (left.table_filters.Equals(right.table_filters)) {
					continue;
				}
				// One varying input avoids cross-combinations between independent filter disjunctions.
				if (varying_scan.IsValid() && varying_scan.GetIndex() != i) {
					return false;
				}
				varying_scan = i;
			}
			differing_scan = varying_scan;
			candidates.emplace_back(candidate);
			return true;
		}

		vector<reference<Candidate>> candidates;
		optional_idx differing_scan;
	};

public:
	explicit CommonJoinFilters(ClientContext &context) : state(context) {
	}

	void Normalize(unique_ptr<LogicalOperator> &op) {
		FindCandidates(op);
		for (auto &entry : candidates) {
			vector<CandidateGroup> groups;
			for (auto &candidate : entry.second) {
				bool added = false;
				for (auto &group : groups) {
					if (group.TryAdd(candidate)) {
						added = true;
						break;
					}
				}
				if (!added) {
					groups.emplace_back(candidate);
				}
			}
			for (auto &group : groups) {
				NormalizeGroup(group);
			}
		}
	}

private:
	static bool IsInnerJoin(const LogicalOperator &op) {
		return op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		       op.Cast<LogicalComparisonJoin>().join_type == JoinType::INNER;
	}

	optional_ptr<PlanSignature> Signature(unique_ptr<LogicalOperator> &op, Candidate &candidate, bool movable) {
		const bool traverse = movable && (IsInnerJoin(*op) || op->type == LogicalOperatorType::LOGICAL_FILTER);
		if (traverse) {
			bool safe = true;
			LogicalOperatorVisitor::EnumerateExpressions(
			    *op, [&](unique_ptr<Expression> *expr) { safe &= CanShareSubplanExpression(**expr); });
			if (!safe) {
				return nullptr;
			}
		}
		vector<reference<PlanSignature>> children;
		for (auto &child : op->children) {
			auto signature = Signature(child, candidate, traverse);
			if (!signature) {
				return nullptr;
			}
			children.emplace_back(*signature);
		}
		auto create = [&]() {
			signatures.push_back(PlanSignature::Create(state, *op, std::move(children)));
			return optional_ptr<PlanSignature>(signatures.back().get());
		};
		if (!movable || op->type != LogicalOperatorType::LOGICAL_GET) {
			return create();
		}
		auto &get = op->Cast<LogicalGet>();
		if (!get.children.empty() || get.table_filters.HasMultiColumnFilters() || get.dynamic_filters ||
		    get.extra_info.sample_options || get.has_pushed_projection) {
			return create();
		}
		for (auto &entry : get.table_filters) {
			auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "CommonJoinFilters");
			if (!CanShareSubplanExpression(*filter.expr) || ExpressionFilter::IsOptionalFilter(filter)) {
				return create();
			}
		}
		auto filters = std::move(get.table_filters);
		auto signature = create();
		get.table_filters = std::move(filters);
		candidate.scans.emplace_back(op);
		return signature;
	}

	void FindCandidates(unique_ptr<LogicalOperator> &op) {
		if (IsInnerJoin(*op)) {
			Candidate candidate(op);
			auto signature = Signature(op, candidate, true);
			if (signature) {
				candidates[*signature].push_back(std::move(candidate));
			}
			return;
		}
		for (auto &child : op->children) {
			FindCandidates(child);
		}
	}

	static unique_ptr<Expression> ScanPredicate(LogicalGet &get) {
		vector<unique_ptr<Expression>> expressions;
		for (auto &entry : get.table_filters) {
			const auto index = entry.GetIndex();
			BoundColumnRefExpression column(get.GetColumnType(get.GetColumnIds()[index]),
			                                ColumnBinding(get.table_index, index));
			expressions.push_back(entry.Filter().ToExpression(column));
		}
		if (expressions.empty()) {
			return make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		}
		return BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_AND, std::move(expressions));
	}

	static void ExposeFilterColumns(LogicalOperator &op) {
		if (IsInnerJoin(op)) {
			auto &join = op.Cast<LogicalComparisonJoin>();
			join.left_projection_map.clear();
			join.right_projection_map.clear();
		} else if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			op.Cast<LogicalFilter>().projection_map.clear();
		} else {
			return;
		}
		for (auto &child : op.children) {
			ExposeFilterColumns(*child);
		}
	}

	void NormalizeGroup(CandidateGroup &candidate_group) {
		auto &group = candidate_group.candidates;
		auto differing_scan = candidate_group.differing_scan;
		if (group.size() < 2 || !differing_scan.IsValid()) {
			return;
		}
		auto &primary = group[0].get();
		idx_t cardinality = 0;
		for (Candidate &candidate : group) {
			auto &op = *candidate.op.get();
			if (!op.has_estimated_cardinality ||
			    op.estimated_cardinality > NumericLimits<idx_t>::Maximum() - cardinality) {
				return;
			}
			cardinality += op.estimated_cardinality;
		}
		if (!CommonSubplanFinder::IsSelectiveMultiTablePlan(*primary.op.get(), cardinality)) {
			return;
		}

		vector<unique_ptr<Expression>> predicates;
		vector<TableIndex> table_indexes;
		for (Candidate &candidate : group) {
			auto &get = candidate.scans[differing_scan.GetIndex()].get()->Cast<LogicalGet>();
			predicates.push_back(ScanPredicate(get));
			table_indexes.push_back(get.table_index);
		}
		for (idx_t i = 0; i < group.size(); i++) {
			auto &candidate = group[i].get();
			auto &op = candidate.op.get();
			const auto old_cardinality = op->estimated_cardinality;
			const auto old_bindings = op->GetColumnBindings();
			auto &scan = candidate.scans[differing_scan.GetIndex()].get();
			auto &get = scan->Cast<LogicalGet>();
			vector<unique_ptr<Expression>> disjuncts;
			for (idx_t j = 0; j < predicates.size(); j++) {
				auto predicate = predicates[j]->Copy();
				ColumnBindingReplacer replacer;
				for (auto index : ProjectionIndex::GetIndexes(get.GetColumnIds().size())) {
					replacer.replacement_bindings.emplace_back(ColumnBinding(table_indexes[j], index),
					                                           ColumnBinding(get.table_index, index));
				}
				replacer.VisitExpression(&predicate);
				disjuncts.push_back(std::move(predicate));
			}
			get.table_filters.ClearFilters();
			get.projection_ids.clear();
			auto guard = make_uniq<LogicalFilter>(
			    BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_OR, std::move(disjuncts)));
			guard->children.push_back(std::move(scan));
			scan = std::move(guard);
			ExposeFilterColumns(*op);
			op->SetEstimatedCardinality(cardinality);
			auto new_bindings = op->GetColumnBindings();
			auto residual = make_uniq<LogicalFilter>(predicates[i]->Copy());
			for (auto &binding : old_bindings) {
				auto entry = std::find(new_bindings.begin(), new_bindings.end(), binding);
				D_ASSERT(entry != new_bindings.end());
				residual->projection_map.emplace_back(NumericCast<idx_t>(entry - new_bindings.begin()));
			}
			residual->SetEstimatedCardinality(old_cardinality);
			residual->children.push_back(std::move(op));
			op = std::move(residual);
		}
	}

private:
	PlanSignatureCreateState state;
	//! Parent signatures and candidate keys reference these objects.
	vector<arena_ptr<PlanSignature>> signatures;
	unordered_map<reference<PlanSignature>, vector<Candidate>, PlanSignatureHash, PlanSignatureEquality> candidates;
};

//===--------------------------------------------------------------------===//
// Aggregates of UNION ALL branches over one input
//===--------------------------------------------------------------------===//
// The branches of a UNION ALL are often the same aggregate over the same input, restricted to a different value of
// one column each:
//
//     SELECT 'jan' AS month, sum(x) FROM t WHERE m = 1
//     UNION ALL
//     SELECT 'feb' AS month, sum(x) FROM t WHERE m = 2
//
// Every branch then reads the input again. As the values differ, a row of the input belongs to at most one branch,
// and grouping by the column computes all of them at once:
//
//     PROJECTION b.month, <the sum, or its value over no rows where the group is missing>
//       LEFT JOIN b.m = a.m
//         (VALUES (1, 'jan'), (2, 'feb')) b(m, month)
//         AGGREGATE sum(x) GROUP BY m                                                   a
//           t WHERE m IN (1, 2)
//
// What is matched are branches PROJECTION - [FILTER] - AGGREGATE whose plans are the same (PlanSignature) up to
//  - the table filters of one scan below the aggregate, which is reached from it through inner joins, cross products
//    and filters only, so that its columns are columns of the aggregate's input and each of its rows flows into the
//    aggregate as it does in its branch. The filters have to differ in the constants of equality comparisons only
//    (col = constant, on one or more columns), and the constants of two branches have to differ: branches that
//    repeat the constants of an earlier one are left alone, as their rows are needed twice.
//  - constants in the select list and constants among the groups (GROUP BY 'jan').
// Nothing in a branch may be volatile: what does not depend on the compared columns is evaluated once instead of
// once per branch. Expressions that can fail are evaluated for the same rows as before.
//
// The columns that are compared become group columns of the aggregate of the first branch, whose scan is restricted
// to the constants of all branches. A grouped aggregate returns the rows of every branch then. If the branches
// differ in constants of the select list or the groups, they are joined to the groups from a list of the branches.
// A branch without groups returns a row also where no row has its constants: the list of the branches is LEFT
// joined then, and the aggregates of the missing groups are replaced by their values over an empty input, which an
// aggregate over an empty result computes (as the decorrelation of aggregates does).
//
// The rows of the branches come out in a different order. The rewrite is therefore only done where the order of
// the UNION ALL cannot be observed (below a sort, a hash aggregate, a join, ...), as determined like
// RemoveRedundantOrder does, or where all children of the UNION ALL are such branches without groups: their one row
// each is put back into the order of the branches by a sort on the position in the list.
//
// One pass is not always faster than the branches, see IsProfitable.
class UnionAggregateMerge {
	//! The branches are merged if their work per thread is estimated to be at least this many times the work of the
	//! merged plan
	static constexpr double MIN_GAIN = 1.25;
	//! The cost of a row for the merged aggregate, relative to its cost for the aggregate of a branch and to the
	//! cost of a row for a scan
	static constexpr double MERGED_AGGREGATE_COST = 2;
	//! The rows that a scan hands out to a thread at a time: a row group of the default size. This is a number of
	//! rows and deliberately not derived from STANDARD_VECTOR_SIZE, so that the choice does not change with it
	static constexpr double ROWS_PER_SCAN_TASK = 122880;
	//! Constants of the select list are joined to the groups if the aggregate is estimated to return at most one
	//! row for this many rows of its input
	static constexpr double MIN_GROUP_REDUCTION = 16;
	//! The row group statistics of the varying scan are checked against the constants of the branches for at most
	//! this many row groups, spread evenly over the table, and with at most this many checks in total (a numeric
	//! column takes one lookup per row group, other columns one check per branch). With fewer row groups than the
	//! minimum within that limit, and for a table with more row groups than the last limit (2 billion rows at the
	//! default row group size), which are all listed first, the scan counts as one without statistics
	static constexpr idx_t MAX_SAMPLED_ROW_GROUPS = 128;
	static constexpr idx_t MIN_SAMPLED_ROW_GROUPS = 8;
	static constexpr idx_t MAX_ZONEMAP_CHECKS = 100000;
	static constexpr idx_t MAX_ZONEMAP_ROW_GROUPS = 16384;

	//! A UNION ALL child of the form PROJECTION - [FILTER] - AGGREGATE
	struct Branch {
		explicit Branch(idx_t child_index_p) : child_index(child_index_p) {
		}

		//! The position among the children of the UNION ALL
		idx_t child_index;
		optional_ptr<LogicalProjection> projection;
		optional_ptr<LogicalAggregate> aggregate;
		//! The scans below the aggregate whose table filters are not part of the signature
		vector<reference<unique_ptr<LogicalOperator>>> scans;
		//! The constants among the groups of the aggregate and then those of the select list, by position (the same
		//! positions in all branches of a signature)
		vector<pair<idx_t, Value>> constants;
		//! How many of the constants are groups
		idx_t group_constant_count = 0;
		//! The constants that the filters of the varying scan compare its columns with
		vector<Value> keys;
	};

	//! The table filters of a scan as conjuncts over the columns of another scan of the same table
	struct ScanFilters {
		//! column = constant, by column (index in the column ids of the scan)
		map<idx_t, Value> equalities;
		//! Everything else, in the order of the filters
		vector<unique_ptr<Expression>> others;
	};

public:
	explicit UnionAggregateMerge(Optimizer &optimizer_p) : optimizer(optimizer_p) {
	}

	//! order_discarded: whether no operator above op reads the order of its rows (see RemoveRedundantOrder)
	void Optimize(unique_ptr<LogicalOperator> &op, bool order_discarded) {
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_ORDER_BY:
		case LogicalOperatorType::LOGICAL_TOP_N:
			Optimize(op->children[0], true);
			return;
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
			Optimize(op->children[0], RemoveRedundantOrder::AggregateDiscardsOrder(op->Cast<LogicalAggregate>()));
			return;
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT: {
			auto &setop = op->Cast<LogicalSetOperation>();
			const auto is_union_all = op->type == LogicalOperatorType::LOGICAL_UNION && setop.setop_all;
			for (auto &child : op->children) {
				Optimize(child, order_discarded || !is_union_all);
			}
			if (is_union_all && HasAggregateBranches(*op, order_discarded)) {
				// the signatures of the operators below this UNION ALL: an operator has one signature per state
				UnionAggregateMerge merge(optimizer);
				merge.state = make_uniq<PlanSignatureCreateState>(optimizer.context);
				merge.MergeBranches(op, order_discarded);
			}
			return;
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
			for (auto &child : op->children) {
				Optimize(child, true);
			}
			return;
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_UNNEST:
			Optimize(op->children[0], order_discarded);
			return;
		default:
			for (auto &child : op->children) {
				Optimize(child, false);
			}
			return;
		}
	}

private:
	static bool IsUngrouped(const LogicalAggregate &aggregate) {
		return aggregate.groups.empty();
	}

	//! The aggregate of a UNION ALL child of the form PROJECTION - [FILTER] - AGGREGATE
	static optional_ptr<LogicalOperator> BranchAggregate(LogicalOperator &child) {
		if (child.type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return nullptr;
		}
		reference<LogicalOperator> below = *child.children[0];
		if (below.get().type == LogicalOperatorType::LOGICAL_FILTER && !below.get().HasProjectionMap()) {
			below = *below.get().children[0];
		}
		if (below.get().type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			return nullptr;
		}
		return below.get();
	}

	//! Whether the children of the UNION ALL are candidates, which is found without computing anything (a UNION ALL
	//! can have thousands of children that are not): two of them are aggregates, and where the order of the UNION ALL
	//! can be observed all of them are aggregates without groups
	static bool HasAggregateBranches(LogicalOperator &op, bool order_discarded) {
		idx_t count = 0;
		for (auto &child : op.children) {
			auto aggregate = BranchAggregate(*child);
			if (aggregate && (order_discarded || IsUngrouped(aggregate->Cast<LogicalAggregate>()))) {
				count++;
			} else if (!order_discarded) {
				return false;
			}
		}
		return count >= 2;
	}

	//! The signature of the input of an aggregate. The table filters of the scans that are reached through inner
	//! joins, cross products and filters are left out
	optional_ptr<PlanSignature> InputSignature(unique_ptr<LogicalOperator> &op, Branch &branch, bool movable) {
		bool traverse = false;
		if (movable) {
			switch (op->type) {
			case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
				traverse = op->Cast<LogicalComparisonJoin>().join_type == JoinType::INNER && !op->HasProjectionMap();
				break;
			case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
				traverse = true;
				break;
			case LogicalOperatorType::LOGICAL_FILTER:
				traverse = !op->HasProjectionMap();
				break;
			default:
				break;
			}
		}
		vector<reference<PlanSignature>> children;
		for (auto &child : op->children) {
			auto signature = InputSignature(child, branch, traverse);
			if (!signature) {
				return nullptr;
			}
			children.emplace_back(*signature);
		}
		auto create = [&]() {
			signatures.push_back(PlanSignature::Create(*state, *op, std::move(children)));
			return optional_ptr<PlanSignature>(signatures.back().get());
		};
		if (!movable || op->type != LogicalOperatorType::LOGICAL_GET) {
			return create();
		}
		auto &get = op->Cast<LogicalGet>();
		if (!get.children.empty() || get.table_filters.HasMultiColumnFilters() || get.dynamic_filters ||
		    get.extra_info.sample_options || get.has_pushed_projection || !get.projection_ids.empty()) {
			return create();
		}
		for (auto &entry : get.table_filters) {
			auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "UnionAggregateMerge");
			if (ExpressionFilter::IsOptionalFilter(filter) || filter.expr->HasParameter() ||
			    filter.expr->IsVolatile() || ExpressionBarrier::Contains(*filter.expr)) {
				return create();
			}
		}
		auto filters = std::move(get.table_filters);
		get.table_filters = TableFilterSet();
		auto signature = create();
		get.table_filters = std::move(filters);
		branch.scans.emplace_back(op);
		return signature;
	}

	//! The signature of a child of the UNION ALL, if it is PROJECTION - [FILTER] - AGGREGATE. The constants of the
	//! select list and constant groups (GROUP BY 'a') are not part of it
	optional_ptr<PlanSignature> BranchSignature(unique_ptr<LogicalOperator> &child, Branch &branch) {
		auto branch_aggregate = BranchAggregate(*child);
		if (!branch_aggregate) {
			return nullptr;
		}
		auto &aggregate = branch_aggregate->Cast<LogicalAggregate>();
		optional_ptr<LogicalOperator> having;
		if (child->children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
			having = child->children[0].get();
		}
		if (aggregate.grouping_sets.size() > 1 || !aggregate.grouping_functions.empty() ||
		    aggregate.preserve_input_order) {
			return nullptr;
		}
		if (aggregate.grouping_sets.size() == 1 && aggregate.grouping_sets[0].size() != aggregate.groups.size()) {
			return nullptr;
		}
		if (IsUngrouped(aggregate) && (having || aggregate.expressions.empty())) {
			// a branch without groups returns a row for an empty input, which a filter above it could remove
			return nullptr;
		}
		for (auto &expr : aggregate.expressions) {
			auto &bound = expr->Cast<BoundAggregateExpression>();
			if (!bound.GetOrderBys() &&
			    bound.Function().GetOrderDependent() == AggregateOrderDependent::ORDER_DEPENDENT) {
				// the merged plan reads the rows in another order, which first(x) or list(x) show
				return nullptr;
			}
			if (IsUngrouped(aggregate) && expr->CanThrow()) {
				// an aggregate without groups keeps one of its aggregates if none is read (count(*) over the
				// branches), which then fails where the merged aggregate, with its groups, drops them all
				return nullptr;
			}
		}
		branch.projection = child->Cast<LogicalProjection>();
		branch.aggregate = aggregate;

		auto signature = InputSignature(aggregate.children[0], branch, true);
		if (!signature || branch.scans.empty()) {
			return nullptr;
		}
		// constants among the groups and in the select list are compared by their type only
		vector<unique_ptr<Expression>> constants;
		auto hide_constants = [&](vector<unique_ptr<Expression>> &expressions) {
			for (idx_t i = 0; i < expressions.size(); i++) {
				if (expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					continue;
				}
				branch.constants.emplace_back(i, expressions[i]->Cast<BoundConstantExpression>().GetValue());
				auto placeholder = make_uniq<BoundConstantExpression>(Value(expressions[i]->GetReturnType()));
				constants.push_back(std::move(expressions[i]));
				expressions[i] = std::move(placeholder);
			}
		};
		hide_constants(aggregate.groups);
		branch.group_constant_count = constants.size();
		signatures.push_back(PlanSignature::Create(*state, aggregate, {*signature}));
		for (idx_t i = 0; i < constants.size(); i++) {
			aggregate.groups[branch.constants[i].first] = std::move(constants[i]);
		}
		constants.clear();
		signature = signatures.back().get();
		if (signature && having) {
			signatures.push_back(PlanSignature::Create(*state, *having, {*signature}));
			signature = signatures.back().get();
		}
		if (!signature) {
			return nullptr;
		}
		hide_constants(child->expressions);
		signatures.push_back(PlanSignature::Create(*state, *child, {*signature}));
		for (idx_t i = 0; i < constants.size(); i++) {
			child->expressions[branch.constants[branch.group_constant_count + i].first] = std::move(constants[i]);
		}
		return signatures.back().get();
	}

	//! Whether the values of a type that are equal are the same value, so that grouping by a column of the type
	//! separates what the comparisons with different constants separate
	static bool IsKeyType(const LogicalType &type) {
		return !type.IsNested() && type.EqualValuesAreIdentical();
	}

	//! The table filters of a scan, over the columns of the scan `target` of the same table with the same column ids
	static bool GetScanFilters(LogicalGet &get, LogicalGet &target, ScanFilters &result) {
		vector<unique_ptr<Expression>> conjuncts;
		for (auto &entry : get.table_filters) {
			const auto index = entry.GetIndex();
			if (index.GetIndex() >= target.GetColumnIds().size()) {
				return false;
			}
			BoundColumnRefExpression column(target.GetColumnType(target.GetColumnIds()[index.GetIndex()]),
			                                ColumnBinding(target.table_index, index));
			conjuncts.push_back(entry.Filter().ToExpression(column));
		}
		LogicalFilter::SplitPredicates(conjuncts);
		for (auto &conjunct : conjuncts) {
			if (conjunct->GetExpressionType() == ExpressionType::COMPARE_EQUAL &&
			    BoundComparisonExpression::IsComparison(*conjunct)) {
				auto &comparison = conjunct->Cast<BoundFunctionExpression>();
				reference<const Expression> left = BoundComparisonExpression::Left(comparison);
				reference<const Expression> right = BoundComparisonExpression::Right(comparison);
				if (left.get().GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					std::swap(left, right);
				}
				if (left.get().GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    right.get().GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					auto &column = left.get().Cast<BoundColumnRefExpression>();
					auto &constant = right.get().Cast<BoundConstantExpression>().GetValue();
					const auto column_index = column.Binding().column_index.GetIndex();
					if (column.Depth() == 0 && column.Binding().table_index == target.table_index &&
					    !constant.IsNull() && constant.type() == column.GetReturnType() && IsKeyType(constant.type()) &&
					    IsKeyType(column.GetReturnType()) &&
					    result.equalities.find(column_index) == result.equalities.end()) {
						result.equalities.emplace(column_index, constant);
						continue;
					}
				}
			}
			result.others.push_back(std::move(conjunct));
		}
		return true;
	}

	//! Chooses the branches of one signature that can be computed by the aggregate of the first of them: `merged`
	//! holds them, `varying_scan` the scan whose filters differ and `key_columns` its compared columns.
	bool FindMergeableBranches(vector<Branch> &branches, vector<reference<Branch>> &merged, idx_t &varying_scan,
	                           vector<idx_t> &key_columns, ScanFilters &primary_filters) {
		auto &primary = branches[0];
		// the scan whose table filters differ from those of the first branch: the same one in every branch
		optional_idx varying;
		vector<reference<Branch>> candidates;
		for (idx_t b = 1; b < branches.size(); b++) {
			auto &branch = branches[b];
			if (branch.scans.size() != primary.scans.size()) {
				continue;
			}
			optional_idx branch_varying;
			bool valid = true;
			for (idx_t i = 0; i < primary.scans.size() && valid; i++) {
				auto &left = primary.scans[i].get()->Cast<LogicalGet>();
				auto &right = branch.scans[i].get()->Cast<LogicalGet>();
				if (left.GetColumnIds() != right.GetColumnIds()) {
					valid = false;
				} else if (!left.table_filters.Equals(right.table_filters)) {
					valid = !branch_varying.IsValid();
					branch_varying = i;
				}
			}
			if (!valid || !branch_varying.IsValid()) {
				continue;
			}
			if (!varying.IsValid()) {
				varying = branch_varying;
			}
			if (varying.GetIndex() == branch_varying.GetIndex()) {
				candidates.push_back(branch);
			}
		}
		if (!varying.IsValid()) {
			return false;
		}
		varying_scan = varying.GetIndex();
		auto &primary_get = primary.scans[varying_scan].get()->Cast<LogicalGet>();
		if (!GetScanFilters(primary_get, primary_get, primary_filters) || primary_filters.equalities.empty()) {
			return false;
		}
		// the filters of the other branches: the same comparisons, with other constants
		vector<map<idx_t, Value>> candidate_equalities;
		vector<reference<Branch>> comparable;
		for (auto &candidate : candidates) {
			ScanFilters filters;
			auto &get = candidate.get().scans[varying_scan].get()->Cast<LogicalGet>();
			if (!GetScanFilters(get, primary_get, filters) ||
			    filters.equalities.size() != primary_filters.equalities.size() ||
			    filters.others.size() != primary_filters.others.size()) {
				continue;
			}
			bool same = true;
			for (auto &entry : primary_filters.equalities) {
				auto other = filters.equalities.find(entry.first);
				same = same && other != filters.equalities.end() && other->second.type() == entry.second.type();
			}
			for (idx_t i = 0; i < filters.others.size() && same; i++) {
				same = Expression::Equals(*filters.others[i], *primary_filters.others[i]);
			}
			if (!same) {
				continue;
			}
			comparable.push_back(candidate);
			candidate_equalities.push_back(std::move(filters.equalities));
		}
		// the columns whose constants differ
		for (auto &entry : primary_filters.equalities) {
			for (auto &equalities : candidate_equalities) {
				if (!Value::NotDistinctFrom(equalities.at(entry.first), entry.second)) {
					key_columns.push_back(entry.first);
					break;
				}
			}
		}
		if (key_columns.empty()) {
			return false;
		}
		// the branches with constants that no earlier branch has
		value_set_t seen;
		auto add_branch = [&](Branch &branch, const map<idx_t, Value> &equalities) {
			child_list_t<Value> key;
			for (auto column : key_columns) {
				branch.keys.push_back(equalities.at(column));
				key.emplace_back("k" + to_string(key.size()), branch.keys.back());
			}
			if (seen.insert(Value::STRUCT(std::move(key))).second) {
				merged.push_back(branch);
			}
		};
		add_branch(primary, primary_filters.equalities);
		for (idx_t i = 0; i < comparable.size(); i++) {
			add_branch(comparable[i], candidate_equalities[i]);
		}
		return merged.size() >= 2;
	}

	//! The constants of the groups and of the select list that differ between the branches (as indexes into
	//! Branch::constants)
	static vector<idx_t> DifferingConstants(const vector<reference<Branch>> &merged) {
		auto &primary = merged[0].get();
		vector<idx_t> result;
		for (idx_t c = 0; c < primary.constants.size(); c++) {
			for (auto &branch : merged) {
				if (!Value::NotDistinctFrom(branch.get().constants[c].second, primary.constants[c].second)) {
					result.push_back(c);
					break;
				}
			}
		}
		return result;
	}

	//! The rows of the row groups that the varying scan reads, as far as the zonemaps of a sample of its row groups
	//! tell: in all branches together (a row group counts for every branch whose constants it can hold), and in the
	//! merged plan (it counts once if it can hold the constants of any branch). False if that is not known
	bool VaryingScanRows(LogicalGet &get, const vector<reference<Branch>> &merged, const vector<idx_t> &key_columns,
	                     double &branch_rows, double &merged_rows) {
		auto &context = optimizer.context;
		auto table = get.GetTable();
		if (!table || !table->IsDuckTable() || !get.function.get_partition_stats || !get.bind_data) {
			return false;
		}
		vector<StorageIndex> storage_indexes;
		vector<LogicalType> key_types;
		for (auto column : key_columns) {
			StorageIndex storage_index;
			if (!get.TryGetStorageIndex(get.GetColumnIds()[column], storage_index)) {
				return false;
			}
			storage_indexes.push_back(std::move(storage_index));
			key_types.push_back(get.GetColumnType(get.GetColumnIds()[column]));
		}
		// with one numeric column the constants within the bounds of a row group are found in the sorted constants,
		// otherwise every branch is checked against every row group
		const bool sorted_keys =
		    key_columns.size() == 1 && BaseStatistics::GetStatsType(key_types[0]) == StatisticsType::NUMERIC_STATS;
		// the limit is checked before any row group is loaded
		auto &storage = table->Cast<DuckTableEntry>().GetStorage();
		if (storage.GetRowGroupCountWithoutLoading(context) > MAX_ZONEMAP_ROW_GROUPS) {
			return false;
		}
		GetPartitionStatsInput input(get.function, get.bind_data.get());
		auto partitions = get.function.get_partition_stats(context, input);
		if (partitions.empty() || partitions.size() > MAX_ZONEMAP_ROW_GROUPS) {
			return false;
		}
		// only so many row groups are looked at, evenly spread over the table: whether the rows are clustered by
		// the compared columns shows in any part of it, and the work stays the same for a table of any size
		idx_t sample_size = MAX_SAMPLED_ROW_GROUPS;
		if (!sorted_keys) {
			sample_size = MinValue(sample_size, MAX_ZONEMAP_CHECKS / (merged.size() * key_columns.size()));
		}
		if (sample_size < MIN_SAMPLED_ROW_GROUPS && sample_size < partitions.size()) {
			return false;
		}
		const auto stride = (partitions.size() + sample_size - 1) / sample_size;
		vector<Value> keys;
		if (sorted_keys) {
			for (auto &branch : merged) {
				keys.push_back(branch.get().keys[0]);
			}
			std::sort(keys.begin(), keys.end(), ValueOperations::LessThan);
		}
		// the comparisons of the branches, as the scan checks them against the statistics of a row group
		vector<vector<unique_ptr<ExpressionFilter>>> filters;
		for (auto &branch : merged) {
			vector<unique_ptr<ExpressionFilter>> branch_filters;
			for (auto &key : branch.get().keys) {
				branch_filters.push_back(ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL, key));
			}
			filters.push_back(std::move(branch_filters));
		}
		branch_rows = 0;
		merged_rows = 0;
		double table_rows = 0;
		for (auto &partition : partitions) {
			table_rows += double(partition.count);
		}
		double sampled_rows = 0;
		vector<unique_ptr<BaseStatistics>> statistics(key_columns.size());
		for (idx_t partition_idx = 0; partition_idx < partitions.size(); partition_idx += stride) {
			auto &partition = partitions[partition_idx];
			sampled_rows += double(partition.count);
			idx_t readers = merged.size();
			if (partition.partition_row_group) {
				for (idx_t k = 0; k < key_columns.size(); k++) {
					statistics[k] = partition.partition_row_group->GetColumnStatistics(storage_indexes[k]);
					if (statistics[k] && !statistics[k]->GetType().EqualsIncludingCollation(key_types[k])) {
						statistics[k].reset();
					}
				}
				readers = 0;
				if (sorted_keys && statistics[0] && NumericStats::HasMinMax(*statistics[0])) {
					// a comparison with a constant outside of the bounds skips the row group
					auto first = std::lower_bound(keys.begin(), keys.end(), NumericStats::Min(*statistics[0]),
					                              ValueOperations::LessThan);
					auto last = std::upper_bound(keys.begin(), keys.end(), NumericStats::Max(*statistics[0]),
					                             ValueOperations::LessThan);
					readers = first < last ? NumericCast<idx_t>(last - first) : 0;
					branch_rows += double(readers) * double(partition.count);
					merged_rows += readers > 0 ? double(partition.count) : 0;
					continue;
				}
				for (auto &branch_filters : filters) {
					bool skipped = false;
					for (idx_t k = 0; k < key_columns.size() && !skipped; k++) {
						if (!statistics[k]) {
							continue;
						}
						auto pruning = ExpressionFilter::CheckExpressionStatistics(context, *branch_filters[k]->expr,
						                                                           *statistics[k]);
						skipped = pruning == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
						          pruning == FilterPropagateResult::FILTER_FALSE_OR_NULL;
					}
					readers += skipped ? 0 : 1;
				}
			}
			branch_rows += double(readers) * double(partition.count);
			merged_rows += readers > 0 ? double(partition.count) : 0;
		}
		if (sampled_rows <= 0) {
			return false;
		}
		// what the sample says, for the whole table
		branch_rows *= table_rows / sampled_rows;
		merged_rows *= table_rows / sampled_rows;
		return true;
	}

	//! The rows of the scans below an operator other than the varying one, and the rows of the largest of them
	static void OtherScanRows(LogicalOperator &op, const LogicalGet &varying, ClientContext &context, double &rows,
	                          double &largest) {
		if (op.type == LogicalOperatorType::LOGICAL_GET && &op != &varying) {
			auto scan_rows = double(op.EstimateCardinality(context));
			rows += scan_rows;
			largest = MaxValue(largest, scan_rows);
		}
		for (auto &child : op.children) {
			OtherScanRows(*child, varying, context, rows, largest);
		}
	}

	//! Whether one aggregate over one pass is expected to be faster than the branches.
	//!
	//! What the merged plan saves are scans: every branch reads the tables below it again, except for the row groups
	//! of the varying scan that its constants rule out. The rows that reach the aggregates are the same in both
	//! plans, but the merged aggregate has more groups and columns to group by, and where the branches have no
	//! groups it is a hash aggregate instead of a simple one: a row is taken to cost it twice as much. So where the
	//! rows of the varying table are clustered by the compared column, and the branches read disjoint parts of it,
	//! or where a join below the aggregate returns many more rows than the scans read, there is nothing to gain.
	//!
	//! The branches also run next to each other, where the merged plan is one pipeline. That is the same as long as
	//! one scan has work for all threads, but the branches over a small table that few threads can share use more
	//! threads together than the merged plan can. The work of both plans is therefore compared per thread that they
	//! can use, with the largest scan handing out its rows in units of a row group.
	//!
	//! Without statistics of the row groups the branches are assumed to read disjoint parts of the varying table,
	//! and without an estimate of the rows that reach the aggregate the branches are kept.
	bool IsProfitable(const vector<reference<Branch>> &merged, idx_t varying_scan, const vector<idx_t> &key_columns) {
		auto &context = optimizer.context;
		auto &primary = merged[0].get();
		auto &aggregate = *primary.aggregate;
		auto &get = primary.scans[varying_scan].get()->Cast<LogicalGet>();
		const auto branch_count = double(merged.size());

		// the rows that the scans read
		double other_rows = 0;
		double largest_scan = 0;
		OtherScanRows(aggregate, get, context, other_rows, largest_scan);
		double branch_rows;
		double merged_rows;
		if (!VaryingScanRows(get, merged, key_columns, branch_rows, merged_rows)) {
			branch_rows = merged_rows = double(get.EstimateCardinality(context));
		}
		largest_scan = MaxValue(largest_scan, merged_rows);
		const auto threads = double(TaskScheduler::GetScheduler(context).NumberOfThreads());
		const auto scan_tasks = std::ceil(MaxValue<double>(largest_scan, 1) / ROWS_PER_SCAN_TASK);
		const auto branch_threads = MinValue(threads, branch_count * scan_tasks);
		const auto merged_threads = MinValue(threads, scan_tasks);
		//! Whether the merged plan is ahead if this many rows reach the aggregates of all branches together
		const auto is_ahead = [&](double input_rows) {
			const auto branch_work = branch_count * other_rows + branch_rows + input_rows;
			const auto merged_work = other_rows + merged_rows + MERGED_AGGREGATE_COST * input_rows;
			return branch_work / branch_threads >= MIN_GAIN * merged_work / merged_threads;
		};
		if (branch_count * other_rows + branch_rows <= other_rows + merged_rows || !is_ahead(0)) {
			// the scans alone decide against it: the merged plan reads no less, or not enough less - the rows that
			// reach the aggregates only add to the merged plan
			return false;
		}

		// the rows that reach the aggregates
		RelationStatsExtractor extractor(context);
		auto input_stats = extractor.Extract(*aggregate.children[0]);
		if (!input_stats) {
			return false;
		}
		const auto branch_input_rows = double(input_stats->cardinality);
		if (!is_ahead(branch_count * branch_input_rows)) {
			return false;
		}
		if (IsUngrouped(aggregate) || DifferingConstants(merged).empty()) {
			return true;
		}
		// the constants of the select list are joined to the groups: every row of the result is probed, where the
		// branches attach a constant. This is only little work if the aggregate reduces its input to few groups
		bool constant_groups = true;
		for (auto &group : aggregate.groups) {
			constant_groups = constant_groups && group->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
		}
		if (constant_groups) {
			// one row per branch
			return true;
		}
		auto aggregate_stats = extractor.Extract(aggregate);
		if (!aggregate_stats) {
			return false;
		}
		return double(aggregate_stats->cardinality) * MIN_GROUP_REDUCTION <= branch_input_rows;
	}

	//! Restricts the varying scan of the first branch to the constants of all branches
	void RestrictScan(unique_ptr<LogicalOperator> &scan, const vector<reference<Branch>> &merged,
	                  const vector<idx_t> &key_columns, ScanFilters &primary_filters) {
		auto &get = scan->Cast<LogicalGet>();
		auto key_column = [&](idx_t column) {
			return make_uniq<BoundColumnRefExpression>(get.GetColumnType(get.GetColumnIds()[column]),
			                                           ColumnBinding(get.table_index, ProjectionIndex(column)));
		};
		vector<unique_ptr<Expression>> expressions = std::move(primary_filters.others);
		for (auto &entry : primary_filters.equalities) {
			if (std::find(key_columns.begin(), key_columns.end(), entry.first) != key_columns.end()) {
				continue;
			}
			expressions.push_back(BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL,
			                                                        key_column(entry.first),
			                                                        make_uniq<BoundConstantExpression>(entry.second)));
		}
		if (key_columns.size() == 1) {
			vector<Value> values;
			for (auto &branch : merged) {
				values.push_back(branch.get().keys[0]);
			}
			expressions.push_back(ExpressionFilter::CreateInExpression(key_column(key_columns[0]), std::move(values)));
		} else {
			// the values of every column by themselves, which a scan can check, and their combinations
			for (idx_t k = 0; k < key_columns.size(); k++) {
				value_set_t seen;
				vector<Value> values;
				for (auto &branch : merged) {
					if (seen.insert(branch.get().keys[k]).second) {
						values.push_back(branch.get().keys[k]);
					}
				}
				if (values.size() == 1) {
					expressions.push_back(
					    BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, key_column(key_columns[k]),
					                                      make_uniq<BoundConstantExpression>(values[0])));
					continue;
				}
				expressions.push_back(
				    ExpressionFilter::CreateInExpression(key_column(key_columns[k]), std::move(values)));
			}
			vector<unique_ptr<Expression>> disjuncts;
			for (auto &branch : merged) {
				vector<unique_ptr<Expression>> conjuncts;
				for (idx_t k = 0; k < key_columns.size(); k++) {
					conjuncts.push_back(
					    BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, key_column(key_columns[k]),
					                                      make_uniq<BoundConstantExpression>(branch.get().keys[k])));
				}
				disjuncts.push_back(
				    BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_AND, std::move(conjuncts)));
			}
			expressions.push_back(
			    BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_OR, std::move(disjuncts)));
		}
		get.table_filters.ClearFilters();
		auto filter = make_uniq<LogicalFilter>();
		filter->expressions = std::move(expressions);
		filter->children.push_back(std::move(scan));
		FilterPushdown pushdown(optimizer);
		scan = pushdown.Rewrite(std::move(filter));
	}

	//! Replaces the references to the aggregates of a branch without groups: where its group is missing, an
	//! aggregate has the value that it has over an empty input
	static void ReplaceMissingAggregates(unique_ptr<Expression> &expr, TableIndex aggregate_index,
	                                     TableIndex empty_index, const ColumnBinding &marker,
	                                     const LogicalType &marker_type) {
		if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &column = expr->Cast<BoundColumnRefExpression>();
			if (column.Binding().table_index != aggregate_index) {
				return;
			}
			auto is_missing =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN,
			                                       make_uniq<BoundColumnRefExpression>(marker_type, marker));
			auto empty = make_uniq<BoundColumnRefExpression>(column.GetReturnType(),
			                                                 ColumnBinding(empty_index, column.Binding().column_index));
			auto alias = expr->GetAlias();
			auto result = make_uniq<BoundCaseExpression>(std::move(is_missing), std::move(empty), std::move(expr));
			result->SetAlias(alias);
			expr = std::move(result);
			return;
		}
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
			ReplaceMissingAggregates(child, aggregate_index, empty_index, marker, marker_type);
		});
	}

	//! Makes the aggregate of the first branch compute all of the branches
	void Merge(const vector<reference<Branch>> &merged, idx_t varying_scan, const vector<idx_t> &key_columns,
	           ScanFilters &primary_filters, bool order_discarded) {
		auto &binder = optimizer.binder;
		auto &primary = merged[0].get();
		auto &projection = *primary.projection;
		auto &aggregate = *primary.aggregate;
		auto &get = primary.scans[varying_scan].get()->Cast<LogicalGet>();
		const auto ungrouped = IsUngrouped(aggregate);

		// an aggregate over no rows, for the values of the aggregates of a missing group
		unique_ptr<LogicalOperator> empty_aggregate;
		TableIndex empty_index;
		if (ungrouped) {
			auto &input = *aggregate.children[0];
			input.ResolveOperatorTypes();
			auto input_bindings = input.GetColumnBindings();
			auto empty_bindings =
			    LogicalOperator::GenerateColumnBindings(binder.GenerateTableIndex(), input_bindings.size());
			vector<unique_ptr<Expression>> aggregates;
			for (auto &expr : aggregate.expressions) {
				aggregates.push_back(expr->Copy());
			}
			empty_index = binder.GenerateTableIndex();
			empty_aggregate =
			    make_uniq<LogicalAggregate>(binder.GenerateTableIndex(), empty_index, std::move(aggregates));
			ColumnBindingReplacer replacer;
			replacer.AddReplacements(input_bindings, empty_bindings);
			replacer.VisitOperatorBindings(*empty_aggregate);
			empty_aggregate->children.push_back(make_uniq<LogicalEmptyResult>(input.types, empty_bindings));
		}

		// group by the compared columns
		vector<LogicalType> key_types;
		vector<ColumnBinding> key_bindings;
		for (auto column : key_columns) {
			key_types.push_back(get.GetColumnType(get.GetColumnIds()[column]));
			auto group = make_uniq<BoundColumnRefExpression>(key_types.back(),
			                                                 ColumnBinding(get.table_index, ProjectionIndex(column)));
			auto group_index = ColumnBinding::PushExpression(aggregate.groups, std::move(group));
			for (auto &set : aggregate.grouping_sets) {
				set.insert(group_index);
			}
			if (!aggregate.group_stats.empty()) {
				aggregate.group_stats.push_back(nullptr);
			}
			key_bindings.emplace_back(aggregate.group_index, group_index);
		}
		RestrictScan(primary.scans[varying_scan].get(), merged, key_columns, primary_filters);

		// the constants of the select list that differ
		auto constants = DifferingConstants(merged);
		const bool restore_order = ungrouped && !order_discarded;
		if (!ungrouped && constants.empty()) {
			return;
		}
		// the list of the branches: their keys, constants and position
		vector<LogicalType> list_types = key_types;
		for (auto c : constants) {
			list_types.push_back(primary.constants[c].second.type());
		}
		if (restore_order) {
			list_types.push_back(LogicalType::BIGINT);
		}
		vector<vector<unique_ptr<Expression>>> rows;
		for (idx_t b = 0; b < merged.size(); b++) {
			auto &branch = merged[b].get();
			vector<unique_ptr<Expression>> row;
			for (auto &key : branch.keys) {
				row.push_back(make_uniq<BoundConstantExpression>(key));
			}
			for (auto c : constants) {
				row.push_back(make_uniq<BoundConstantExpression>(branch.constants[c].second));
			}
			if (restore_order) {
				row.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(b))));
			}
			rows.push_back(std::move(row));
		}
		const auto list_index = binder.GenerateTableIndex();
		unique_ptr<LogicalOperator> list = make_uniq<LogicalExpressionGet>(list_index, list_types, std::move(rows));
		list->children.push_back(make_uniq<LogicalDummyScan>(binder.GenerateTableIndex()));

		auto join = make_uniq<LogicalComparisonJoin>(ungrouped ? JoinType::LEFT : JoinType::INNER);
		for (idx_t k = 0; k < key_columns.size(); k++) {
			auto list_key =
			    make_uniq<BoundColumnRefExpression>(key_types[k], ColumnBinding(list_index, ProjectionIndex(k)));
			auto group_key = make_uniq<BoundColumnRefExpression>(key_types[k], key_bindings[k]);
			join->conditions.emplace_back(std::move(list_key), std::move(group_key), ExpressionType::COMPARE_EQUAL);
		}
		if (ungrouped) {
			list = LogicalCrossProduct::Create(std::move(list), std::move(empty_aggregate));
		}
		// the list is joined to the groups below a HAVING, which can read a constant group
		reference<unique_ptr<LogicalOperator>> groups = projection.children[0];
		optional_ptr<LogicalOperator> having;
		if (groups.get()->type == LogicalOperatorType::LOGICAL_FILTER) {
			having = groups.get().get();
			groups = having->children[0];
		}
		join->children.push_back(std::move(list));
		join->children.push_back(std::move(groups.get()));
		groups.get() = std::move(join);

		ColumnBindingReplacer group_replacer;
		for (idx_t c = 0; c < constants.size(); c++) {
			const auto list_binding = ColumnBinding(list_index, ProjectionIndex(key_types.size() + c));
			auto &constant = primary.constants[constants[c]];
			if (constants[c] < primary.group_constant_count) {
				// a constant group: what reads it above the aggregate reads the list
				group_replacer.replacement_bindings.emplace_back(
				    ColumnBinding(aggregate.group_index, ProjectionIndex(constant.first)), list_binding);
				continue;
			}
			auto &expr = projection.expressions[constant.first];
			auto alias = expr->GetAlias();
			expr = make_uniq<BoundColumnRefExpression>(list_types[key_types.size() + c], list_binding);
			expr->SetAlias(alias);
		}
		if (!group_replacer.replacement_bindings.empty()) {
			if (having) {
				group_replacer.VisitOperatorBindings(*having);
			}
			group_replacer.VisitOperatorBindings(projection);
		}
		if (ungrouped) {
			for (auto &expr : projection.expressions) {
				ReplaceMissingAggregates(expr, aggregate.aggregate_index, empty_index, key_bindings[0], key_types[0]);
			}
		}
		if (restore_order) {
			// the one row of every branch, in the order of the branches
			auto position = ColumnBinding(list_index, ProjectionIndex(list_types.size() - 1));
			auto order_node = BoundOrderByNode(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
			                                   make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, position));
			vector<BoundOrderByNode> orders;
			orders.push_back(std::move(order_node));
			auto order = make_uniq<LogicalOrder>(std::move(orders));
			order->children.push_back(std::move(projection.children[0]));
			projection.children[0] = std::move(order);
		}
	}

	void MergeBranches(unique_ptr<LogicalOperator> &op, bool order_discarded) {
		auto &setop = op->Cast<LogicalSetOperation>();
		if (op->children.size() < 2) {
			return;
		}
		// the children by signature, in the order of the first child of each
		unordered_map<reference<PlanSignature>, idx_t, PlanSignatureHash, PlanSignatureEquality> signature_groups;
		vector<vector<Branch>> groups;
		for (idx_t i = 0; i < op->children.size(); i++) {
			Branch branch(i);
			auto signature = BranchSignature(op->children[i], branch);
			if (!signature) {
				continue;
			}
			auto entry = signature_groups.emplace(*signature, groups.size());
			if (entry.second) {
				groups.emplace_back();
			}
			groups[entry.first->second].push_back(std::move(branch));
		}
		unordered_set<idx_t> removed;
		for (auto &branches : groups) {
			if (branches.size() < 2) {
				continue;
			}
			const auto ungrouped = IsUngrouped(*branches[0].aggregate);
			if (!ungrouped && !order_discarded) {
				continue;
			}
			if (ungrouped && !order_discarded && branches.size() != op->children.size()) {
				// the sort that restores the order of the branches cannot keep the place of other children
				continue;
			}
			vector<reference<Branch>> merged;
			idx_t varying_scan;
			vector<idx_t> key_columns;
			ScanFilters primary_filters;
			if (!FindMergeableBranches(branches, merged, varying_scan, key_columns, primary_filters)) {
				continue;
			}
			if (ungrouped && !order_discarded && merged.size() != op->children.size()) {
				continue;
			}
			if (!IsProfitable(merged, varying_scan, key_columns)) {
				continue;
			}
			Merge(merged, varying_scan, key_columns, primary_filters, order_discarded);
			for (idx_t b = 1; b < merged.size(); b++) {
				removed.insert(merged[b].get().child_index);
			}
		}
		if (removed.empty()) {
			return;
		}
		vector<unique_ptr<LogicalOperator>> children;
		for (idx_t i = 0; i < op->children.size(); i++) {
			if (removed.find(i) == removed.end()) {
				children.push_back(std::move(op->children[i]));
			}
		}
		if (children.size() > 1) {
			op->children = std::move(children);
			return;
		}
		// one branch is left: it takes the place of the UNION ALL
		auto &child = children[0];
		child->ResolveOperatorTypes();
		auto bindings = child->GetColumnBindings();
		vector<unique_ptr<Expression>> expressions;
		for (idx_t i = 0; i < bindings.size(); i++) {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(child->types[i], bindings[i]));
		}
		auto projection = make_uniq<LogicalProjection>(setop.table_index, std::move(expressions));
		projection->children.push_back(std::move(child));
		op = std::move(projection);
	}

private:
	Optimizer &optimizer;
	unique_ptr<PlanSignatureCreateState> state;
	//! The signatures reference each other
	vector<arena_ptr<PlanSignature>> signatures;
};

UnionAggregateMerger::UnionAggregateMerger(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

unique_ptr<LogicalOperator> UnionAggregateMerger::Optimize(unique_ptr<LogicalOperator> op) {
	UnionAggregateMerge merge(optimizer);
	// the order of the query result is observable
	merge.Optimize(op, false);
	return op;
}

//===--------------------------------------------------------------------===//
// CommonSubplanOptimizer
//===--------------------------------------------------------------------===//
CommonSubplanOptimizer::CommonSubplanOptimizer(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

unique_ptr<LogicalOperator> CommonSubplanOptimizer::Optimize(unique_ptr<LogicalOperator> op) {
	CommonJoinFilters join_filters(optimizer.context);
	join_filters.Normalize(op);
	// Bottom-up identification of identical subplans
	CommonSubplanFinder finder(optimizer.context);
	finder.FindCommonSubplans(op);
	finder.FilterSubplans();
	finder.ConvertSubplansToCTEs(optimizer, op);
	return op;
}

} // namespace duckdb
