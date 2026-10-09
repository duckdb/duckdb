#include "duckdb/planner/logical_operator_copy.hpp"

#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

static void CheckCopyExpression(const Expression &expression) {
	for (auto name : {DynamicFilterScalarFun::NAME, BloomFilterScalarFun::NAME, PrefixRangeScalarFun::NAME}) {
		if (ExpressionFilter::ContainsInternalFunction(expression, name)) {
			throw NotImplementedException("Bound plan copy does not support shared dynamic filter state");
		}
	}
}

static void CheckCopyFilter(const TableFilter &filter) {
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		throw NotImplementedException("Bound plan copy does not support non-expression table filters");
	}
	CheckCopyExpression(*ExpressionFilter::GetExpressionFilter(filter, "bound plan copy").expr);
}

static bool ContainsScan(const LogicalOperator &op, const LogicalGet &scan) {
	if (&op == &scan) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsScan(*child, scan)) {
			return true;
		}
	}
	return false;
}

static bool IsScanBinding(const LogicalGet &scan, const ColumnBinding &binding) {
	if (binding.table_index != scan.table_index || binding.column_index.GetIndex() >= scan.GetColumnIds().size()) {
		return false;
	}
	if (scan.projection_ids.empty()) {
		return true;
	}
	for (auto index : scan.projection_ids) {
		if (index == binding.column_index) {
			return true;
		}
	}
	return false;
}

void LogicalOperatorCopyState::ValidateJoin(const LogicalComparisonJoin &join) {
	if (join.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN || join.join_type != JoinType::INNER ||
	    join.children.size() != 2 || join.filter_pushdown->join_condition.empty()) {
		throw NotImplementedException(
		    "Bound plan copy does not support shared join filters without native INNER descriptors");
	}
	auto &info = *join.filter_pushdown;
	if (!join_filters.emplace(join, nullptr).second) {
		throw NotImplementedException("Bound plan copy requires a tree of join filter producers");
	}
	if (info.min_max_aggregates.size() != 2 * info.join_condition.size()) {
		throw NotImplementedException("Bound plan copy requires complete join filter aggregates");
	}
	for (auto index : info.join_condition) {
		if (index >= join.conditions.size() || !join.conditions[index].IsComparison()) {
			throw NotImplementedException("Bound plan copy requires valid join filter conditions");
		}
		switch (join.conditions[index].GetComparisonType()) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			break;
		default:
			throw NotImplementedException("Bound plan copy requires native join filter comparisons");
		}
	}
	for (auto &expression : info.min_max_aggregates) {
		if (!expression || expression->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			throw NotImplementedException("Bound plan copy requires native join filter aggregates");
		}
		CheckCopyExpression(*expression);
	}
	for (auto &probe : info.probe_info) {
		if (!probe.dynamic_filters || probe.columns.empty()) {
			throw NotImplementedException("Bound plan copy requires complete join filter targets");
		}
		for (auto &column : probe.columns) {
			if (column.join_filter_idx >= info.join_condition.size()) {
				throw NotImplementedException("Bound plan copy requires valid join filter target columns");
			}
		}
		dynamic_filters[*probe.dynamic_filters].producers.push_back(join);
	}
}

void LogicalOperatorCopyState::Validate(const LogicalOperator &op) {
	ValidateOperator(op);
	for (auto &entry : dynamic_filters) {
		auto &filters = entry.second;
		if (!filters.scan || filters.producers.empty() || entry.first.get().HasFilters()) {
			throw NotImplementedException("Bound plan copy requires unexecuted, connected join dynamic filters");
		}
		for (auto &producer : filters.producers) {
			auto &join = producer.get();
			if (!ContainsScan(*join.children[0], *filters.scan)) {
				throw NotImplementedException("Bound plan copy requires join dynamic filters within the probe subtree");
			}
			for (auto &probe : join.filter_pushdown->probe_info) {
				if (probe.dynamic_filters.get() != &entry.first.get()) {
					continue;
				}
				for (auto &column : probe.columns) {
					if (!IsScanBinding(*filters.scan, column.probe_column_index)) {
						throw NotImplementedException("Bound plan copy requires bound join filter target columns");
					}
				}
			}
		}
		filters.copy = make_shared_ptr<DynamicTableFilterSet>();
	}
	for (auto &join : join_filters) {
		join.second = CopyJoinFilters(*join.first.get().filter_pushdown);
	}
}

void LogicalOperatorCopyState::ValidateOperator(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (get.bind_info) {
			throw NotImplementedException("Bound plan copy does not support process-local bind input");
		}
		if (get.dynamic_filters) {
			auto &filters = dynamic_filters[*get.dynamic_filters];
			if (filters.scan) {
				throw NotImplementedException("Bound plan copy requires one scan per join dynamic filter set");
			}
			filters.scan = get;
		}
		for (auto &entry : get.table_filters) {
			CheckCopyFilter(entry.Filter());
		}
		for (auto &filter : get.table_filters.GetMultiColumnFilters()) {
			CheckCopyFilter(*filter);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_TOP_N:
		if (op.Cast<LogicalTopN>().dynamic_filter) {
			throw NotImplementedException("Bound plan copy does not support shared top-N filters");
		}
		break;
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		if (op.Cast<LogicalComparisonJoin>().filter_pushdown) {
			ValidateJoin(op.Cast<LogicalComparisonJoin>());
		}
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_SECURE_VIEW:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
	case LogicalOperatorType::LOGICAL_CTE_REF:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		break;
	default:
		throw NotImplementedException("Bound plan copy does not support operator %s", op.GetName());
	}
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [](const unique_ptr<Expression> *expression) { CheckCopyExpression(**expression); });
	for (auto &child : op.children) {
		ValidateOperator(*child);
	}
}

idx_t LogicalOperatorCopyState::CopyScan(const LogicalGet &get) {
	auto scan = make_uniq<Scan>();
	scan->function = get.function;
	if (get.bind_data) {
		scan->bind_data = get.bind_data->Copy();
		if (!scan->bind_data) {
			throw InternalException("Bound plan copy received a null bind-data copy for %s", get.function.GetName());
		}
	}
	scan->virtual_columns = get.virtual_columns;
	auto index = scans.size();
	scans.push_back(std::move(scan));
	return index;
}

unique_ptr<LogicalOperatorCopyState::Scan> LogicalOperatorCopyState::TakeScan(idx_t index) {
	if (index >= scans.size() || !scans[index]) {
		throw InternalException("Bound plan copy encountered an invalid or repeated scan identifier");
	}
	return std::move(scans[index]);
}

void LogicalOperatorCopyState::VerifyConsumed() const {
	for (auto &scan : scans) {
		if (scan) {
			throw InternalException("Bound plan copy did not consume every scan binding");
		}
	}
	for (auto &join : join_filters) {
		if (join.second) {
			throw InternalException("Bound plan copy did not consume every join filter descriptor");
		}
	}
	for (auto &entry : dynamic_filters) {
		if (!entry.second.attached) {
			throw InternalException("Bound plan copy did not attach every join dynamic filter set");
		}
	}
}

unique_ptr<JoinFilterPushdownInfo> LogicalOperatorCopyState::CopyJoinFilters(const JoinFilterPushdownInfo &source) {
	auto result = make_uniq<JoinFilterPushdownInfo>();
	result->join_condition = source.join_condition;
	// The native min/max-only descriptor does not initialize this unused flag.
	result->build_side_has_filter = !source.probe_info.empty() && source.build_side_has_filter;
	for (auto &probe : source.probe_info) {
		JoinFilterPushdownFilter copy;
		copy.columns = probe.columns;
		copy.dynamic_filters = dynamic_filters.at(*probe.dynamic_filters).copy;
		result->probe_info.push_back(std::move(copy));
	}
	return result;
}

void LogicalOperatorCopyState::SerializeJoinExpressions(BinarySerializer &serializer) const {
	// Private descriptors follow the same expression reconstruction as the copied join conditions.
	// The descriptor map is unchanged between this walk and DeserializeJoinExpressions.
	for (auto &join : join_filters) {
		for (auto &expression : join.first.get().filter_pushdown->min_max_aggregates) {
			serializer.Begin();
			expression->Serialize(serializer);
			serializer.End();
		}
	}
}

void LogicalOperatorCopyState::DeserializeJoinExpressions(BinaryDeserializer &deserializer) {
	for (auto &join : join_filters) {
		for (idx_t i = 0; i < join.first.get().filter_pushdown->min_max_aggregates.size(); i++) {
			join.second->min_max_aggregates.push_back(deserializer.Deserialize<Expression>());
		}
	}
}

void LogicalOperatorCopyState::CopyAnnotations(const LogicalOperator &source, LogicalOperator &target) {
	if (source.type != target.type || source.children.size() != target.children.size()) {
		throw NotImplementedException("Bound plan copy changed native operator topology");
	}
	target.has_estimated_cardinality = source.has_estimated_cardinality;
	target.estimated_cardinality = source.estimated_cardinality;
	if (source.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = source.Cast<LogicalGet>();
		if (get.dynamic_filters) {
			auto &filters = dynamic_filters.at(*get.dynamic_filters);
			if (filters.attached) {
				throw InternalException("Bound plan copy encountered a repeated join dynamic filter scan");
			}
			target.Cast<LogicalGet>().dynamic_filters = filters.copy;
			filters.attached = true;
		}
	} else if (source.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = source.Cast<LogicalComparisonJoin>();
		if (join.filter_pushdown) {
			auto &filters = join_filters.at(join);
			if (!filters) {
				throw InternalException("Bound plan copy encountered a repeated join filter descriptor");
			}
			target.Cast<LogicalComparisonJoin>().filter_pushdown = std::move(filters);
		}
	}
	for (idx_t i = 0; i < source.children.size(); i++) {
		CopyAnnotations(*source.children[i], *target.children[i]);
	}
}

} // namespace duckdb
