#include "duckdb/planner/logical_operator_copy.hpp"

#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

static void CheckCopyFilter(const TableFilter &filter) {
	auto &expression = *ExpressionFilter::GetExpressionFilter(filter, "bound plan copy").expr;
	for (auto name : {DynamicFilterScalarFun::NAME, BloomFilterScalarFun::NAME, PrefixRangeScalarFun::NAME}) {
		if (ExpressionFilter::ContainsInternalFunction(expression, name)) {
			throw NotImplementedException("Bound plan copy does not support shared dynamic filter state");
		}
	}
}

void LogicalOperatorCopyState::Validate(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (get.bind_info || get.dynamic_filters) {
			throw NotImplementedException(
			    "Bound plan copy does not support process-local bind input or dynamic filters");
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
			throw NotImplementedException("Bound plan copy does not support shared join filters");
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
	for (auto &child : op.children) {
		Validate(*child);
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
}

void LogicalOperatorCopyState::CopyCardinality(const LogicalOperator &source, LogicalOperator &target) {
	if (source.type != target.type || source.children.size() != target.children.size()) {
		throw NotImplementedException("Bound plan copy changed native operator topology");
	}
	target.has_estimated_cardinality = source.has_estimated_cardinality;
	target.estimated_cardinality = source.estimated_cardinality;
	for (idx_t i = 0; i < source.children.size(); i++) {
		CopyCardinality(*source.children[i], *target.children[i]);
	}
}

} // namespace duckdb
