#include "duckdb/planner/operator/logical_secure_view.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

LogicalSecureView::LogicalSecureView() : LogicalOperator(LogicalOperatorType::LOGICAL_SECURE_VIEW) {
}

LogicalSecureView::LogicalSecureView(string view_name_p, unique_ptr<LogicalOperator> child)
    : LogicalOperator(LogicalOperatorType::LOGICAL_SECURE_VIEW), view_name(std::move(view_name_p)) {
	children.push_back(std::move(child));
}

vector<ColumnBinding> LogicalSecureView::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

idx_t LogicalSecureView::EstimateCardinality(ClientContext &context) {
	if (has_estimated_cardinality) {
		// the estimate is frozen before filters are pushed into the view - what the optimizer derives from those
		// filters using the statistics of the view contents must not escape the boundary
		return estimated_cardinality;
	}
	return children[0]->EstimateCardinality(context);
}

InsertionOrderPreservingMap<string> LogicalSecureView::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["View"] = view_name;
	if (!pushed_filters.empty()) {
		result["Filters"] = StringUtil::Join(pushed_filters, "\n");
	}
	SetParamsEstimatedCardinality(result);
	return result;
}

void LogicalSecureView::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb
