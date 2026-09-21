#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

LogicalSecureView::LogicalSecureView() : LogicalOperator(LogicalOperatorType::LOGICAL_SECURE_VIEW) {
}

LogicalSecureView::LogicalSecureView(string view_name_p, unique_ptr<LogicalOperator> child)
    : LogicalOperator(LogicalOperatorType::LOGICAL_SECURE_VIEW), view_name(std::move(view_name_p)) {
	children.push_back(std::move(child));
}

LogicalSecureView::LogicalSecureView(string view_name_p, QualifiedName source_name_p,
                                     vector<LogicalType> source_types_p, optional_ptr<BoundAtClause> at_clause,
                                     unique_ptr<LogicalOperator> child)
    : LogicalOperator(LogicalOperatorType::LOGICAL_SECURE_VIEW), view_name(std::move(view_name_p)), has_source(true),
      source_name(std::move(source_name_p)), source_types(std::move(source_types_p)),
      has_at_clause(at_clause != nullptr) {
	if (at_clause) {
		at_unit = at_clause->Unit();
		at_value = at_clause->GetValue();
	}
	output_bindings = child->GetColumnBindings();
	D_ASSERT(output_bindings.size() == source_types.size());
	for (idx_t i = 0; i < source_types.size(); i++) {
		output_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(source_types[i], ColumnBinding(TableIndex(0), ProjectionIndex(i))));
	}
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
