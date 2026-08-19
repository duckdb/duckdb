#include "duckdb/execution/operator/helper/physical_secure_view.hpp"

namespace duckdb {

PhysicalSecureView::PhysicalSecureView(PhysicalPlan &physical_plan, PhysicalOperator &child, string view_name_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::SECURE_VIEW, child.GetTypes(), child.estimated_cardinality),
      view_name(std::move(view_name_p)) {
	children.push_back(child);
}

OperatorResultType PhysicalSecureView::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state) const {
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

InsertionOrderPreservingMap<string> PhysicalSecureView::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["View"] = view_name;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
