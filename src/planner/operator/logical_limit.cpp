#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

LogicalLimit::LogicalLimit(int64_t limit_val, int64_t offset_val, unique_ptr<Expression> limit,
                           unique_ptr<Expression> offset)
    : LogicalOperator(LogicalOperatorType::LOGICAL_LIMIT), limit_val(limit_val), offset_val(offset_val),
      limit(std::move(limit)), offset(std::move(offset)) {
}

vector<ColumnBinding> LogicalLimit::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

idx_t LogicalLimit::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = children[0]->EstimateCardinality(context);
	if (limit_val >= 0 && idx_t(limit_val) < child_cardinality) {
		child_cardinality = limit_val;
	}
	return child_cardinality;
}

void LogicalLimit::ResolveTypes() {
	types = children[0]->types;
}

void LogicalLimit::Serialize(FieldWriter &writer) const {
	writer.WriteField(limit_val);
	writer.WriteField(offset_val);
	writer.WriteOptional(limit);
	writer.WriteOptional(offset);
}

unique_ptr<LogicalOperator> LogicalLimit::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto limit_val = reader.ReadRequired<int64_t>();
	auto offset_val = reader.ReadRequired<int64_t>();
	auto limit = reader.ReadOptional<Expression>(nullptr, state.gstate);
	auto offset = reader.ReadOptional<Expression>(nullptr, state.gstate);
	return make_unique<LogicalLimit>(limit_val, offset_val, std::move(limit), std::move(offset));
}

} // namespace duckdb
