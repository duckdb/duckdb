#include "duckdb/planner/operator/logical_limit_percent.hpp"

namespace duckdb {

void LogicalLimitPercent::Serialize(FieldWriter &writer) const {
	writer.WriteField(limit_percent);
	writer.WriteField(offset_val);
	writer.WriteOptional(limit);
	writer.WriteOptional(offset);
}

unique_ptr<LogicalOperator> LogicalLimitPercent::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                             FieldReader &reader) {
	auto limit_percent = reader.ReadRequired<double>();
	auto offset_val = reader.ReadRequired<int64_t>();
	unique_ptr<Expression> limit = reader.ReadOptional<Expression>(move(limit), context);
	unique_ptr<Expression> offset = reader.ReadOptional<Expression>(move(offset), context);
	return make_unique<LogicalLimitPercent>(limit_percent, offset_val, move(limit), move(offset));
}
} // namespace duckdb
