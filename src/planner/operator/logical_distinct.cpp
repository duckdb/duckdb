#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

string LogicalDistinct::ParamsToString() const {
	string result = LogicalOperator::ParamsToString();
	if (!distinct_targets.empty()) {
		result += StringUtil::Join(distinct_targets, distinct_targets.size(), "\n",
		                           [](const unique_ptr<Expression> &child) { return child->GetName(); });
	}

	return result;
}
void LogicalDistinct::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(distinct_targets);
	if (order_by) {
		throw NotImplementedException("Serializing ORDER BY not yet supported");
	}
}

unique_ptr<LogicalOperator> LogicalDistinct::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto distinct_targets = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	return make_unique<LogicalDistinct>(std::move(distinct_targets));
}

} // namespace duckdb
