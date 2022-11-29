#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {
unique_ptr<LogicalExtensionOperator> LogicalExtensionOperator::Deserialize(LogicalDeserializationState &state,
                                                                           FieldReader &reader) {
	auto &config = DBConfig::GetConfig(state.gstate.context);

	auto extension_name = reader.ReadRequired<std::string>();
	for (auto &extension : config.operator_extensions) {
		if (extension->GetName() == extension_name) {
			return extension->Deserialize(state, reader);
		}
	}

	throw SerializationException("No serialization method exists for extension: " + extension_name);
}
} // namespace duckdb
