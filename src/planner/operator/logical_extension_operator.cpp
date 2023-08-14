#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

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

void LogicalExtensionOperator::FormatSerialize(FormatSerializer &serializer) const {
	LogicalOperator::FormatSerialize(serializer);
	serializer.WriteProperty(200, "extension_name", GetExtensionName());
}

unique_ptr<LogicalOperator> LogicalExtensionOperator::FormatDeserialize(FormatDeserializer &deserializer) {
	auto &config = DBConfig::GetConfig(deserializer.Get<ClientContext &>());
	auto extension_name = deserializer.ReadProperty<string>(200, "extension_name");
	for (auto &extension : config.operator_extensions) {
		if (extension->GetName() == extension_name) {
			return extension->FormatDeserialize(deserializer);
		}
	}
	throw SerializationException("No deserialization method exists for extension: " + extension_name);
}

string LogicalExtensionOperator::GetExtensionName() const {
	throw SerializationException("LogicalExtensionOperator::GetExtensionName not implemented which is required for "
	                             "serializing extension operators");
}

} // namespace duckdb
