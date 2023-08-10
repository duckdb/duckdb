//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_parameter_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/bound_parameter_map.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

struct BoundParameterData {
public:
	BoundParameterData() {
	}
	explicit BoundParameterData(Value val) : value(std::move(val)), return_type(value.type()) {
	}

private:
	Value value;

public:
	LogicalType return_type;

public:
	void SetValue(Value val) {
		value = std::move(val);
	}

	const Value &GetValue() const {
		return value;
	}

	void Serialize(Serializer &serializer) const {
		FieldWriter writer(serializer);
		value.Serialize(writer.GetSerializer());
		writer.WriteSerializable(return_type);
		writer.Finalize();
	}

	static shared_ptr<BoundParameterData> Deserialize(Deserializer &source) {
		FieldReader reader(source);
		auto value = Value::Deserialize(reader.GetSource());
		auto result = make_shared<BoundParameterData>(std::move(value));
		result->return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
		reader.Finalize();
		return result;
	}

	void FormatSerialize(FormatSerializer &serializer) const;
	static shared_ptr<BoundParameterData> FormatDeserialize(FormatDeserializer &deserializer);
};

struct BoundParameterMap {
	explicit BoundParameterMap(case_insensitive_map_t<BoundParameterData> &parameter_data)
	    : parameter_data(parameter_data) {
	}

	bound_parameter_map_t parameters;
	case_insensitive_map_t<BoundParameterData> &parameter_data;

	LogicalType GetReturnType(const string &identifier) {
		auto it = parameter_data.find(identifier);
		if (it == parameter_data.end()) {
			return LogicalTypeId::UNKNOWN;
		}
		return it->second.return_type;
	}
};

} // namespace duckdb
