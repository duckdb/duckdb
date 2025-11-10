//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_parameter_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

struct BoundParameterData {
public:
	BoundParameterData() {
	}
	explicit BoundParameterData(Value val) : value(std::move(val)), return_type(GetDefaultType(value.type())) {
	}
	BoundParameterData(Value val, LogicalType type_p) : value(std::move(val)), return_type(std::move(type_p)) {
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

	void Serialize(Serializer &serializer) const;
	static shared_ptr<BoundParameterData> Deserialize(Deserializer &deserializer);

private:
	LogicalType GetDefaultType(const LogicalType &type) {
		if (value.type().id() == LogicalTypeId::VARCHAR && StringType::GetCollation(type).empty()) {
			return LogicalTypeId::STRING_LITERAL;
		}
		return value.type();
	}
};

} // namespace duckdb
