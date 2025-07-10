#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

enum class VariantValueType : uint8_t { PRIMITIVE, OBJECT, ARRAY, INVALID };

struct VariantValue {
public:
	VariantValue() : value_type(VariantValueType::INVALID) {
	}
	explicit VariantValue(VariantValueType type) : value_type(type) {
	}
	explicit VariantValue(Value &&val, LogicalTypeId type_id = LogicalTypeId::ANY)
	    : value_type(VariantValueType::PRIMITIVE), primitive_value(std::move(val)), primitive_value_type(type_id) {
		if (type_id == LogicalTypeId::ANY) {
			primitive_value_type = primitive_value.type().id();
		}
	}
	// Delete copy constructor and copy assignment operator
	VariantValue(const VariantValue &) = delete;
	VariantValue &operator=(const VariantValue &) = delete;

	// Default move constructor and move assignment operator
	VariantValue(VariantValue &&) noexcept = default;
	VariantValue &operator=(VariantValue &&) noexcept = default;

public:
	bool IsNull() const {
		return value_type == VariantValueType::PRIMITIVE && primitive_value.IsNull();
	}

public:
	void AddChild(const string &key, VariantValue &&val);
	void AddItem(VariantValue &&val);

public:
	yyjson_mut_val *ToJSON(ClientContext &context, yyjson_mut_doc *doc) const;

public:
	VariantValueType value_type;
	//! FIXME: how can we get a deterministic child order for a partially shredded object?
	map<string, VariantValue> object_children;
	vector<VariantValue> array_items;
	Value primitive_value;
	LogicalTypeId primitive_value_type;
};

} // namespace duckdb
