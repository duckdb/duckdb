#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

enum class VariantValueType : uint8_t { PRIMITIVE, OBJECT, ARRAY, MISSING };

struct VariantValue {
public:
	VariantValue() : value_type(VariantValueType::MISSING) {
	}
	explicit VariantValue(VariantValueType type) : value_type(type) {
	}
	explicit VariantValue(Value &&val) : value_type(VariantValueType::PRIMITIVE), primitive_value(std::move(val)) {
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
	bool IsMissing() const {
		return value_type == VariantValueType::MISSING;
	}

public:
	void AddChild(const string &key, VariantValue &&val);
	void AddItem(VariantValue &&val);

public:
	yyjson_mut_val *ToJSON(ClientContext &context, yyjson_mut_doc *doc) const;
	static void ToVARIANT(vector<VariantValue> &input, Vector &result);

public:
	VariantValueType value_type;
	//! FIXME: how can we get a deterministic child order for a partially shredded object?
	map<string, VariantValue> object_children;
	vector<VariantValue> array_items;
	Value primitive_value;
};

} // namespace duckdb
