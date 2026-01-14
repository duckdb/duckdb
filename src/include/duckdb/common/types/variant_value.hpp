#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb_yyjson {
struct yyjson_mut_doc;
struct yyjson_mut_val;
} // namespace duckdb_yyjson

namespace duckdb {

enum class VariantValueType : uint8_t { PRIMITIVE, OBJECT, ARRAY, MISSING };

struct VariantValueIntermediate {
public:
	VariantValueIntermediate() : value_type(VariantValueType::MISSING) {
	}
	explicit VariantValueIntermediate(VariantValueType type) : value_type(type) {
	}
	explicit VariantValueIntermediate(Value &&val)
	    : value_type(VariantValueType::PRIMITIVE), primitive_value(std::move(val)) {
	}
	// Delete copy constructor and copy assignment operator
	VariantValueIntermediate(const VariantValueIntermediate &) = delete;
	VariantValueIntermediate &operator=(const VariantValueIntermediate &) = delete;

	// Default move constructor and move assignment operator
	VariantValueIntermediate(VariantValueIntermediate &&) noexcept = default;
	VariantValueIntermediate &operator=(VariantValueIntermediate &&) noexcept = default;

public:
	bool IsNull() const {
		return value_type == VariantValueType::PRIMITIVE && primitive_value.IsNull();
	}
	bool IsMissing() const {
		return value_type == VariantValueType::MISSING;
	}

public:
	void AddChild(const string &key, VariantValueIntermediate &&val);
	void AddItem(VariantValueIntermediate &&val);

public:
	duckdb_yyjson::yyjson_mut_val *ToJSON(ClientContext &context, duckdb_yyjson::yyjson_mut_doc *doc) const;
	static void ToVARIANT(vector<VariantValueIntermediate> &input, Vector &result);

public:
	VariantValueType value_type;
	//! FIXME: how can we get a deterministic child order for a partially shredded object?
	map<string, VariantValueIntermediate> object_children;
	vector<VariantValueIntermediate> array_items;
	Value primitive_value;
};

} // namespace duckdb
