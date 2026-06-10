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

	static VariantValue NullValue() {
		return VariantValue(Value());
	}

public:
	void AddChild(const string &key, VariantValue &&val);
	void AddItem(VariantValue &&val);

	void SetItems(vector<VariantValue> &&values);
	void ReserveItems(idx_t count);
	void AddItems(vector<VariantValue>::iterator begin, vector<VariantValue>::iterator end);
	map<string, VariantValue> TakeObjectChildren();
	const map<string, VariantValue> &ObjectChildren() const;
	const vector<VariantValue> &ArrayItems() const;

public:
	duckdb_yyjson::yyjson_mut_val *ToJSON(ClientContext &context, duckdb_yyjson::yyjson_mut_doc *doc) const;
	static void ToVARIANT(vector<VariantValue> &input, Vector &result);

public:
	VariantValueType value_type;
	Value primitive_value;

private:
	//! FIXME: how can we get a deterministic child order for a partially shredded object?
	map<string, VariantValue> object_children;
	vector<VariantValue> array_items;
};

} // namespace duckdb
