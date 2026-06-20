#include "duckdb/common/types/variant/variant_builder.hpp"
#include "duckdb/common/types/variant_value.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

void VariantValue::AddChild(const string &key, VariantValue &&val) {
	D_ASSERT(value_type == VariantValueType::OBJECT);
	if (val.IsMissing()) {
		return;
	}
	object_children.emplace(key, std::move(val));
}

void VariantValue::AddItem(VariantValue &&val) {
	D_ASSERT(value_type == VariantValueType::ARRAY);
	if (val.IsMissing()) {
		//! SPEC: If a Variant is missing in a context where a value is required, readers must return a Variant null
		val = VariantValue::NullValue();
	}
	array_items.push_back(std::move(val));
}

void VariantValue::SetItems(vector<VariantValue> &&values) {
	D_ASSERT(value_type == VariantValueType::ARRAY);
	for (auto &value : values) {
		if (value.IsMissing()) {
			//! SPEC: If a Variant is missing in a context where a value is required, readers must return a Variant null
			value = VariantValue::NullValue();
		}
	}
	array_items = std::move(values);
}

void VariantValue::ReserveItems(idx_t count) {
	array_items.reserve(count);
}

void VariantValue::AddItems(vector<VariantValue>::iterator begin, vector<VariantValue>::iterator end) {
	D_ASSERT(value_type == VariantValueType::ARRAY);
	for (; begin != end; begin++) {
		auto &value = *begin;
		if (value.IsMissing()) {
			//! SPEC: If a Variant is missing in a context where a value is required, readers must return a Variant null
			value = VariantValue::NullValue();
		}
		array_items.push_back(std::move(value));
	}
}

map<string, VariantValue> VariantValue::TakeObjectChildren() {
	D_ASSERT(value_type == VariantValueType::OBJECT);
	return std::move(object_children);
}

const map<string, VariantValue> &VariantValue::ObjectChildren() const {
	return object_children;
}

const vector<VariantValue> &VariantValue::ArrayItems() const {
	return array_items;
}

Value VariantValue::GetValue(const Value &variant_val) {
	D_ASSERT(variant_val.type().id() == LogicalTypeId::VARIANT && !variant_val.IsNull());
	Vector tmp(variant_val, count_t(1));
	RecursiveUnifiedVectorFormat format;
	Vector::RecursiveToUnifiedFormat(tmp, format);
	UnifiedVariantVectorData vector_data(format);
	return VariantUtils::ConvertVariantToValue(vector_data, 0, 0);
}

//===--------------------------------------------------------------------===//
// ToVARIANT sources
//===--------------------------------------------------------------------===//
// The single-pass build machinery (VariantBuilder / EmitIterator / BuildVariant) lives in
// variant_builder.hpp so it can be shared with the parquet reader. A "source" just implements
// 'bool Emit(idx_t row, VariantBuilder &builder)' (returning whether the row is a SQL NULL).
namespace {

struct VariantValueSource {
	explicit VariantValueSource(vector<VariantValue> &input) : input(input) {
	}
	//! Emit row 'row' into the builder, returning whether the row is a (SQL) NULL
	bool Emit(idx_t row, VariantBuilder &builder) const {
		auto &value = input[row];
		if (value.IsNull() || value.IsMissing()) {
			return true;
		}
		builder.EmitVariantValue(value);
		return false;
	}

	vector<VariantValue> &input;
};

struct VariantIteratorSource {
	explicit VariantIteratorSource(const VariantIterator &state) : state(state) {
	}
	bool Emit(idx_t row, VariantBuilder &builder) const {
		auto root = state.Root(row);
		//! Root() resolves a missing/absent root to a SQL NULL
		if (root.IsNull()) {
			return true;
		}
		EmitIterator(root, builder);
		return false;
	}

	const VariantIterator &state;
};

} // namespace

void VariantValue::ToVARIANT(vector<VariantValue> &input, Vector &result) {
	VariantValueSource source(input);
	BuildVariant(source, input.size(), result);
}

void VariantValue::ToVARIANT(const VariantIterator &state, idx_t count, Vector &result) {
	VariantIteratorSource source(state);
	BuildVariant(source, count, result);
}

yyjson_mut_val *VariantValue::ToJSON(ClientContext &context, yyjson_mut_doc *doc) const {
	switch (value_type) {
	case VariantValueType::PRIMITIVE: {
		if (primitive_value.IsNull()) {
			return yyjson_mut_null(doc);
		}
		switch (primitive_value.type().id()) {
		case LogicalTypeId::BOOLEAN: {
			if (primitive_value.GetValue<bool>()) {
				return yyjson_mut_true(doc);
			} else {
				return yyjson_mut_false(doc);
			}
		}
		case LogicalTypeId::TINYINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int8_t>());
		case LogicalTypeId::SMALLINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int16_t>());
		case LogicalTypeId::INTEGER:
			return yyjson_mut_int(doc, primitive_value.GetValue<int32_t>());
		case LogicalTypeId::BIGINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int64_t>());
		case LogicalTypeId::FLOAT:
			return yyjson_mut_real(doc, primitive_value.GetValue<float>());
		case LogicalTypeId::DOUBLE:
			return yyjson_mut_real(doc, primitive_value.GetValue<double>());
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::VARCHAR: {
			auto value_str = primitive_value.ToString();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP: {
			auto value_str = primitive_value.ToString();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			auto value_str = primitive_value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP_TZ_NS: {
			auto value_str = primitive_value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			auto value_str = primitive_value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		default:
			throw InternalException("Unexpected primitive type: %s", primitive_value.type().ToString());
		}
	}
	case VariantValueType::OBJECT: {
		auto obj = yyjson_mut_obj(doc);
		for (const auto &it : object_children) {
			auto &key = it.first;
			auto value = it.second.ToJSON(context, doc);
			yyjson_mut_obj_add_val(doc, obj, key.c_str(), value);
		}
		return obj;
	}
	case VariantValueType::ARRAY: {
		auto arr = yyjson_mut_arr(doc);
		for (auto &item : array_items) {
			auto value = item.ToJSON(context, doc);
			yyjson_mut_arr_add_val(arr, value);
		}
		return arr;
	}
	default:
		throw InternalException("Can't serialize this VariantValue type to JSON");
	}
}

} // namespace duckdb
