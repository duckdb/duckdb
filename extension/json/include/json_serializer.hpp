#pragma once

#include "json_common.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

struct JsonSerializer : Serializer {
private:
	yyjson_mut_doc *doc;
	yyjson_mut_val *current_tag;
	vector<yyjson_mut_val *> stack;

	// Skip writing property if null
	bool skip_if_null = false;
	// Skip writing property if empty string, empty list or empty map.
	bool skip_if_empty = false;

	// Get the current json value
	inline yyjson_mut_val *Current() {
		return stack.back();
	};

	// Either adds a value to the current object with the current tag, or appends it to the current array
	void PushValue(yyjson_mut_val *val);

	explicit JsonSerializer(yyjson_mut_doc *doc, bool skip_if_null, bool skip_if_empty)
	    : doc(doc), stack({yyjson_mut_obj(doc)}), skip_if_null(skip_if_null), skip_if_empty(skip_if_empty) {
		serialize_enum_as_string = true;
		serialize_default_values = true;
	}

public:
	template <class T>
	static yyjson_mut_val *Serialize(T &value, yyjson_mut_doc *doc, bool skip_if_null, bool skip_if_empty) {
		JsonSerializer serializer(doc, skip_if_null, skip_if_empty);
		value.Serialize(serializer);
		return serializer.GetRootObject();
	}

	yyjson_mut_val *GetRootObject() {
		D_ASSERT(stack.size() == 1); // or we forgot to pop somewhere
		return stack.front();
	};

	//===--------------------------------------------------------------------===//
	// Nested Types Hooks
	//===--------------------------------------------------------------------===//
	void OnPropertyBegin(const field_id_t field_id, const char *tag) final;
	void OnPropertyEnd() final;
	void OnOptionalPropertyBegin(const field_id_t field_id, const char *tag, bool present) final;
	void OnOptionalPropertyEnd(bool present) final;

	void OnListBegin(idx_t count) final;
	void OnListEnd() final;
	void OnObjectBegin() final;
	void OnObjectEnd() final;
	void OnNullableBegin(bool present) final;
	void OnNullableEnd() final;

	//===--------------------------------------------------------------------===//
	// Primitive Types
	//===--------------------------------------------------------------------===//
	void WriteNull() final;
	void WriteValue(uint8_t value) final;
	void WriteValue(int8_t value) final;
	void WriteValue(uint16_t value) final;
	void WriteValue(int16_t value) final;
	void WriteValue(uint32_t value) final;
	void WriteValue(int32_t value) final;
	void WriteValue(uint64_t value) final;
	void WriteValue(int64_t value) final;
	void WriteValue(hugeint_t value) final;
	void WriteValue(float value) final;
	void WriteValue(double value) final;
	void WriteValue(const string_t value) final;
	void WriteValue(const string &value) final;
	void WriteValue(const char *value) final;
	void WriteValue(bool value) final;
	void WriteDataPtr(const_data_ptr_t ptr, idx_t count) final;
};

} // namespace duckdb
