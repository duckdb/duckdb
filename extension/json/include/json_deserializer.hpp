#pragma once
#include "json_common.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

class JsonDeserializer : public FormatDeserializer {
public:
	JsonDeserializer(const char *json, idx_t len) {
		doc = yyjson_read(json, len, 0);
		stack = {yyjson_doc_get_root(doc)};
	}
	JsonDeserializer(yyjson_doc *doc) : doc(doc) {
		stack = {yyjson_doc_get_root(doc)};
	}
	JsonDeserializer(yyjson_val *val, yyjson_doc *doc) : doc(doc) {
		stack = {val};
	}
	~JsonDeserializer() {
		yyjson_doc_free(doc);
	}

private:
	yyjson_doc *doc;
	const char *current_tag = nullptr;
	vector<yyjson_val *> stack;
	yyjson_arr_iter arr_iter;

	void DumpDoc();
	void DumpCurrent();

	// Get the current json value
	inline yyjson_val *Current() {
		return stack.back();
	};
	yyjson_val *GetNextValue();

	void ThrowTypeError(yyjson_val *val, const char *expected);

	// Set the 'tag' of the property to read
	void SetTag(const char *tag) final;

	//===--------------------------------------------------------------------===//
	// Nested Types Hooks
	//===--------------------------------------------------------------------===//
	void OnObjectBegin() final;
	void OnObjectEnd() final;
	idx_t OnListBegin() final;
	void OnListEnd() final;
	idx_t OnMapBegin() final;
	void OnMapEnd() final;
	void OnMapEntryBegin() final;
	void OnMapEntryEnd() final;
	void OnMapKeyBegin() final;
	void OnMapValueBegin() final;
	bool OnOptionalBegin() final;

	//===--------------------------------------------------------------------===//
	// Primitive Types
	//===--------------------------------------------------------------------===//
	bool ReadBool() final;
	int8_t ReadSignedInt8() final;
	uint8_t ReadUnsignedInt8() final;
	int16_t ReadSignedInt16() final;
	uint16_t ReadUnsignedInt16() final;
	int32_t ReadSignedInt32() final;
	uint32_t ReadUnsignedInt32() final;
	int64_t ReadSignedInt64() final;
	uint64_t ReadUnsignedInt64() final;
	float ReadFloat() final;
	double ReadDouble() final;
	string ReadString() final;
	interval_t ReadInterval() final;
};

} // namespace duckdb
