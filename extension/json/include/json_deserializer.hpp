#pragma once
#include "json_common.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

class JsonDeserializer : public FormatDeserializer {
public:
	JsonDeserializer(const char* json, idx_t len) {
		doc = yyjson_read(json, len, 0);
		stack = {yyjson_doc_get_root(doc) };
	}
	~JsonDeserializer() {
		yyjson_doc_free(doc);
	}
private:
	yyjson_doc *doc;
	const char* current_tag = nullptr;
	vector<yyjson_val *> stack;
	yyjson_arr_iter* arr_iter;

	// Get the current json value
	inline yyjson_val *Current() { return stack.back(); };

	// Set the 'tag' of the property to read
	void SetTag(const char* tag) final;

	yyjson_val* GetNextValue();

	void ThrowTypeError(yyjson_val* val, const char* expected);

	//===--------------------------------------------------------------------===//
	// Nested Types Hooks
	//===--------------------------------------------------------------------===//
	idx_t BeginReadList() final;
	void EndReadList() final;

	//===--------------------------------------------------------------------===//
	// Primitive Types
	//===--------------------------------------------------------------------===//
	bool ReadBool() final;
	int32_t ReadSignedInt32() final;
	uint32_t ReadUnsignedInt32() final;
	float ReadFloat() final;
	double ReadDouble() final;
	string ReadString() final;
};

} // namespace duckdb