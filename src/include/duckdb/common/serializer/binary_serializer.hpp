#pragma once
#include "duckdb/common/serializer/format_serializer.hpp"

namespace duckdb {
class BinarySerializer : public FormatSerializer {
public:
	explicit BinarySerializer(unique_ptr<BufferedSerializer> serializer) {
		stack.emplace_back(std::move(serializer));
	}
	vector<const char*> trace;

	BinaryData GetData();

protected:
	struct BinarySerializerState {
		uint32_t field_count;
		unique_ptr<BufferedSerializer> writer;

		void AddField() {
			field_count++;
		}

		template<class T>
		void Write(T value) {
			writer->Write<T>(value);
		}

		explicit BinarySerializerState(unique_ptr<BufferedSerializer> serializer) : writer(std::move(serializer)) {}
		BinarySerializerState() : field_count(0), writer(make_unique<BufferedSerializer>()) {}
 	};

	vector<BinarySerializerState> stack;
	BinarySerializerState& GetCurrent() { return stack.back(); };


	void WriteTag(const char* tag) final;

	//===--------------------------------------------------------------------===//
	// Nested Types Hooks
	//===--------------------------------------------------------------------===//
	void BeginWriteOptional(bool present) final;
	void BeginWriteList(idx_t count) final;
	void EndWriteList(idx_t count) final;
	void BeginWriteMap(idx_t count) final;
	void EndWriteMap(idx_t count) final;
	void BeginWriteObject() final;
	void EndWriteObject() final;

	//===--------------------------------------------------------------------===//
	// Primitive Types
	//===--------------------------------------------------------------------===//
	void WriteNull() final;
	void WriteValue(uint8_t value) final;
	void WriteValue(int8_t value) final;
	void WriteValue(const string &value) final;
	void WriteValue(const string_t value) final;
	void WriteValue(const char *value) final;
	void WriteValue(uint64_t value) final;
	void WriteValue(uint16_t value) final;
	void WriteValue(int16_t value) final;
	void WriteValue(uint32_t value) final;
	void WriteValue(int32_t value) final;
	void WriteValue(int64_t value) final;
	void WriteValue(hugeint_t value) final;
	void WriteValue(float value) final;
	void WriteValue(double value) final;
	void WriteValue(interval_t value) final;
	void WriteValue(bool value) final;
};

} // namespace duckdb