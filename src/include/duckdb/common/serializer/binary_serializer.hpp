#pragma once
#include "duckdb/common/serializer/format_serializer.hpp"

namespace duckdb {
class BinarySerializer : public FormatSerializer {

public:
	explicit BinarySerializer(unique_ptr<BufferedSerializer> serializer) {
		stack.emplace_back(std::move(serializer));
	}
	vector<const char*> trace;


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


	void WriteTag(const char* tag) override;

	void BeginWriteList(idx_t count) override;
	void BeginWriteMap(idx_t count) override;
	void BeginWriteObject() override;
	void EndWriteObject() override;

	void WriteNull() override;
	void WriteValue(uint8_t value) override;
	void WriteValue(int8_t value) override;
	void WriteValue(const string &value) override;
	void WriteValue(const string_t value) override;
	void WriteValue(const char *value) override;
	void WriteValue(uint64_t value) override;
	void WriteValue(uint16_t value) override;
	void WriteValue(int16_t value) override;
	void WriteValue(uint32_t value) override;
	void WriteValue(int32_t value) override;
	void WriteValue(int64_t value) override;
	void WriteValue(hugeint_t value) override;
	void WriteValue(float value) override;
	void WriteValue(double value) override;
	void WriteValue(interval_t value) override;
	void WriteValue(bool value) override;
public:
	BinaryData GetData();
};

} // namespace duckdb