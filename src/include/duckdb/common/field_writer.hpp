//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/field_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include <type_traits>

namespace duckdb {
class BufferedSerializer;

struct IndexWriteOperation {
	template <class SRC, class DST>
	static DST Operation(SRC input) {
		return input.index;
	}
};

class FieldWriter {
public:
	DUCKDB_API FieldWriter(Serializer &serializer);
	DUCKDB_API ~FieldWriter();

public:
	template <class T>
	void WriteField(const T &element) {
		static_assert(std::is_trivially_destructible<T>(), "WriteField object must be trivially destructible");

		AddField();
		WriteData((const_data_ptr_t)&element, sizeof(T));
	}

	//! Write a string with a length prefix
	void WriteString(const string &val) {
		WriteStringLen((const_data_ptr_t)val.c_str(), val.size());
	}
	void WriteStringLen(const_data_ptr_t val, idx_t len) {
		AddField();
		Write<uint32_t>((uint32_t)len);
		if (len > 0) {
			WriteData(val, len);
		}
	}
	void WriteBlob(const_data_ptr_t val, idx_t len) {
		AddField();
		if (len > 0) {
			WriteData(val, len);
		}
	}

	template <class T, class CONTAINER_TYPE = vector<T>>
	void WriteList(const CONTAINER_TYPE &elements) {
		AddField();
		Write<uint32_t>(elements.size());
		for (auto &element : elements) {
			Write<T>(element);
		}
	}

	template <class T, class SRC, class OP, class CONTAINER_TYPE = vector<SRC>>
	void WriteGenericList(const CONTAINER_TYPE &elements) {
		AddField();
		Write<uint32_t>(elements.size());
		for (auto &element : elements) {
			Write<T>(OP::template Operation<SRC, T>(element));
		}
	}

	template <class T>
	void WriteIndexList(const vector<T> &elements) {
		WriteGenericList<idx_t, T, IndexWriteOperation>(elements);
	}

	// vector<bool> yay
	template <class T, class CONTAINER_TYPE = vector<T>>
	void WriteListNoReference(const CONTAINER_TYPE &elements) {
		AddField();
		Write<uint32_t>(elements.size());
		for (auto element : elements) {
			Write<T>(element);
		}
	}

	template <class T>
	void WriteSerializable(const T &element) {
		AddField();
		element.Serialize(*buffer);
	}

	template <class T>
	void WriteSerializableList(const vector<unique_ptr<T>> &elements) {
		AddField();
		Write<uint32_t>(elements.size());
		for (idx_t i = 0; i < elements.size(); i++) {
			elements[i]->Serialize(*buffer);
		}
	}

	template <class T>
	void WriteRegularSerializableList(const vector<T> &elements) {
		AddField();
		Write<uint32_t>(elements.size());
		for (idx_t i = 0; i < elements.size(); i++) {
			elements[i].Serialize(*buffer);
		}
	}

	template <class T>
	void WriteOptional(const unique_ptr<T> &element) {
		AddField();
		Write<bool>(element ? true : false);
		if (element) {
			element->Serialize(*buffer);
		}
	}

	// Called after all fields have been written. Should always be called.
	DUCKDB_API void Finalize();

	Serializer &GetSerializer() {
		return *buffer;
	}

private:
	void AddField() {
		field_count++;
	}

	template <class T>
	void Write(const T &element) {
		WriteData((const_data_ptr_t)&element, sizeof(T));
	}

	DUCKDB_API void WriteData(const_data_ptr_t buffer, idx_t write_size);

private:
	Serializer &serializer;
	unique_ptr<BufferedSerializer> buffer;
	idx_t field_count;
	bool finalized;
};

template <>
DUCKDB_API void FieldWriter::Write(const string &val);

class FieldDeserializer : public Deserializer {
public:
	FieldDeserializer(Deserializer &root);

public:
	void ReadData(data_ptr_t buffer, idx_t read_size) override;

	void SetRemainingData(idx_t remaining_data);
	idx_t RemainingData();
	Deserializer &GetRoot() {
		return root;
	}

private:
	Deserializer &root;
	idx_t remaining_data;
};

struct IndexReadOperation {
	template <class SRC, class DST>
	static DST Operation(SRC input) {
		return DST(input);
	}
};

class FieldReader {
public:
	DUCKDB_API FieldReader(Deserializer &source);
	DUCKDB_API ~FieldReader();

public:
	template <class T>
	T ReadRequired() {
		if (field_count >= max_field_count) {
			// field is not there, throw an exception
			throw SerializationException("Attempting to read a required field, but field is missing");
		}
		// field is there, read the actual value
		AddField();
		return source.Read<T>();
	}

	template <class T>
	T ReadField(T default_value) {
		if (field_count >= max_field_count) {
			// field is not there, read the default value
			return default_value;
		}
		// field is there, read the actual value
		AddField();
		return source.Read<T>();
	}

	template <class T, class CONTAINER_TYPE = vector<T>>
	bool ReadList(CONTAINER_TYPE &result) {
		if (field_count >= max_field_count) {
			// field is not there, return false and leave the result empty
			return false;
		}
		AddField();
		auto result_count = source.Read<uint32_t>();
		result.reserve(result_count);
		for (idx_t i = 0; i < result_count; i++) {
			result.push_back(source.Read<T>());
		}
		return true;
	}

	template <class T, class CONTAINER_TYPE = vector<T>>
	CONTAINER_TYPE ReadRequiredList() {
		if (field_count >= max_field_count) {
			// field is not there, throw an exception
			throw SerializationException("Attempting to read a required field, but field is missing");
		}
		AddField();
		auto result_count = source.Read<uint32_t>();
		CONTAINER_TYPE result;
		result.reserve(result_count);
		for (idx_t i = 0; i < result_count; i++) {
			result.push_back(source.Read<T>());
		}
		return result;
	}

	template <class T, class SRC, class OP>
	vector<T> ReadRequiredGenericList() {
		if (field_count >= max_field_count) {
			// field is not there, throw an exception
			throw SerializationException("Attempting to read a required field, but field is missing");
		}
		AddField();
		auto result_count = source.Read<uint32_t>();
		vector<T> result;
		result.reserve(result_count);
		for (idx_t i = 0; i < result_count; i++) {
			result.push_back(OP::template Operation<SRC, T>(source.Read<SRC>()));
		}
		return result;
	}

	template <class T>
	vector<T> ReadRequiredIndexList() {
		return ReadRequiredGenericList<T, idx_t, IndexReadOperation>();
	}

	template <class T>
	set<T> ReadRequiredSet() {
		if (field_count >= max_field_count) {
			// field is not there, throw an exception
			throw SerializationException("Attempting to read a required field, but field is missing");
		}
		AddField();
		auto result_count = source.Read<uint32_t>();
		set<T> result;
		for (idx_t i = 0; i < result_count; i++) {
			result.insert(source.Read<T>());
		}
		return result;
	}

	template <class T, typename... ARGS>
	unique_ptr<T> ReadOptional(unique_ptr<T> default_value, ARGS &&... args) {
		if (field_count >= max_field_count) {
			// field is not there, read the default value
			return default_value;
		}
		// field is there, read the actual value
		AddField();
		return source.template ReadOptional<T>(std::forward<ARGS>(args)...);
	}

	template <class T, class RETURN_TYPE = unique_ptr<T>>
	RETURN_TYPE ReadSerializable(RETURN_TYPE default_value) {
		if (field_count >= max_field_count) {
			// field is not there, read the default value
			return default_value;
		}
		// field is there, read the actual value
		AddField();
		return T::Deserialize(source);
	}

	template <class T, class RETURN_TYPE = unique_ptr<T>, typename... ARGS>
	RETURN_TYPE ReadSerializable(RETURN_TYPE default_value, ARGS &&... args) {
		if (field_count >= max_field_count) {
			// field is not there, read the default value
			return default_value;
		}
		// field is there, read the actual value
		AddField();
		return T::Deserialize(source, std::forward<ARGS>(args)...);
	}

	template <class T, class RETURN_TYPE = unique_ptr<T>>
	RETURN_TYPE ReadRequiredSerializable() {
		if (field_count >= max_field_count) {
			// field is not there, throw an exception
			throw SerializationException("Attempting to read mandatory field, but field is missing");
		}
		// field is there, read the actual value
		AddField();
		return T::Deserialize(source);
	}

	template <class T, class RETURN_TYPE = unique_ptr<T>, typename... ARGS>
	RETURN_TYPE ReadRequiredSerializable(ARGS &&... args) {
		if (field_count >= max_field_count) {
			// field is not there, throw an exception
			throw SerializationException("Attempting to read mandatory field, but field is missing");
		}
		// field is there, read the actual value
		AddField();
		return T::Deserialize(source, std::forward<ARGS>(args)...);
	}

	template <class T, class RETURN_TYPE = unique_ptr<T>, typename... ARGS>
	vector<RETURN_TYPE> ReadRequiredSerializableList(ARGS &&... args) {
		if (field_count >= max_field_count) {
			// field is not there, throw an exception
			throw SerializationException("Attempting to read mandatory field, but field is missing");
		}
		// field is there, read the actual value
		AddField();
		auto result_count = source.Read<uint32_t>();

		vector<RETURN_TYPE> result;
		for (idx_t i = 0; i < result_count; i++) {
			result.push_back(T::Deserialize(source, std::forward<ARGS>(args)...));
		}
		return result;
	}

	void ReadBlob(data_ptr_t result, idx_t read_size) {
		if (field_count >= max_field_count) {
			// field is not there, throw an exception
			throw SerializationException("Attempting to read a required field, but field is missing");
		}
		// field is there, read the actual value
		AddField();
		source.ReadData(result, read_size);
	}

	//! Called after all fields have been read. Should always be called.
	DUCKDB_API void Finalize();

	Deserializer &GetSource() {
		return source;
	}

private:
	void AddField() {
		field_count++;
	}

private:
	FieldDeserializer source;
	idx_t field_count;
	idx_t max_field_count;
	idx_t total_size;
	bool finalized;
};

} // namespace duckdb
