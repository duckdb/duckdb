//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/field_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"

namespace duckdb {
class BufferedSerializer;

class FieldWriter {
public:
	FieldWriter(Serializer &serializer);
	~FieldWriter();

public:
	template <class T>
	void WriteField(const T &element) {
		AddField();
		WriteData((const_data_ptr_t)&element, sizeof(T));
	}

	template <class T>
	void WriteList(const vector<T> &elements) {
		AddField();
		Write<uint32_t>(elements.size());
		for(idx_t i = 0; i < elements.size(); i++) {
			Write<T>(elements[i]);
		}
	}

	template <class T>
	void WriteSerializable(const T &element) {
		AddField();
		element.Serialize(*buffer);
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
	void Finalize();

private:
	void AddField() {
		field_count++;
	}

	template <class T>
	void Write(const T &element) {
		WriteData((const_data_ptr_t)&element, sizeof(T));
	}

	void WriteData(const_data_ptr_t buffer, idx_t write_size);

private:
	Serializer &serializer;
	unique_ptr<BufferedSerializer> buffer;
	idx_t field_count;
};

template <>
void FieldWriter::Write(const string &element);

class FieldReader {
public:
	FieldReader(Deserializer &source);
	~FieldReader();

public:
	template<class T>
	T ReadRequired() {
		if (field_count >= max_field_count) {
			// field is not there, throw an exception
			throw SerializationException("Attempting to read a required field, but field is missing");
		}
		// field is there, read the actual value
		AddField();
		return source.Read<T>();
	}

	template<class T>
	T ReadField(T default_value) {
		if (field_count >= max_field_count) {
			// field is not there, read the default value
			return default_value;
		}
		// field is there, read the actual value
		AddField();
		return source.Read<T>();
	}

	template <class T>
	vector<T> ReadRequiredList() {
		if (field_count >= max_field_count) {
			// field is not there, throw an exception
			throw SerializationException("Attempting to read a required field, but field is missing");
		}
		AddField();
		auto result_count = source.Read<uint32_t>();
		vector<T> result;
		for(idx_t i = 0; i < result_count; i++) {
			result.push_back(source.Read<T>());
		}
		return result;
	}

	template <class T, class RETURN_TYPE = T>
	unique_ptr<RETURN_TYPE> ReadOptional(unique_ptr<RETURN_TYPE> default_value) {
		if (field_count >= max_field_count) {
			// field is not there, read the default value
			return default_value;
		}
		// field is there, read the actual value
		AddField();
		return source.template ReadOptional<T, RETURN_TYPE>();
	}

	template<class T, class RETURN_TYPE = T>
	unique_ptr<RETURN_TYPE> ReadSerializable(unique_ptr<RETURN_TYPE> default_value) {
		if (field_count >= max_field_count) {
			// field is not there, read the default value
			return default_value;
		}
		// field is there, read the actual value
		AddField();
		return T::Deserialize(source);
	}

	template<class T, class RETURN_TYPE = T>
	unique_ptr<RETURN_TYPE> ReadSerializableMandatory() {
		if (field_count >= max_field_count) {
			// field is not there, read the default value
			throw SerializationException("Attempting to read mandatory field, but field is missing");
		}
		// field is there, read the actual value
		AddField();
		return T::Deserialize(source);
	}

	//! Called after all fields have been read. Should always be called.
	void Finalize();

private:
	void AddField() {
		field_count++;
	}

private:
	Deserializer &source;
	idx_t field_count;
	idx_t max_field_count;
	idx_t total_size;
};

} // namespace duckdb
