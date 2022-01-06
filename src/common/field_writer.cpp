#include "duckdb/common/field_writer.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Field Writer
//===--------------------------------------------------------------------===//
FieldWriter::FieldWriter(Serializer &serializer_p) :
      serializer(serializer_p), buffer(make_unique<BufferedSerializer>()), field_count(0) {
}

FieldWriter::~FieldWriter() {
	if (Exception::UncaughtException()) {
		return;
	}
	// finalize should always have been called, unless this is destroyed as part of stack unwinding
	D_ASSERT(!buffer);
}

void FieldWriter::WriteData(const_data_ptr_t buffer_ptr, idx_t write_size) {
	D_ASSERT(buffer);
	buffer->WriteData(buffer_ptr, write_size);
}

template <>
void FieldWriter::Write(const string &val) {
	AddField();
	Write<uint32_t>((uint32_t)val.size());
	if (!val.empty()) {
		WriteData((const_data_ptr_t)val.c_str(), val.size());
	}
}

void FieldWriter::Finalize() {
	D_ASSERT(buffer);

	serializer.Write<uint32_t>(field_count);
	serializer.Write<uint64_t>(buffer->blob.size);
	serializer.WriteData(buffer->blob.data.get(), buffer->blob.size);

	buffer.reset();
}

//===--------------------------------------------------------------------===//
// Field Reader
//===--------------------------------------------------------------------===//
FieldReader::FieldReader(Deserializer &source_p) : source(source_p), field_count(0) {
	max_field_count = source.Read<uint32_t>();
	total_size = source.Read<uint64_t>();
}

FieldReader::~FieldReader() {}

void FieldReader::Finalize() {
	if (field_count < max_field_count) {
		throw SerializationException("Not all fields were read. This file might have been written with a newer version of DuckDB and is incompatible with this version of DuckDB.");
	}
}

}
