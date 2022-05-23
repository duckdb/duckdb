#include "duckdb/common/field_writer.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Field Writer
//===--------------------------------------------------------------------===//
FieldWriter::FieldWriter(Serializer &serializer_p)
    : serializer(serializer_p), buffer(make_unique<BufferedSerializer>()), field_count(0), finalized(false) {
}

FieldWriter::~FieldWriter() {
	if (Exception::UncaughtException()) {
		return;
	}
	D_ASSERT(finalized);
	// finalize should always have been called, unless this is destroyed as part of stack unwinding
	D_ASSERT(!buffer);
}

void FieldWriter::WriteData(const_data_ptr_t buffer_ptr, idx_t write_size) {
	D_ASSERT(buffer);
	buffer->WriteData(buffer_ptr, write_size);
}

template <>
void FieldWriter::Write(const string &val) {
	Write<uint32_t>((uint32_t)val.size());
	if (!val.empty()) {
		WriteData((const_data_ptr_t)val.c_str(), val.size());
	}
}

void FieldWriter::Finalize() {
	D_ASSERT(buffer);
	D_ASSERT(!finalized);
	finalized = true;
	serializer.Write<uint32_t>(field_count);
	serializer.Write<uint64_t>(buffer->blob.size);
	serializer.WriteData(buffer->blob.data.get(), buffer->blob.size);

	buffer.reset();
}

//===--------------------------------------------------------------------===//
// Field Deserializer
//===--------------------------------------------------------------------===//
FieldDeserializer::FieldDeserializer(Deserializer &root) : root(root), remaining_data(idx_t(-1)) {
}

void FieldDeserializer::ReadData(data_ptr_t buffer, idx_t read_size) {
	D_ASSERT(remaining_data != idx_t(-1));
	D_ASSERT(read_size <= remaining_data);
	root.ReadData(buffer, read_size);
	remaining_data -= read_size;
}

idx_t FieldDeserializer::RemainingData() {
	return remaining_data;
}

void FieldDeserializer::SetRemainingData(idx_t remaining_data) {
	this->remaining_data = remaining_data;
}

//===--------------------------------------------------------------------===//
// Field Reader
//===--------------------------------------------------------------------===//
FieldReader::FieldReader(Deserializer &source_p) : source(source_p), field_count(0), finalized(false) {
	max_field_count = source_p.Read<uint32_t>();
	total_size = source_p.Read<uint64_t>();
	D_ASSERT(max_field_count > 0);
	source.SetRemainingData(total_size);
}

FieldReader::~FieldReader() {
	if (Exception::UncaughtException()) {
		return;
	}
	D_ASSERT(finalized);
}

void FieldReader::Finalize() {
	D_ASSERT(!finalized);
	finalized = true;
	if (field_count < max_field_count) {
		// we can handle this case by calling source.ReadData(buffer, source.RemainingData())
		throw SerializationException("Not all fields were read. This file might have been written with a newer version "
		                             "of DuckDB and is incompatible with this version of DuckDB.");
	}
	D_ASSERT(source.RemainingData() == 0);
}

} // namespace duckdb
