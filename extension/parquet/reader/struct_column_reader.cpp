#include "reader/struct_column_reader.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Struct Column Reader
//===--------------------------------------------------------------------===//
StructColumnReader::StructColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema,
                                       vector<unique_ptr<ColumnReader>> child_readers_p)
    : ColumnReader(reader, schema), child_readers(std::move(child_readers_p)) {
	D_ASSERT(Type().InternalType() == PhysicalType::STRUCT);
}

ColumnReader &StructColumnReader::GetChildReader(idx_t child_idx) {
	if (!child_readers[child_idx]) {
		throw InternalException("StructColumnReader::GetChildReader(%d) - but this child reader is not set", child_idx);
	}
	return *child_readers[child_idx].get();
}

void StructColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                        TProtocol &protocol_p) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->InitializeRead(row_group_idx_p, columns, protocol_p);
	}
}

idx_t StructColumnReader::Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) {
	auto &struct_entries = StructVector::GetEntries(result);
	D_ASSERT(StructType::GetChildTypes(Type()).size() == struct_entries.size());

	if (pending_skips > 0) {
		throw InternalException("StructColumnReader cannot have pending skips");
	}

	optional_idx read_count;
	for (idx_t i = 0; i < child_readers.size(); i++) {
		auto &child = child_readers[i];
		auto &target_vector = *struct_entries[i];
		if (!child) {
			// if we are not scanning this vector - set it to NULL
			target_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(target_vector, true);
			continue;
		}
		auto child_num_values = child->Read(num_values, define_out, repeat_out, target_vector);
		if (!read_count.IsValid()) {
			read_count = child_num_values;
		} else if (read_count.GetIndex() != child_num_values) {
			throw std::runtime_error("Struct child row count mismatch");
		}
	}
	if (!read_count.IsValid()) {
		read_count = num_values;
	}
	// set the validity mask for this level
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < read_count.GetIndex(); i++) {
		if (define_out[i] < MaxDefine()) {
			validity.SetInvalid(i);
		}
	}

	return read_count.GetIndex();
}

void StructColumnReader::Skip(idx_t num_values) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->Skip(num_values);
	}
}

void StructColumnReader::RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->RegisterPrefetch(transport, allow_merge);
	}
}

uint64_t StructColumnReader::TotalCompressedSize() {
	uint64_t size = 0;
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		size += child->TotalCompressedSize();
	}
	return size;
}

static bool TypeHasExactRowCount(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return false;
	case LogicalTypeId::STRUCT:
		for (auto &kv : StructType::GetChildTypes(type)) {
			if (TypeHasExactRowCount(kv.second)) {
				return true;
			}
		}
		return false;
	default:
		return true;
	}
}

idx_t StructColumnReader::GroupRowsAvailable() {
	for (idx_t i = 0; i < child_readers.size(); i++) {
		if (TypeHasExactRowCount(child_readers[i]->Type())) {
			return child_readers[i]->GroupRowsAvailable();
		}
	}
	return child_readers[0]->GroupRowsAvailable();
}

} // namespace duckdb
