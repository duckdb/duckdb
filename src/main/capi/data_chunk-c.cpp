#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/type_visitor.hpp"

#include <string.h>

duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *column_types, idx_t column_count) {
	if (!column_types) {
		return nullptr;
	}
	duckdb::vector<duckdb::LogicalType> types;
	for (idx_t i = 0; i < column_count; i++) {
		auto logical_type = reinterpret_cast<duckdb::LogicalType *>(column_types[i]);
		if (duckdb::TypeVisitor::Contains(*logical_type, duckdb::LogicalTypeId::INVALID) ||
		    duckdb::TypeVisitor::Contains(*logical_type, duckdb::LogicalTypeId::ANY)) {
			return nullptr;
		}
		types.push_back(*logical_type);
	}

	auto result = new duckdb::DataChunk();
	try {
		result->Initialize(duckdb::Allocator::DefaultAllocator(), types);
	} catch (...) {
		delete result;
		return nullptr;
	}

	return reinterpret_cast<duckdb_data_chunk>(result);
}

void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk) {
	if (chunk && *chunk) {
		auto data_chunk = reinterpret_cast<duckdb::DataChunk *>(*chunk);
		delete data_chunk;
		*chunk = nullptr;
	}
}

void duckdb_data_chunk_reset(duckdb_data_chunk chunk) {
	if (!chunk) {
		return;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	dchunk->Reset();
}

duckdb_vector duckdb_create_vector(duckdb_logical_type type, idx_t capacity) {
	auto dtype = reinterpret_cast<duckdb::LogicalType *>(type);
	try {
		auto vector = new duckdb::Vector(*dtype, capacity);
		return reinterpret_cast<duckdb_vector>(vector);
	} catch (...) {
		return nullptr;
	}
}

void duckdb_destroy_vector(duckdb_vector *vector) {
	if (vector && *vector) {
		auto dvector = reinterpret_cast<duckdb::Vector *>(*vector);
		delete dvector;
		*vector = nullptr;
	}
}

idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk) {
	if (!chunk) {
		return 0;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	return dchunk->ColumnCount();
}

duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx) {
	if (!chunk || col_idx >= duckdb_data_chunk_get_column_count(chunk)) {
		return nullptr;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	return reinterpret_cast<duckdb_vector>(&dchunk->data[col_idx]);
}

idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk) {
	if (!chunk) {
		return 0;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	return dchunk->size();
}

void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size) {
	if (!chunk) {
		return;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	dchunk->SetCardinality(size);
}

duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(v->GetType()));
}

void *duckdb_vector_get_data(duckdb_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return duckdb::FlatVector::GetData(*v);
}

uint64_t *duckdb_vector_get_validity(duckdb_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	switch (v->GetVectorType()) {
	case duckdb::VectorType::CONSTANT_VECTOR:
		return duckdb::ConstantVector::Validity(*v).GetData();
	case duckdb::VectorType::FLAT_VECTOR:
		return duckdb::FlatVector::Validity(*v).GetData();
	default:
		return nullptr;
	}
}

void duckdb_vector_ensure_validity_writable(duckdb_vector vector) {
	if (!vector) {
		return;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	auto &validity = duckdb::FlatVector::Validity(*v);
	validity.EnsureWritable();
}

void duckdb_vector_assign_string_element(duckdb_vector vector, idx_t index, const char *str) {
	duckdb_vector_assign_string_element_len(vector, index, str, strlen(str));
}

void duckdb_vector_assign_string_element_len(duckdb_vector vector, idx_t index, const char *str, idx_t str_len) {
	if (!vector) {
		return;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	auto data = duckdb::FlatVector::GetData<duckdb::string_t>(*v);
	data[index] = duckdb::StringVector::AddStringOrBlob(*v, str, str_len);
}

duckdb_vector duckdb_list_vector_get_child(duckdb_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return reinterpret_cast<duckdb_vector>(&duckdb::ListVector::GetEntry(*v));
}

idx_t duckdb_list_vector_get_size(duckdb_vector vector) {
	if (!vector) {
		return 0;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return duckdb::ListVector::GetListSize(*v);
}

duckdb_state duckdb_list_vector_set_size(duckdb_vector vector, idx_t size) {
	if (!vector) {
		return DuckDBError;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	duckdb::ListVector::SetListSize(*v, size);
	return DuckDBSuccess;
}

duckdb_state duckdb_list_vector_reserve(duckdb_vector vector, idx_t required_capacity) {
	if (!vector) {
		return DuckDBError;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	duckdb::ListVector::Reserve(*v, required_capacity);
	return DuckDBSuccess;
}

duckdb_vector duckdb_struct_vector_get_child(duckdb_vector vector, idx_t index) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return reinterpret_cast<duckdb_vector>(duckdb::StructVector::GetEntries(*v)[index].get());
}

duckdb_vector duckdb_array_vector_get_child(duckdb_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return reinterpret_cast<duckdb_vector>(&duckdb::ArrayVector::GetEntry(*v));
}

bool duckdb_validity_row_is_valid(uint64_t *validity, idx_t row) {
	if (!validity) {
		return true;
	}
	idx_t entry_idx = row / 64;
	idx_t idx_in_entry = row % 64;
	return validity[entry_idx] & ((idx_t)1 << idx_in_entry);
}

void duckdb_validity_set_row_validity(uint64_t *validity, idx_t row, bool valid) {
	if (valid) {
		duckdb_validity_set_row_valid(validity, row);
	} else {
		duckdb_validity_set_row_invalid(validity, row);
	}
}

void duckdb_validity_set_row_invalid(uint64_t *validity, idx_t row) {
	if (!validity) {
		return;
	}
	idx_t entry_idx = row / 64;
	idx_t idx_in_entry = row % 64;
	validity[entry_idx] &= ~((uint64_t)1 << idx_in_entry);
}

void duckdb_validity_set_row_valid(uint64_t *validity, idx_t row) {
	if (!validity) {
		return;
	}
	idx_t entry_idx = row / 64;
	idx_t idx_in_entry = row % 64;
	validity[entry_idx] |= (uint64_t)1 << idx_in_entry;
}

duckdb_selection_vector duckdb_create_selection_vector(idx_t size) {
	return reinterpret_cast<duckdb_selection_vector>(new duckdb::SelectionVector(size));
}

void duckdb_destroy_selection_vector(duckdb_selection_vector sel) {
	delete reinterpret_cast<duckdb::SelectionVector *>(sel);
}

sel_t *duckdb_selection_vector_get_data_ptr(duckdb_selection_vector sel) {
	return reinterpret_cast<duckdb::SelectionVector *>(sel)->data();
}

void duckdb_slice_vector(duckdb_vector dict, duckdb_selection_vector sel, idx_t len) {
	auto d_dict = reinterpret_cast<duckdb::Vector *>(dict);
	auto d_sel = reinterpret_cast<duckdb::SelectionVector *>(sel);
	d_dict->Slice(*d_sel, len);
}

void duckdb_vector_copy_sel(duckdb_vector src, duckdb_vector dst, duckdb_selection_vector sel, idx_t src_count,
                            idx_t src_offset, idx_t dst_offset) {
	auto d_src = reinterpret_cast<duckdb::Vector *>(src);
	auto d_dst = reinterpret_cast<duckdb::Vector *>(dst);
	auto d_sel = reinterpret_cast<duckdb::SelectionVector *>(sel);
	duckdb::VectorOperations::Copy(*d_src, *d_dst, *d_sel, src_count, src_offset, dst_offset);
}

void duckdb_vector_reference_value(duckdb_vector vector, duckdb_value value) {
	auto dvector = reinterpret_cast<duckdb::Vector *>(vector);
	auto dvalue = reinterpret_cast<duckdb::Value *>(value);
	dvector->Reference(*dvalue);
}

void duckdb_vector_reference_vector(duckdb_vector to_vector, duckdb_vector from_vector) {
	auto dto_vector = reinterpret_cast<duckdb::Vector *>(to_vector);
	auto dfrom_vector = reinterpret_cast<duckdb::Vector *>(from_vector);
	dto_vector->Reference(*dfrom_vector);
}
