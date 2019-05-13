#include "common/types/tuple.hpp"

#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

TupleSerializer::TupleSerializer() : base_size(0), has_variable_columns(false) {
}

TupleSerializer::TupleSerializer(const vector<TypeId> &types_, vector<index_t> columns_) : TupleSerializer() {
	Initialize(types_, columns_);
}

void TupleSerializer::Initialize(const vector<TypeId> &types_, vector<index_t> columns_) {
	types = types_;

	if (columns_.size() == 0) {
		// if _columns is not supplied we use all columns
		for (index_t i = 0; i < types.size(); i++) {
			columns.push_back(i);
		}
	} else {
		columns = columns_;
	}

	assert(types.size() == columns.size());
	is_variable.resize(columns.size());
	type_sizes.resize(columns.size());
	for (index_t i = 0; i < columns.size(); i++) {
		auto type = types[i];
		type_sizes[i] = GetTypeIdSize(type);
		if (TypeIsConstantSize(type)) {
			base_size += type_sizes[i];
		} else {
			assert(type == TypeId::VARCHAR);
			has_variable_columns = true;
			is_variable[i] = true;
			base_size += type_sizes[i];
		}
	}
}

void TupleSerializer::Serialize(DataChunk &chunk, Tuple targets[]) {
	// initialize the tuples
	// first compute the sizes, if there are variable length columns
	// if there are none, we can just use the base size
	data_ptr_t target_locations[STANDARD_VECTOR_SIZE];
	for (index_t i = 0; i < chunk.size(); i++) {
		targets[i].size = base_size;
		targets[i].data = unique_ptr<data_t[]>(new data_t[targets[i].size]);
		target_locations[i] = targets[i].data.get();
	}
	// now serialize to the targets
	Serialize(chunk, target_locations);
}

void TupleSerializer::Serialize(DataChunk &chunk, data_ptr_t targets[]) {
	index_t offset = 0;
	for (index_t i = 0; i < columns.size(); i++) {
		SerializeColumn(chunk, targets, i, offset);
	}
}

void TupleSerializer::Deserialize(Vector &source, DataChunk &chunk) {
	for (index_t i = 0; i < columns.size(); i++) {
		DeserializeColumn(source, i, chunk.data[columns[i]]);
	}
}

void TupleSerializer::Serialize(vector<data_ptr_t> &column_data, index_t offset, data_ptr_t target) {
	for (index_t i = 0; i < columns.size(); i++) {
		auto source = column_data[columns[i]] + type_sizes[i] * offset;
		memcpy(target, source, type_sizes[i]);
		target += type_sizes[i];
	}
}

void TupleSerializer::Deserialize(vector<data_ptr_t> &column_data, index_t offset, data_ptr_t target) {
	for (index_t i = 0; i < columns.size(); i++) {
		auto source = column_data[columns[i]] + type_sizes[i] * offset;
		memcpy(source, target, type_sizes[i]);
		target += type_sizes[i];
	}
}

void TupleSerializer::Serialize(vector<data_ptr_t> &column_data, Vector &index_vector, data_ptr_t targets[]) {
	auto indices = (index_t *)index_vector.data;
	index_t offset = 0;
	for (index_t i = 0; i < columns.size(); i++) {
		for (index_t j = 0; j < index_vector.count; j++) {
			auto index = index_vector.sel_vector ? indices[index_vector.sel_vector[j]] : indices[j];
			auto source = column_data[columns[i]] + type_sizes[i] * index;
			memcpy(targets[j] + offset, source, type_sizes[i]);
		}
		offset += type_sizes[i];
	}
}

void TupleSerializer::SerializeUpdate(vector<data_ptr_t> &column_data, vector<column_t> &affected_columns,
                                      DataChunk &update_chunk, Vector &index_vector, index_t index_offset,
                                      Tuple targets[]) {
	auto indices = (index_t *)index_vector.data;
	assert(index_vector.count == update_chunk.size());

	// first initialize the tuples
	for (index_t i = 0; i < index_vector.count; i++) {
		targets[i].size = base_size;
		targets[i].data = unique_ptr<data_t[]>(new data_t[targets[i].size]);
	}

	// now copy the data to the tuples
	index_t offset = 0;
	for (index_t i = 0; i < columns.size(); i++) {
		auto column = columns[i];
		auto update_column = affected_columns[i];
		if (update_column == (column_t)-1) {
			// fetch from base column
			for (index_t j = 0; j < index_vector.count; j++) {
				auto index =
				    (index_vector.sel_vector ? indices[index_vector.sel_vector[j]] : indices[j]) - index_offset;
				auto source = column_data[column] + type_sizes[i] * index;
				memcpy(targets[j].data.get() + offset, source, type_sizes[i]);
			}
		} else {
			// fetch from update column
			auto baseptr = update_chunk.data[update_column].data;
			for (index_t j = 0; j < index_vector.count; j++) {
				auto index = update_chunk.sel_vector ? update_chunk.sel_vector[j] : j;
				auto source = baseptr + type_sizes[i] * index;
				memcpy(targets[j].data.get() + offset, source, type_sizes[i]);
			}
		}
		offset += type_sizes[i];
	}
}

static void SerializeValue(data_ptr_t target_data, Vector &col, index_t index, index_t result_index,
                           index_t type_size) {
	if (col.nullmask[index]) {
		SetNullValue(target_data, col.type);
	} else {
		memcpy(target_data, col.data + index * type_size, type_size);
	}
}

void TupleSerializer::SerializeColumn(DataChunk &chunk, data_ptr_t targets[], index_t column_index, index_t &offset) {
	const auto column = columns[column_index];
	const auto type_size = type_sizes[column_index];
	assert(types[column_index] == chunk.data[column].type);
	VectorOperations::Exec(chunk.data[column], [&](index_t i, index_t k) {
		auto target_data = targets[k] + offset;
		SerializeValue(target_data, chunk.data[column], i, k, type_size);
	});
	offset += type_size;
}

void TupleSerializer::DeserializeColumn(Vector &source, index_t column_index, Vector &target) {
	const auto type_size = type_sizes[column_index];
	assert(types[column_index] == target.type);
	target.count = source.count;
	VectorOperations::Gather::Set(source, target);
	VectorOperations::AddInPlace(source, type_size);
}

int TupleSerializer::Compare(Tuple &a, Tuple &b) {
	assert(a.size == b.size);
	return Compare(a.data.get(), b.data.get());
}

int TupleSerializer::Compare(const_data_ptr_t a, const_data_ptr_t b) {
	if (!has_variable_columns) {
		// we can just do a memcmp
		return memcmp(a, b, base_size);
	} else {
		int cmp = 0;
		for (index_t i = 0; i < columns.size(); i++) {
			auto type_size = type_sizes[i];
			if (is_variable[i]) {
				auto left = *((const char **)a);
				auto right = *((const char **)b);
				cmp = strcmp(left, right);
			} else {
				cmp = memcmp(a, b, type_size);
			}
			if (cmp != 0) {
				break;
			}
			a += type_size;
			b += type_size;
		}
		return cmp;
	}
}

TupleComparer::TupleComparer(TupleSerializer &left, TupleSerializer &right) : left(left) {
	assert(left.columns.size() <= right.columns.size());
	// compute the offsets
	left_offsets.resize(left.columns.size());
	right_offsets.resize(left.columns.size());

	index_t left_offset = 0;
	for (index_t i = 0; i < left.columns.size(); i++) {
		left_offsets[i] = left_offset;
		right_offsets[i] = INVALID_INDEX;

		index_t right_offset = 0;
		for (index_t j = 0; j < right.columns.size(); j++) {
			if (left.columns[i] == right.columns[j]) {
				assert(left.types[i] == right.types[j]);

				// found it!
				right_offsets[i] = right_offset;
				break;
			}
			right_offset += right.type_sizes[j];
		}
		// assert that we found the column
		left_offset += left.type_sizes[i];
		assert(right_offsets[i] != INVALID_INDEX);
	}
}

int TupleComparer::Compare(const_data_ptr_t left_data, const_data_ptr_t right_data) {
	int cmp;
	for (index_t i = 0; i < left_offsets.size(); i++) {
		auto left_element = left_data + left_offsets[i];
		auto right_element = right_data + right_offsets[i];

		if (left.is_variable[i]) {
			// string comparison
			assert(left.types[i] == TypeId::VARCHAR);
			auto left_string = *((char **)left_element);
			auto right_string = *((char **)right_element);
			cmp = strcmp(left_string, right_string);
		} else {
			cmp = memcmp(left_element, right_element, left.type_sizes[i]);
		}

		if (cmp != 0) {
			return cmp;
		}
	}
	return 0;
}
