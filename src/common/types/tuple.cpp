
#include "common/types/tuple.hpp"
#include "common/types/null_value.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

TupleSerializer::TupleSerializer(const std::vector<TypeId> &types,
                                 std::vector<size_t> _columns)
    : types(types), columns(_columns), base_size(0),
      has_variable_columns(false) {
	if (_columns.size() == 0) {
		// if _columns is not supplied we use all columns
		for (size_t i = 0; i < types.size(); i++) {
			columns.push_back(i);
		}
	}
	assert(types.size() == columns.size());
	is_variable.resize(columns.size());
	type_sizes.resize(columns.size());
	for (size_t i = 0; i < columns.size(); i++) {
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
	uint8_t *target_locations[STANDARD_VECTOR_SIZE];
	for (size_t i = 0; i < chunk.size(); i++) {
		targets[i].size = base_size;
		targets[i].data = unique_ptr<uint8_t[]>(new uint8_t[targets[i].size]);
		target_locations[i] = targets[i].data.get();
	}
	// now serialize to the targets
	Serialize(chunk, target_locations);
}

void TupleSerializer::Serialize(DataChunk &chunk, uint8_t *targets[]) {
	size_t offset = 0;
	for (size_t i = 0; i < columns.size(); i++) {
		SerializeColumn(chunk, targets, i, offset);
	}
}

void TupleSerializer::Serialize(std::vector<char *> &column_data, size_t offset,
                                uint8_t *target) {
	for (size_t i = 0; i < columns.size(); i++) {
		auto source = column_data[columns[i]] + type_sizes[i] * offset;
		memcpy(target, source, type_sizes[i]);
		target += type_sizes[i];
	}
}

void TupleSerializer::Deserialize(std::vector<char *> &column_data,
                                  size_t offset, uint8_t *target) {
	for (size_t i = 0; i < columns.size(); i++) {
		auto source = column_data[columns[i]] + type_sizes[i] * offset;
		memcpy(source, target, type_sizes[i]);
		target += type_sizes[i];
	}
}

void TupleSerializer::Serialize(vector<char *> &column_data,
                                Vector &index_vector, uint8_t *targets[]) {
	auto indices = (uint64_t *)index_vector.data;
	size_t offset = 0;
	for (size_t i = 0; i < columns.size(); i++) {
		for (size_t j = 0; j < index_vector.count; j++) {
			auto index = index_vector.sel_vector
			                 ? indices[index_vector.sel_vector[j]]
			                 : indices[j];
			auto source = column_data[columns[i]] + type_sizes[i] * index;
			memcpy(targets[j] + offset, source, type_sizes[i]);
		}
		offset += type_sizes[i];
	}
}

void TupleSerializer::SerializeUpdate(vector<char *> &column_data,
                                      vector<column_t> &affected_columns,
                                      DataChunk &update_chunk,
                                      Vector &index_vector, size_t index_offset,
                                      Tuple targets[]) {
	auto indices = (uint64_t *)index_vector.data;
	assert(index_vector.count == update_chunk.size());

	// first initialize the tuples
	for (size_t i = 0; i < index_vector.count; i++) {
		targets[i].size = base_size;
		targets[i].data = unique_ptr<uint8_t[]>(new uint8_t[targets[i].size]);
	}

	// now copy the data to the tuples
	size_t offset = 0;
	for (size_t i = 0; i < columns.size(); i++) {
		auto column = columns[i];
		auto update_column = affected_columns[i];
		if (update_column == (column_t)-1) {
			// fetch from base column
			for (size_t j = 0; j < index_vector.count; j++) {
				auto index = (index_vector.sel_vector
				                  ? indices[index_vector.sel_vector[j]]
				                  : indices[j]) -
				             index_offset;
				auto source = column_data[column] + type_sizes[i] * index;
				memcpy(targets[j].data.get() + offset, source, type_sizes[i]);
			}
		} else {
			// fetch from update column
			auto baseptr = update_chunk.data[update_column].data;
			for (size_t j = 0; j < index_vector.count; j++) {
				auto index =
				    update_chunk.sel_vector ? update_chunk.sel_vector[j] : j;
				auto source = baseptr + type_sizes[i] * index;
				memcpy(targets[j].data.get() + offset, source, type_sizes[i]);
			}
		}
		offset += type_sizes[i];
	}
}

static void SerializeValue(uint8_t *target_data, Vector &col, size_t index,
                           size_t result_index, size_t type_size) {
	if (col.nullmask[index]) {
		SetNullValue(target_data, col.type);
	} else {
		memcpy(target_data, col.data + index * type_size, type_size);
	}
}

void TupleSerializer::SerializeColumn(DataChunk &chunk, uint8_t *targets[],
                                      size_t column_index, size_t offsets[]) {
	const auto column = columns[column_index];
	const auto type_size = type_sizes[column_index];
	const auto type = types[column_index];

	assert(type == chunk.data[column].type);
	for (size_t i = 0; i < chunk.size(); i++) {
		size_t index = chunk.sel_vector ? chunk.sel_vector[i] : i;
		auto target_data = targets[i] + offsets[i];

		SerializeValue(target_data, chunk.data[column], index, i, type_size);
		offsets[i] += type_size;
	}
}

void TupleSerializer::SerializeColumn(DataChunk &chunk, uint8_t *targets[],
                                      size_t column_index, size_t &offset) {
	const auto column = columns[column_index];
	const auto type_size = type_sizes[column_index];
	assert(types[column_index] == chunk.data[column].type);
	for (size_t i = 0; i < chunk.size(); i++) {
		size_t index = chunk.sel_vector ? chunk.sel_vector[i] : i;
		auto target_data = targets[i] + offset;
		SerializeValue(target_data, chunk.data[column], index, i, type_size);
	}
	offset += type_size;
}

int TupleSerializer::Compare(Tuple &a, Tuple &b) {
	assert(a.size == b.size);
	return Compare(a.data.get(), b.data.get());
}

int TupleSerializer::Compare(const uint8_t *a, const uint8_t *b) {
	if (!has_variable_columns) {
		// we can just do a memcmp
		return memcmp(a, b, base_size);
	} else {
		int cmp = 0;
		for (size_t i = 0; i < columns.size(); i++) {
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

TupleComparer::TupleComparer(TupleSerializer &left, TupleSerializer &right)
    : left(left) {
	assert(left.columns.size() <= right.columns.size());
	// compute the offsets
	left_offsets.resize(left.columns.size());
	right_offsets.resize(left.columns.size());

	size_t left_offset = 0;
	for (size_t i = 0; i < left.columns.size(); i++) {
		left_offsets[i] = left_offset;
		right_offsets[i] = (size_t)-1;

		size_t right_offset = 0;
		for (size_t j = 0; j < right.columns.size(); j++) {
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
		assert(right_offsets[i] != (size_t)-1);
	}
}

int TupleComparer::Compare(const uint8_t *left_data,
                           const uint8_t *right_data) {
	int cmp;
	for (size_t i = 0; i < left_offsets.size(); i++) {
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
