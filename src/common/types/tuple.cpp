
#include "common/types/tuple.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

TupleSerializer::TupleSerializer(const std::vector<TypeId> &types,
                                 bool inline_varlength,
                                 std::vector<size_t> _columns)
    : types(types), columns(_columns), base_size(0),
      has_variable_columns(false), inline_varlength(inline_varlength) {
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
			if (!inline_varlength) {
				// if we don't inline the var length type we just
				// store a pointer of fixed size
				base_size += type_sizes[i];
			}
		}
	}
}

void TupleSerializer::Serialize(DataChunk &chunk, Tuple targets[],
                                bool *has_null) {
	// initialize the tuples
	// first compute the sizes, if there are variable length columns
	// if there are none, we can just use the base size
	uint8_t *target_locations[STANDARD_VECTOR_SIZE];
	if (inline_varlength && has_variable_columns) {
		// first assign the base size
		for (size_t i = 0; i < chunk.count; i++) {
			targets[i].size = base_size;
		}
		// compute size of variable length columns
		for (size_t j = 0; j < columns.size(); j++) {
			if (is_variable[j]) {
				assert(chunk.data[columns[j]].type == TypeId::VARCHAR);
				auto dataptr = (const char **)chunk.data[columns[j]].data;
				for (size_t i = 0; i < chunk.count; i++) {
					size_t index = chunk.sel_vector ? chunk.sel_vector[i] : i;
					auto str = dataptr[index];
					if (!str || chunk.data[columns[j]].nullmask[index]) {
						str = NullValue<const char *>();
					}
					targets[i].size += strlen(str) + 1;
				}
			}
		}
		// now allocate the data
		for (size_t i = 0; i < chunk.count; i++) {
			targets[i].data =
			    unique_ptr<uint8_t[]>(new uint8_t[targets[i].size]);
			target_locations[i] = targets[i].data.get();
		}
	} else {
		for (size_t i = 0; i < chunk.count; i++) {
			targets[i].size = base_size;
			targets[i].data =
			    unique_ptr<uint8_t[]>(new uint8_t[targets[i].size]);
			target_locations[i] = targets[i].data.get();
		}
	}
	// now serialize to the targets
	Serialize(chunk, target_locations, has_null);
}

void TupleSerializer::Serialize(DataChunk &chunk, uint8_t *targets[],
                                bool *has_null) {
	if (inline_varlength && has_variable_columns) {
		// we have variable columns, the offsets might be different
		size_t offsets[STANDARD_VECTOR_SIZE] = {0};
		for (size_t i = 0; i < columns.size(); i++) {
			SerializeColumn(chunk, targets, i, offsets, has_null);
		}
	} else {
		size_t offset = 0;
		for (size_t i = 0; i < columns.size(); i++) {
			SerializeColumn(chunk, targets, i, offset, has_null);
		}
	}
}

static void SerializeValue(uint8_t *target_data, Vector &col, size_t index,
                           size_t result_index, size_t type_size,
                           bool *has_null) {
	if (col.nullmask[index]) {
		SetNullValue(target_data, col.type);
		if (has_null) {
			has_null[result_index] = true;
		}
	} else {
		memcpy(target_data, col.data + index * type_size, type_size);
	}
}

void TupleSerializer::SerializeColumn(DataChunk &chunk, uint8_t *targets[],
                                      size_t column_index, size_t offsets[],
                                      bool *has_null) {
	const auto column = columns[column_index];
	const auto type_size = type_sizes[column_index];
	const auto type = types[column_index];
	const auto is_constant = TypeIsConstantSize(type) || !inline_varlength;

	assert(type == chunk.data[column].type);
	for (size_t i = 0; i < chunk.count; i++) {
		size_t index = chunk.sel_vector ? chunk.sel_vector[i] : i;
		auto target_data = targets[i] + offsets[i];

		if (is_constant) {
			SerializeValue(target_data, chunk.data[column], index, i, type_size,
			               has_null);
			offsets[i] += type_size;
		} else {
			// inline the string
			assert(type == TypeId::VARCHAR);
			const char **dataptr = (const char **)chunk.data[column].data;
			auto str = dataptr[index];
			if (chunk.data[column].nullmask[i] || !dataptr[index]) {
				str = NullValue<const char *>();
				if (has_null) {
					has_null[i] = true;
				}
			}
			strcpy((char *)target_data, str);
			offsets[i] += strlen(str) + 1;
		}
	}
}

void TupleSerializer::SerializeColumn(DataChunk &chunk, uint8_t *targets[],
                                      size_t column_index, size_t &offset,
                                      bool *has_null) {
	const auto column = columns[column_index];
	const auto type_size = type_sizes[column_index];
	assert(types[column_index] == chunk.data[column].type);
	for (size_t i = 0; i < chunk.count; i++) {
		size_t index = chunk.sel_vector ? chunk.sel_vector[i] : i;
		auto target_data = targets[i] + offset;
		SerializeValue(target_data, chunk.data[column], index, i, type_size,
		               has_null);
	}
	offset += type_size;
}

int TupleSerializer::Compare(Tuple &a, Tuple &b) {
	if (!inline_varlength || !has_variable_columns) {
		assert(a.size == b.size);
		return Compare(a.data.get(), b.data.get());
	} else {
		// sizes can be different
		auto min_size = min(a.size, b.size);
		int cmp = memcmp(a.data.get(), b.data.get(), min_size);
		if (cmp == 0 && a.size != b.size) {
			// the compared bytes are equivalent but size is not
			// comparison result depends on the size
			// a < b if a is smaller
			// b < a if b is smaller
			return (a.size < b.size) ? -1 : 1;
		} else {
			return cmp;
		}
	}
}

int TupleSerializer::Compare(const uint8_t *a, const uint8_t *b) {
	assert(!inline_varlength || !has_variable_columns);
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
