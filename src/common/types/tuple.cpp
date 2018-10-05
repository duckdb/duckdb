
#include "common/types/tuple.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

template <bool inline_varlength>
TupleSerializer<inline_varlength>::TupleSerializer(
    const std::vector<TypeId> &types, std::vector<size_t> _columns)
    : base_size(base_size), columns(_columns), has_variable_columns(false) {
	assert(types.size() > 0);
	base_size = 0;
	if (_columns.size() == 0) {
		// if _columns is not supplied we use all columns
		for (size_t i = 0; i < types.size(); i++) {
			columns.push_back(i);
		}
	}
	is_variable.resize(columns.size());
	type_sizes.resize(types.size());
	for (size_t i = 0; i < columns.size(); i++) {
		auto column = columns[i];
		auto type = types[column];
		type_sizes[column] = GetTypeIdSize(type);
		if (TypeIsConstantSize(type)) {
			base_size += type_sizes[column];
		} else {
			assert(type == TypeId::VARCHAR);
			has_variable_columns = true;
			is_variable[i] = true;
			if (!inline_varlength) {
				// if we don't inline the var length type we just
				// store a pointer of fixed size
				base_size += type_sizes[column];
			}
		}
	}
}

template <bool inline_varlength>
void TupleSerializer<inline_varlength>::Serialize(DataChunk &chunk,
                                                  Tuple targets[]) {
	// initialize the tuples
	// first compute the sizes, if there are variable length columns
	// if there are none, we can just use the base size
	char *target_locations[STANDARD_VECTOR_SIZE];
	if (inline_varlength && has_variable_columns) {
		// first assign the base size
		for (size_t i = 0; i < chunk.count; i++) {
			targets[i].size = base_size;
		}
		// compute size of variable length columns
		for (size_t j = 0; j < columns.size(); j++) {
			if (is_variable[j]) {
				assert(chunk.data[columns[j]].type == TypeId::VARCHAR);
				const char **dataptr = chunk.data[columns[j]].data;
				for (size_t i = 0; i < chunk.count; i++) {
					size_t index = chunk.sel_vector ? chunk.sel_vector[i] : i;
					targets[i].size += strlen(dataptr[index]) + 1;
				}
			}
		}
		// now allocate the data
		for (size_t i = 0; i < chunk.count; i++) {
			targets[i].data =
			    unique_ptr<uint8_t[]>(new uint8_t[targets[i].size]);
			target_locations[i] = (char *)targets[i].data.get();
		}
	} else {
		for (size_t i = 0; i < chunk.count; i++) {
			targets[i].size = base_size;
			targets[i].data =
			    unique_ptr<uint8_t[]>(new uint8_t[targets[i].size]);
			target_locations[i] = (char *)targets[i].data.get();
		}
	}
	// now serialize to the targets
	Serialize(chunk, target_locations);
}

template <bool inline_varlength>
void TupleSerializer<inline_varlength>::Serialize(DataChunk &chunk,
                                                  const char *targets[]) {
	if (inline_varlength && has_variable_columns) {
		// we have variable columns, the offsets might be different
		size_t offsets[STANDARD_VECTOR_SIZE] = {0};
		for (auto &column : columns) {
			SerializeColumn(chunk, targets, column, offsets);
		}
	} else {
		size_t offset = 0;
		for (auto &column : columns) {
			SerializeColumn(chunk, targets, column, offset);
		}
	}
}

//! Writes NullValue<T> value of a specific type to a memory address
static void SetNullValue(uint8_t *ptr, TypeId type) {
	switch (type) {
	case TypeId::TINYINT:
		*((int8_t *)ptr) = NullValue<int8_t>();
		break;
	case TypeId::SMALLINT:
		*((int16_t *)ptr) = NullValue<int16_t>();
		break;
	case TypeId::INTEGER:
		*((int32_t *)ptr) = NullValue<int32_t>();
		break;
	case TypeId::DATE:
		*((date_t *)ptr) = NullValue<date_t>();
		break;
	case TypeId::BIGINT:
		*((int64_t *)ptr) = NullValue<int64_t>();
		break;
	case TypeId::TIMESTAMP:
		*((timestamp_t *)ptr) = NullValue<timestamp_t>();
		break;
	case TypeId::DECIMAL:
		*((double *)ptr) = NullValue<double>();
		break;
	case TypeId::VARCHAR:
		*((const char **)ptr) = NullValue<const char *>();
		break;
	default:
		throw InvalidTypeException(type,
		                           "Non-integer type in HT initialization!");
	}
}

static void SerializeValue(uint8_t *target_data, Vector &col, size_t index,
                           size_t type_size) {
	if (col.nullmask[index]) {
		SetNullValue(target_data, col.type);
	} else {
		memcpy(target_data, col.data + index * type_size, type_size);
	}
}

template <bool inline_varlength>
void TupleSerializer<inline_varlength>::SerializeColumn(DataChunk &chunk,
                                                        const char *targets[],
                                                        size_t column,
                                                        size_t offsets[]) {
	const auto type = chunk.data[column].type;
	const auto is_constant = TypeIsConstantSize(type) || !inline_varlength;
	const auto type_size = type_sizes[column];
	for (size_t i = 0; i < chunk.count; i++) {
		size_t index = chunk.sel_vector ? chunk.sel_vector[i] : i;
		auto target_data = targets[i] + offsets[i];

		if (is_constant) {
			SerializeValue(target_data + offsets[i], chunk.data[column], index,
			               type_size);
			offsets[i] += type_size;
		} else {
			assert(type == TypeId::VARCHAR);
			// inline the string
			const char **dataptr = (const char **)chunk.data[column].data;
			auto str = (chunk.data[column].nullmask[i] || !dataptr[index])
			               ? NullValue<const char *>()
			               : dataptr[index];

			strcpy((char *)(target_data + offsets[i]), str);
			offsets[i] += strlen(str) + 1;
		}
	}
}

template <bool inline_varlength>
void TupleSerializer<inline_varlength>::SerializeColumn(DataChunk &chunk,
                                                        const char *targets[],
                                                        size_t column,
                                                        size_t &offset) {
	const auto type_size = type_sizes[column];
	for (size_t i = 0; i < chunk.count; i++) {
		size_t index = chunk.sel_vector ? chunk.sel_vector[i] : i;
		auto target_data = targets[i] + offset;
		SerializeValue(target_data + offset, chunk.data[column], index,
		               type_size);
	}
	offset += type_size;
}

template <bool inline_varlength>
int TupleSerializer<inline_varlength>::Compare(Tuple &a, Tuple &b) {
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

template <bool inline_varlength>
int TupleSerializer<inline_varlength>::Compare(const char *a, const char *b) {
	if (!has_variable_columns) {
		// we can just do a memcmp
		return memcmp(a, b, base_size);
	} else {
		int cmp = 0;
		for (size_t i = 0; i < columns.size(); i++) {
			auto column = columns[i];
			auto type_size = type_sizes[column];
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
