
#include "common/types/data_chunk.hpp"
#include "common/types/vector_operations.hpp"

#include "common/exception.hpp"
#include "common/helper.hpp"

using namespace duckdb;
using namespace std;

DataChunk::DataChunk()
    : count(0), column_count(0), maximum_size(0), data(nullptr) {}

DataChunk::~DataChunk() {}

void DataChunk::Initialize(std::vector<TypeId> &types, oid_t maximum_chunk_size,
                           bool zero_data) {
	maximum_size = maximum_chunk_size;
	count = 0;
	column_count = types.size();
	oid_t size = 0;
	for (auto &type : types) {
		size += GetTypeIdSize(type) * maximum_size;
	}
	if (size > 0) {
		owned_data = unique_ptr<char[]>(new char[size]);
		if (zero_data) {
			memset(owned_data.get(), 0, size);
		}
	}

	char *ptr = owned_data.get();
	data = unique_ptr<unique_ptr<Vector>[]>(
	    new unique_ptr<Vector>[ types.size() ]);
	for (oid_t i = 0; i < types.size(); i++) {
		data[i] = make_unique<Vector>(types[i], ptr, maximum_size);
		ptr += GetTypeIdSize(types[i]) * maximum_size;
	}
}

void DataChunk::Reset() {
	count = 0;
	char *ptr = owned_data.get();
	for (oid_t i = 0; i < column_count; i++) {
		data[i]->data = ptr;
		data[i]->owns_data = false;
		data[i]->count = 0;
		data[i]->sel_vector = nullptr;
		ptr += GetTypeIdSize(data[i]->type) * maximum_size;
	}
	sel_vector.reset();
}

void DataChunk::Destroy() {
	data = nullptr;
	owned_data.reset();
	column_count = 0;
	count = 0;
	maximum_size = 0;
}

void DataChunk::ForceOwnership() {
	char *ptr = owned_data.get();
	for (oid_t i = 0; i < column_count;
	     ptr += GetTypeIdSize(data[i]->type) * maximum_size, i++) {
		if (data[i]->owns_data)
			continue;
		if (data[i]->data == ptr)
			continue;

		VectorOperations::Copy(*data[i].get(), ptr);
		data[i]->data = ptr;
	}
}

void DataChunk::Append(DataChunk &other) {
	if (other.count == 0) {
		return;
	}
	if (column_count != other.column_count) {
		throw Exception("Column counts of appending chunk doesn't match!");
	}
	for (oid_t i = 0; i < column_count; i++) {
		if (other.data[i]->type != data[i]->type) {
			throw Exception("Column types do not match!");
		}
	}
	if (count + other.count > maximum_size) {
		// resize
		maximum_size = maximum_size * 2;
		for (oid_t i = 0; i < column_count; i++) {
			data[i]->Resize(maximum_size);
		}
	}
	for (oid_t i = 0; i < column_count; i++) {
		data[i]->Append(*other.data[i].get());
	}
	count += other.count;
}
