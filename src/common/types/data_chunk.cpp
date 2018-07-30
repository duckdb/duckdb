
#include "common/types/data_chunk.hpp"

#include "common/exception.hpp"
#include "common/helper.hpp"

using namespace duckdb;
using namespace std;

DataChunk::DataChunk()
    : count(0), column_count(0), maximum_size(0), data(nullptr) {}

DataChunk::~DataChunk() {
	if (data) {
		delete[] data;
	}
}

void DataChunk::Initialize(std::vector<TypeId> &types,
                           size_t maximum_chunk_size, bool zero_data) {
	maximum_size = maximum_chunk_size;
	count = 0;
	column_count = types.size();
	size_t size = 0;
	for(auto &type : types) {
		size += GetTypeIdSize(type) * maximum_size;
	}
	default_vector_data = new char[size];
	if (zero_data) {
		memset(default_vector_data, 0, size);
	}

	char *ptr = default_vector_data;
	data = new unique_ptr<Vector>[types.size()];
	for (size_t i = 0; i < types.size(); i++) {
		data[i] = make_unique<Vector>(types[i], ptr, maximum_size);
		ptr += GetTypeIdSize(types[i]) * maximum_size;
	}
}

void DataChunk::Reset() {
	count = 0;
	char *ptr = default_vector_data;
	for (size_t i = 0; i < column_count; i++) {
		data[i]->data = ptr;
		data[i]->owns_data = false;
		data[i]->count = 0;
		data[i]->sel_vector = nullptr;
		ptr += GetTypeIdSize(data[i]->type) * maximum_size;
	}
	sel_vector.reset();
}

void DataChunk::Clear() {
	if (data) {
		delete[] data;
	}
	if (default_vector_data) {
		delete[] default_vector_data;
	}
	data = nullptr;
	default_vector_data = nullptr;
	column_count = 0;
	count = 0;
	maximum_size = 0;
}

void DataChunk::Append(DataChunk &other) {
	if (other.count == 0) {
		return;
	}
	if (count + other.count > maximum_size) {
		// resize
		maximum_size = maximum_size * 2;
		for (size_t i = 0; i < column_count; i++) {
			data[i]->Resize(maximum_size);
		}
	}
	for (size_t i = 0; i < column_count; i++) {
		data[i]->Append(*other.data[i].get());
	}
	count += other.count;
}
