
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
	data = new unique_ptr<Vector>[types.size()];
	for (size_t i = 0; i < types.size(); i++) {
		data[i] = make_unique<Vector>(types[i], maximum_size, zero_data);
	}
}

void DataChunk::Reset() {
	count = 0;
	for (size_t i = 0; i < column_count; i++) {
		data[i]->count = 0;
		data[i]->sel_vector = nullptr;
	}
	sel_vector.reset();
}

void DataChunk::Clear() {
	if (data) {
		delete[] data;
	}
	data = nullptr;
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
