
#include "execution/datachunk.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

DataChunk::DataChunk() : data(nullptr) { }


DataChunk::~DataChunk() {
	if (data) {
		for(size_t i = 0; i < colcount; i++) {
			free(data[i]);
		}
		free(data);
		data = nullptr;
	}
}


void DataChunk::Initialize(std::vector<TypeId>& types, size_t maximum_chunk_size) {
	maximum_size = maximum_chunk_size;
	count = 0;
	colcount = types.size();
	data = (void**) malloc(colcount * sizeof(void*));
	if (!data) {
		throw NullPointerException("malloc failure!");
	}
	size_t i = 0;
	for(TypeId& id : types) {
		data[i] = malloc(GetTypeIdSize(id) * maximum_size);
		if (!data[i]) {
			throw NullPointerException("malloc failure!");
		}
		i++;
	}
}

void DataChunk::Reset() {
	count = 0;
}
