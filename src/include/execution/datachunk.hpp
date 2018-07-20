
#pragma once

#include <vector>

#include "common/internal_types.hpp"

namespace duckdb {
class DataChunk {
  public:
	size_t count;
	size_t colcount;
	void** data;
	size_t maximum_size;


	DataChunk();
	~DataChunk();

	void Initialize(std::vector<TypeId>& types, size_t maximum_chunk_size);

	void Reset();
};
}
