

#pragma once

#include "common/internal_types.hpp"
#include "common/types/vector.hpp"
#include "common/types/data_chunk.hpp"

namespace duckdb {

class SuperLargeHashTable {
public:
	SuperLargeHashTable(size_t initial_capacity, size_t group_width, size_t payload_width, std::vector<ExpressionType> aggregate_types, bool parallel = false);
	~SuperLargeHashTable();

	void Resize(size_t size);
	void AddChunk(DataChunk& groups, DataChunk& payload);
	void Scan(size_t& scan_position, DataChunk& result);

private:
	std::vector<ExpressionType> aggregate_types;

	size_t group_width;
	size_t payload_width;
	size_t tuple_size;
	size_t capacity;
	size_t entries;
	uint8_t *data;
	size_t max_chain;
	bool parallel = false;

	static constexpr int FLAG_SIZE = sizeof(uint8_t);
	static constexpr int EMPTY_CELL = 0x00;
	static constexpr int FULL_CELL = 0xFF;

	SuperLargeHashTable(const SuperLargeHashTable &) = delete;
};

}
