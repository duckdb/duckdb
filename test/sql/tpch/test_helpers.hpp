
#pragma once

#include "duckdb.hpp"

static bool CHECK_COLUMN(std::unique_ptr<duckdb::DuckDBResult> &result,
                         size_t column_number, std::vector<int64_t> values) {
	if (!result->GetSuccess()) {
		fprintf(stderr, "Query failed with message: %s\n",
		        result->GetErrorMessage().c_str());
		return false;
	}
	if (values.size() == 0) {
		return result->data.size() == 0;
	}
	if (result->data.size() == 0) {
		return false;
	}
	if (column_number >= result->data[0]->column_count) {
		return false;
	}
	size_t chunk_index = 0;
	for (size_t i = 0; i < values.size();) {
		if (chunk_index > result->data.size()) {
			// ran out of chunks
			return false;
		}
		// check this vector
		auto &vector = result->data[chunk_index]->data[column_number];
		if (i + vector->count > values.size()) {
			// too many values in this vector
			return false;
		}
		for (size_t j = 0; j < vector->count; j++) {
			if (vector->GetValue(j).GetNumericValue() != values[i + j]) {
				return false;
			}
		}
		chunk_index++;
		i += vector->count;
	}
	return true;
}
