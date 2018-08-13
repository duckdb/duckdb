
#pragma once

#include "duckdb.hpp"

static void CHECK_COLUMN(std::unique_ptr<duckdb::DuckDBResult> &result,
                         size_t column_number, std::vector<duckdb::Value> values) {
	if (!result->GetSuccess()) {
		fprintf(stderr, "Query failed with message: %s\n",
		        result->GetErrorMessage().c_str());
		FAIL(result->GetErrorMessage().c_str());
	}
	if (values.size() == 0) {
		if (result->data.size() != 0) {
			FAIL("Data size does not match value size!");
		} else {
			return;
		}
	}
	if (result->data.size() == 0) {
		FAIL("Data size does not match value size!");
	}
	if (column_number >= result->data[0]->column_count) {
		FAIL("Column number out of range of result!");
	}
	size_t chunk_index = 0;
	for (size_t i = 0; i < values.size();) {
		if (chunk_index > result->data.size()) {
			// ran out of chunks
			FAIL("Data size does not match value size!");
		}
		// check this vector
		auto &vector = result->data[chunk_index]->data[column_number];
		if (i + vector->count > values.size()) {
			// too many values in this vector
			FAIL("Too many values in result!");
		}
		for (size_t j = 0; j < vector->count; j++) {
			if (!duckdb::Value::Equals(vector->GetValue(j), values[i + j])) {
				FAIL("Incorrect result! Got " + vector->GetValue(j).ToString() +
				     " but expected " + values[i + j].ToString());
			}
		}
		chunk_index++;
		i += vector->count;
	}
}

static void RESULT_NO_ERROR(std::unique_ptr<duckdb::DuckDBResult> &result) {
	if (!result->GetSuccess()) {
		FAIL(result->GetErrorMessage().c_str());
	}
}