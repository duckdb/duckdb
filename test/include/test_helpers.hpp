
#pragma once

#define CATCH_CONFIG_MAIN

#include "catch.hpp"
#include "common/string_util.hpp"
#include "compare_result.hpp"
#include "duckdb.hpp"

namespace duckdb {
static void CHECK_COLUMN(std::unique_ptr<duckdb::DuckDBResult> &result,
                         size_t column_number,
                         std::vector<duckdb::Value> values) {
	if (!result->GetSuccess()) {
		fprintf(stderr, "Query failed with message: %s\n",
		        result->GetErrorMessage().c_str());
		FAIL(result->GetErrorMessage().c_str());
	}
	if (values.size() == 0) {
		if (result->size() != 0) {
			FAIL("Data size does not match value size!");
		} else {
			return;
		}
	}
	if (result->size() == 0) {
		FAIL("Data size does not match value size!");
	}
	if (column_number >= result->column_count()) {
		FAIL("Column number out of range of result!");
	}
	size_t chunk_index = 0;
	for (size_t i = 0; i < values.size();) {
		if (chunk_index > result->size()) {
			// ran out of chunks
			FAIL("Data size does not match value size!");
		}
		// check this vector
		auto &vector =
		    result->collection.chunks[chunk_index]->data[column_number];
		if (i + vector.count > values.size()) {
			vector.Print();
			// too many values in this vector
			FAIL("Too many values in result!");
		}
		for (size_t j = 0; j < vector.count; j++) {
			// NULL <> NULL, hence special handling
			if (vector.GetValue(j).is_null && values[i + j].is_null) {
				continue;
			}
			if (!duckdb::Value::Equals(vector.GetValue(j), values[i + j])) {
				FAIL("Incorrect result! Got " + vector.GetValue(j).ToString() +
				     " but expected " + values[i + j].ToString());
			}
		}
		chunk_index++;
		i += vector.count;
	}
}

static void RESULT_NO_ERROR(std::unique_ptr<duckdb::DuckDBResult> &result) {
	if (!result->GetSuccess()) {
		FAIL(result->GetErrorMessage().c_str());
	}
}

static std::string compare_csv(std::unique_ptr<duckdb::DuckDBResult> &result,
                               std::string csv, bool header = false) {
	if (!result->GetSuccess()) {
		fprintf(stderr, "Query failed with message: %s\n",
		        result->GetErrorMessage().c_str());
		return result->GetErrorMessage().c_str();
	}
	std::string error;
	if (!compare_result(csv, result->collection, header, error)) {
		return error;
	}
	return "";
}

#define COMPARE_CSV(result, csv, header)                                       \
	{                                                                          \
		auto res = compare_csv(result, csv, header);                           \
		if (!res.empty())                                                      \
			FAIL(res);                                                         \
	}

} // namespace duckdb
