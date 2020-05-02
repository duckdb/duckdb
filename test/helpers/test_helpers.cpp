// #define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "compare_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

#include <cmath>
#include <fstream>

using namespace std;

#define TESTING_DIRECTORY_NAME "duckdb_unittest_tempdir"

namespace duckdb {

bool NO_FAIL(QueryResult &result) {
	if (!result.success) {
		fprintf(stderr, "Query failed with message: %s\n", result.error.c_str());
	}
	return result.success;
}

bool NO_FAIL(unique_ptr<QueryResult> result) {
	return NO_FAIL(*result);
}

void TestDeleteDirectory(string path) {
	FileSystem fs;
	if (fs.DirectoryExists(path)) {
		fs.RemoveDirectory(path);
	}
}

void TestDeleteFile(string path) {
	FileSystem fs;
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

void DeleteDatabase(string path) {
	TestDeleteFile(path);
	TestDeleteFile(path + ".wal");
}

void TestCreateDirectory(string path) {
	FileSystem fs;
	fs.CreateDirectory(path);
}

string TestCreatePath(string suffix) {
	FileSystem fs;
	if (!fs.DirectoryExists(TESTING_DIRECTORY_NAME)) {
		fs.CreateDirectory(TESTING_DIRECTORY_NAME);
	}
	return fs.JoinPath(TESTING_DIRECTORY_NAME, suffix);
}

unique_ptr<DBConfig> GetTestConfig() {
	auto result = make_unique<DBConfig>();
	result->checkpoint_wal_size = 0;
	return result;
}

string GetCSVPath() {
	FileSystem fs;
	string csv_path = TestCreatePath("csv_files");
	if (fs.DirectoryExists(csv_path)) {
		fs.RemoveDirectory(csv_path);
	}
	fs.CreateDirectory(csv_path);
	return csv_path;
}

void WriteCSV(string path, const char *csv) {
	ofstream csv_writer(path);
	csv_writer << csv;
	csv_writer.close();
}

void WriteBinary(string path, const uint8_t *data, uint64_t length) {
	ofstream binary_writer(path, ios::binary);
	binary_writer.write((const char *)data, length);
	binary_writer.close();
}

bool CHECK_COLUMN(QueryResult &result_, size_t column_number, vector<duckdb::Value> values) {
	unique_ptr<MaterializedQueryResult> materialized;
	if (result_.type == QueryResultType::STREAM_RESULT) {
		materialized = ((StreamQueryResult &)result_).Materialize();
	}
	auto &result = materialized ? *materialized : (MaterializedQueryResult &)result_;
	if (!result.success) {
		fprintf(stderr, "Query failed with message: %s\n", result.error.c_str());
		return false;
	}
	if (!(result.names.size() == result.types.size())) {
		// column names do not match
		result.Print();
		return false;
	}
	if (values.size() == 0) {
		if (result.collection.count != 0) {
			result.Print();
			return false;
		} else {
			return true;
		}
	}
	if (result.collection.count == 0) {
		result.Print();
		return false;
	}
	if (column_number >= result.types.size()) {
		result.Print();
		return false;
	}
	size_t chunk_index = 0;
	for (size_t i = 0; i < values.size();) {
		if (chunk_index >= result.collection.chunks.size()) {
			// ran out of chunks
			result.Print();
			return false;
		}
		// check this vector
		auto &chunk = *result.collection.chunks[chunk_index];
		auto &vector = chunk.data[column_number];
		if (i + chunk.size() > values.size()) {
			// too many values in this vector
			result.Print();
			return false;
		}
		for (size_t j = 0; j < chunk.size(); j++) {
			// NULL <> NULL, hence special handling
			if (vector.GetValue(j).is_null && values[i + j].is_null) {
				continue;
			}

			if (!Value::ValuesAreEqual(vector.GetValue(j), values[i + j])) {
				// FAIL("Incorrect result! Got " + vector.GetValue(j).ToString()
				// +
				//      " but expected " + values[i + j].ToString());
				result.Print();
				return false;
			}
		}
		chunk_index++;
		i += chunk.size();
	}
	return true;
}

bool CHECK_COLUMN(unique_ptr<duckdb::QueryResult> &result, size_t column_number, vector<duckdb::Value> values) {
	return CHECK_COLUMN(*result, column_number, values);
}

bool CHECK_COLUMN(unique_ptr<duckdb::MaterializedQueryResult> &result, size_t column_number,
                  vector<duckdb::Value> values) {
	return CHECK_COLUMN((QueryResult &)*result, column_number, values);
}

string compare_csv(duckdb::QueryResult &result, string csv, bool header) {
	assert(result.type == QueryResultType::MATERIALIZED_RESULT);
	auto &materialized = (MaterializedQueryResult &)result;
	if (!materialized.success) {
		fprintf(stderr, "Query failed with message: %s\n", materialized.error.c_str());
		return materialized.error;
	}
	string error;
	if (!compare_result(csv, materialized.collection, materialized.sql_types, header, error)) {
		return error;
	}
	return "";
}

string show_diff(DataChunk &left, DataChunk &right) {
	if (left.column_count() != right.column_count()) {
		return StringUtil::Format("Different column counts: %d vs %d", (int)left.column_count(),
		                          (int)right.column_count());
	}
	if (left.size() != right.size()) {
		return StringUtil::Format("Different sizes: %zu vs %zu", left.size(), right.size());
	}
	string difference;
	for (size_t i = 0; i < left.column_count(); i++) {
		bool has_differences = false;
		auto &left_vector = left.data[i];
		auto &right_vector = right.data[i];
		string left_column = StringUtil::Format("Result\n------\n%s [", TypeIdToString(left_vector.type).c_str());
		string right_column = StringUtil::Format("Expect\n------\n%s [", TypeIdToString(right_vector.type).c_str());
		if (left_vector.type == right_vector.type) {
			for (size_t j = 0; j < left.size(); j++) {
				auto left_value = left_vector.GetValue(j);
				auto right_value = right_vector.GetValue(j);
				if (!Value::ValuesAreEqual(left_value, right_value)) {
					left_column += left_value.ToString() + ",";
					right_column += right_value.ToString() + ",";
					has_differences = true;
				} else {
					left_column += "_,";
					right_column += "_,";
				}
			}
		} else {
			left_column += "...";
			right_column += "...";
		}
		left_column += "]\n";
		right_column += "]\n";
		if (has_differences) {
			difference += StringUtil::Format("Difference in column %d:\n", i);
			difference += left_column + "\n" + right_column + "\n";
		}
	}
	return difference;
}

bool compare_chunk(DataChunk &left, DataChunk &right) {
	if (left.column_count() != right.column_count()) {
		return false;
	}
	if (left.size() != right.size()) {
		return false;
	}
	for (size_t i = 0; i < left.column_count(); i++) {
		auto &left_vector = left.data[i];
		auto &right_vector = right.data[i];
		if (left_vector.type == right_vector.type) {
			for (size_t j = 0; j < left.size(); j++) {
				auto left_value = left_vector.GetValue(j);
				auto right_value = right_vector.GetValue(j);
				if (!Value::ValuesAreEqual(left_value, right_value)) {
					return false;
				}
			}
		}
	}
	return true;
}

//! Compares the result of a pipe-delimited CSV with the given DataChunk
//! Returns true if they are equal, and stores an error_message otherwise
bool compare_result(string csv, ChunkCollection &collection, vector<SQLType> sql_types, bool has_header,
                    string &error_message) {
	assert(collection.count == 0 || collection.types.size() == sql_types.size());

	// set up the CSV reader
	CopyInfo info;
	info.delimiter = "|";
	info.header = true;
	info.quote = "\"";
	info.escape = "\"";
	// set up the intermediate result chunk
	vector<TypeId> internal_types;
	for (auto &type : sql_types) {
		internal_types.push_back(GetInternalType(type));
	}
	DataChunk parsed_result;
	parsed_result.Initialize(internal_types);

	// convert the CSV string into a stringstream
	auto source = make_unique<istringstream>(csv);

	BufferedCSVReader reader(info, sql_types, move(source));
	idx_t collection_index = 0;
	idx_t tuple_count = 0;
	while (true) {
		// parse a chunk from the CSV file
		try {
			parsed_result.Reset();
			reader.ParseCSV(parsed_result);
		} catch (Exception &ex) {
			error_message = "Could not parse CSV: " + string(ex.what());
			return false;
		}
		if (parsed_result.size() == 0) {
			// out of tuples in CSV file
			if (collection_index < collection.chunks.size()) {
				error_message = StringUtil::Format("Too many tuples in result! Found %llu tuples, but expected %llu",
				                                   collection.count, tuple_count);
				return false;
			}
			return true;
		}
		if (collection_index >= collection.chunks.size()) {
			// ran out of chunks in the collection, but there are still tuples in the result
			// keep parsing the csv file to get the total expected count
			while (parsed_result.size() > 0) {
				tuple_count += parsed_result.size();
				parsed_result.Reset();
				reader.ParseCSV(parsed_result);
			}
			error_message = StringUtil::Format("Too few tuples in result! Found %llu tuples, but expected %llu",
			                                   collection.count, tuple_count);
			return false;
		}
		// same counts, compare tuples in chunks
		if (!compare_chunk(*collection.chunks[collection_index], parsed_result)) {
			error_message = show_diff(*collection.chunks[collection_index], parsed_result);
		}

		collection_index++;
		tuple_count += parsed_result.size();
	}
}
} // namespace duckdb
