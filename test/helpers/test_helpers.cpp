// #define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "compare_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/main/client_context.hpp"

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
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	if (fs->DirectoryExists(path)) {
		fs->RemoveDirectory(path);
	}
}

void TestDeleteFile(string path) {
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	if (fs->FileExists(path)) {
		fs->RemoveFile(path);
	}
}

void TestChangeDirectory(string path) {
	// set the base path for the tests
	FileSystem::SetWorkingDirectory(path);
}

void DeleteDatabase(string path) {
	TestDeleteFile(path);
	TestDeleteFile(path + ".wal");
}

void TestCreateDirectory(string path) {
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	fs->CreateDirectory(path);
}

string TestDirectoryPath() {
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	if (!fs->DirectoryExists(TESTING_DIRECTORY_NAME)) {
		fs->CreateDirectory(TESTING_DIRECTORY_NAME);
	}
	return TESTING_DIRECTORY_NAME;
}

string TestCreatePath(string suffix) {
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	return fs->JoinPath(TestDirectoryPath(), suffix);
}

bool TestIsInternalError(unordered_set<string> &internal_error_messages, const string &error) {
	for (auto &error_message : internal_error_messages) {
		if (StringUtil::Contains(error, error_message)) {
			return true;
		}
	}
	return false;
}

unique_ptr<DBConfig> GetTestConfig() {
	auto result = make_unique<DBConfig>();
	result->options.checkpoint_wal_size = 0;
	result->options.allow_unsigned_extensions = true;
	return result;
}

string GetCSVPath() {
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	string csv_path = TestCreatePath("csv_files");
	if (fs->DirectoryExists(csv_path)) {
		fs->RemoveDirectory(csv_path);
	}
	fs->CreateDirectory(csv_path);
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
	if (result_.type == QueryResultType::STREAM_RESULT) {
		fprintf(stderr, "Unexpected stream query result in CHECK_COLUMN\n");
		return false;
	}
	auto &result = (MaterializedQueryResult &)result_;
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
		if (result.collection.Count() != 0) {
			result.Print();
			return false;
		} else {
			return true;
		}
	}
	if (result.collection.Count() == 0) {
		result.Print();
		return false;
	}
	if (column_number >= result.types.size()) {
		result.Print();
		return false;
	}
	size_t chunk_index = 0;
	for (size_t i = 0; i < values.size();) {
		if (chunk_index >= result.collection.ChunkCount()) {
			// ran out of chunks
			result.Print();
			return false;
		}
		// check this vector
		auto &chunk = result.collection.GetChunk(chunk_index);
		auto &vector = chunk.data[column_number];
		if (i + chunk.size() > values.size()) {
			// too many values in this vector
			result.Print();
			return false;
		}
		for (size_t j = 0; j < chunk.size(); j++) {
			// NULL <> NULL, hence special handling
			if (vector.GetValue(j).IsNull() && values[i + j].IsNull()) {
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
	if (result->type == QueryResultType::STREAM_RESULT) {
		auto &stream = (StreamQueryResult &)*result;
		result = stream.Materialize();
	}
	return CHECK_COLUMN(*result, column_number, values);
}

bool CHECK_COLUMN(unique_ptr<duckdb::MaterializedQueryResult> &result, size_t column_number,
                  vector<duckdb::Value> values) {
	return CHECK_COLUMN((QueryResult &)*result, column_number, values);
}

string compare_csv(duckdb::QueryResult &result, string csv, bool header) {
	D_ASSERT(result.type == QueryResultType::MATERIALIZED_RESULT);
	auto &materialized = (MaterializedQueryResult &)result;
	if (!materialized.success) {
		fprintf(stderr, "Query failed with message: %s\n", materialized.error.c_str());
		return materialized.error;
	}
	string error;
	if (!compare_result(csv, materialized.collection, materialized.types, header, error)) {
		return error;
	}
	return "";
}

string show_diff(DataChunk &left, DataChunk &right) {
	if (left.ColumnCount() != right.ColumnCount()) {
		return StringUtil::Format("Different column counts: %d vs %d", (int)left.ColumnCount(),
		                          (int)right.ColumnCount());
	}
	if (left.size() != right.size()) {
		return StringUtil::Format("Different sizes: %zu vs %zu", left.size(), right.size());
	}
	string difference;
	for (size_t i = 0; i < left.ColumnCount(); i++) {
		bool has_differences = false;
		auto &left_vector = left.data[i];
		auto &right_vector = right.data[i];
		string left_column = StringUtil::Format("Result\n------\n%s [", left_vector.GetType().ToString().c_str());
		string right_column = StringUtil::Format("Expect\n------\n%s [", right_vector.GetType().ToString().c_str());
		if (left_vector.GetType() == right_vector.GetType()) {
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
	if (left.ColumnCount() != right.ColumnCount()) {
		return false;
	}
	if (left.size() != right.size()) {
		return false;
	}
	for (size_t i = 0; i < left.ColumnCount(); i++) {
		auto &left_vector = left.data[i];
		auto &right_vector = right.data[i];
		if (left_vector.GetType() == right_vector.GetType()) {
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
bool compare_result(string csv, ChunkCollection &collection, vector<LogicalType> sql_types, bool has_header,
                    string &error_message) {
	D_ASSERT(collection.Count() == 0 || collection.Types().size() == sql_types.size());

	// create the csv on disk
	auto csv_path = TestCreatePath("__test_csv_path.csv");
	ofstream f(csv_path);
	f << csv;
	f.close();

	// set up the CSV reader
	BufferedCSVReaderOptions options;
	options.auto_detect = false;
	options.delimiter = "|";
	options.header = has_header;
	options.quote = "\"";
	options.escape = "\"";
	options.file_path = csv_path;

	// set up the intermediate result chunk
	DataChunk parsed_result;
	parsed_result.Initialize(Allocator::DefaultAllocator(), sql_types);

	DuckDB db;
	Connection con(db);
	BufferedCSVReader reader(*con.context, move(options), sql_types);
	idx_t collection_index = 0;
	idx_t tuple_count = 0;
	while (true) {
		// parse a chunk from the CSV file
		try {
			parsed_result.Reset();
			reader.ParseCSV(parsed_result);
		} catch (std::exception &ex) {
			error_message = "Could not parse CSV: " + string(ex.what());
			return false;
		}
		if (parsed_result.size() == 0) {
			// out of tuples in CSV file
			if (collection_index < collection.ChunkCount()) {
				error_message = StringUtil::Format("Too many tuples in result! Found %llu tuples, but expected %llu",
				                                   collection.Count(), tuple_count);
				return false;
			}
			return true;
		}
		if (collection_index >= collection.ChunkCount()) {
			// ran out of chunks in the collection, but there are still tuples in the result
			// keep parsing the csv file to get the total expected count
			while (parsed_result.size() > 0) {
				tuple_count += parsed_result.size();
				parsed_result.Reset();
				reader.ParseCSV(parsed_result);
			}
			error_message = StringUtil::Format("Too few tuples in result! Found %llu tuples, but expected %llu",
			                                   collection.Count(), tuple_count);
			return false;
		}
		// same counts, compare tuples in chunks
		if (!compare_chunk(collection.GetChunk(collection_index), parsed_result)) {
			error_message = show_diff(collection.GetChunk(collection_index), parsed_result);
			return false;
		}

		collection_index++;
		tuple_count += parsed_result.size();
	}
}
} // namespace duckdb
