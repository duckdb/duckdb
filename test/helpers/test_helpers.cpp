
// #define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "common/file_system.hpp"
#include "common/value_operations/value_operations.hpp"
#include "compare_result.hpp"
#include "main/query_result.hpp"
#include "test_helpers.hpp"

#include <cmath>

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
	return fs.JoinPath(TESTING_DIRECTORY_NAME, suffix);
}

bool ApproxEqual(float ldecimal, float rdecimal) {
	float epsilon = fabs(rdecimal) * 0.01;
	return fabs(ldecimal - rdecimal) <= epsilon;
}

bool ApproxEqual(double ldecimal, double rdecimal) {
	double epsilon = fabs(rdecimal) * 0.01;
	return fabs(ldecimal - rdecimal) <= epsilon;
}

static bool ValuesAreEqual(Value result_value, Value value) {
	if (result_value.is_null && value.is_null) {
		// NULL = NULL in checking code
		return true;
	}
	switch (value.type) {
	case TypeId::FLOAT: {
		float ldecimal = value.value_.float_;
		float rdecimal = result_value.value_.float_;
		if (!ApproxEqual(ldecimal, rdecimal)) {
			return false;
		}
		break;
	}
	case TypeId::DOUBLE: {
		double ldecimal = value.value_.double_;
		double rdecimal = result_value.value_.double_;
		if (!ApproxEqual(ldecimal, rdecimal)) {
			return false;
		}
		break;
	}
	case TypeId::VARCHAR: {
		// some results might contain padding spaces, e.g. when rendering
		// VARCHAR(10) and the string only has 6 characters, they will be padded
		// with spaces to 10 in the rendering. We don't do that here yet as we
		// are looking at internal structures. So just ignore any extra spaces
		// on the right
		string left = result_value.str_value;
		string right = value.str_value;
		StringUtil::RTrim(left);
		StringUtil::RTrim(right);
		return left == right;
	}
	default:
		if (value != result_value) {
			return false;
		}
		break;
	}
	return true;
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
		auto &vector = result.collection.chunks[chunk_index]->data[column_number];
		if (i + vector.count > values.size()) {
			// too many values in this vector
			result.Print();
			return false;
		}
		for (size_t j = 0; j < vector.count; j++) {
			// NULL <> NULL, hence special handling
			if (vector.GetValue(j).is_null && values[i + j].is_null) {
				continue;
			}

			if (!ValuesAreEqual(vector.GetValue(j), values[i + j])) {
				// FAIL("Incorrect result! Got " + vector.GetValue(j).ToString()
				// +
				//      " but expected " + values[i + j].ToString());
				result.Print();
				return false;
			}
		}
		chunk_index++;
		i += vector.count;
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

bool parse_datachunk(string csv, DataChunk &result, vector<SQLType> sql_types, bool has_header) {
	istringstream f(csv);
	string line;

	size_t row = 0;
	while (getline(f, line)) {
		auto split = StringUtil::Split(line, '|');

		if (has_header) {
			if (split.size() != result.column_count) {
				// column length is different
				return false;
			}
			has_header = false;
			continue;
		}

		if (line.back() == '|') {
			split.push_back("");
		}
		if (split.size() != result.column_count) {
			// column length is different
			return false;
		}
		if (row >= STANDARD_VECTOR_SIZE) {
			// chunk is full, but parsing so far was successful
			// return the partially parsed chunk
			// this function is used for printing, and we probably don't want to
			// print >1000 rows anyway
			return true;
		}
		// now compare the values
		for (size_t i = 0; i < split.size(); i++) {
			// first create a string value
			Value value(split[i]);
			value.is_null = split[i].empty();

			// cast to the type of the column
			try {
				value = value.CastAs(SQLType(SQLTypeId::VARCHAR), sql_types[i]);
			} catch (...) {
				return false;
			}
			// now perform a comparison
			result.data[i].count++;
			result.data[i].SetValue(row, value);
		}
		row++;
	}
	return true;
}

string show_diff(DataChunk &left, DataChunk &right) {
	if (left.column_count != right.column_count) {
		return StringUtil::Format("Different column counts: %d vs %d", (int)left.column_count, (int)right.column_count);
	}
	if (left.size() != right.size()) {
		return StringUtil::Format("Different sizes: %zu vs %zu", left.size(), right.size());
	}
	string difference;
	for (size_t i = 0; i < left.column_count; i++) {
		bool has_differences = false;
		auto &left_vector = left.data[i];
		auto &right_vector = right.data[i];
		string left_column = StringUtil::Format("Result\n------\n%s [", TypeIdToString(left_vector.type).c_str());
		string right_column = StringUtil::Format("Expect\n------\n%s [", TypeIdToString(right_vector.type).c_str());
		if (left_vector.type == right_vector.type) {
			for (size_t j = 0; j < left_vector.count; j++) {
				auto left_value = left_vector.GetValue(j);
				auto right_value = right_vector.GetValue(j);
				if (!ValuesAreEqual(left_value, right_value)) {
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

string show_diff(ChunkCollection &collection, DataChunk &chunk) {
	if (collection.chunks.size() == 0) {
		return "<EMPTY RESULT>";
	}
	return show_diff(*collection.chunks[0], chunk);
}

//! Compares the result of a pipe-delimited CSV with the given DataChunk
//! Returns true if they are equal, and stores an error_message otherwise
bool compare_result(string csv, ChunkCollection &collection, vector<SQLType> sql_types, bool has_header,
                    string &error_message) {
	assert(collection.types.size() == sql_types.size());
	DataChunk correct_result;

	istringstream f(csv);
	string line;
	size_t row = 0;
	/// read and parse the header line
	if (has_header) {
		getline(f, line);
		// check if the column length matches
		auto split = StringUtil::Split(line, '|');
		if (line.back() == '|') {
			split.push_back("");
		}
		if (split.size() != sql_types.size()) {
			// column length is different
			goto incorrect;
		}
	}
	// now compare the actual data
	while (getline(f, line)) {
		if (collection.count <= row) {
			goto incorrect;
		}
		auto split = StringUtil::Split(line, '|');
		if (line.back() == '|') {
			split.push_back("");
		}
		if (split.size() != sql_types.size()) {
			// column length is different
			goto incorrect;
		}
		// now compare the values
		for (size_t i = 0; i < split.size(); i++) {
			// first create a string value
			Value value(split[i]);
			value.is_null = split[i].empty();

			// cast to the type of the column
			try {
				value = value.CastAs(SQLType(SQLTypeId::VARCHAR), sql_types[i]);
			} catch (...) {
				goto incorrect;
			}
			// now perform a comparison
			Value result_value = collection.GetValue(i, row);
			if (!ValuesAreEqual(result_value, value)) {
				goto incorrect;
			}
		}
		row++;
	}
	if (collection.count != row) {
		goto incorrect;
	}
	return true;
incorrect:
	correct_result.Initialize(collection.types);
	if (!parse_datachunk(csv, correct_result, sql_types, has_header)) {
		error_message = "Incorrect answer for query!\nProvided answer:\n" + collection.ToString() +
		                "\nExpected answer [could not parse]:\n" + string(csv) + "\n";
	} else {
		error_message = "Incorrect answer for query!\nProvided answer:\n" + collection.ToString() +
		                "\nExpected answer:\n" + correct_result.ToString() + "\n" +
		                show_diff(collection, correct_result);
	}
	return false;
}
} // namespace duckdb
