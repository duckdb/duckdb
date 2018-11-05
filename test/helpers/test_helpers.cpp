
#include "common/value_operations/value_operations.hpp"
#include "compare_result.hpp"

using namespace std;

namespace duckdb {

bool CHECK_COLUMN(unique_ptr<duckdb::DuckDBResult> &result,
                  size_t column_number, vector<duckdb::Value> values) {
	if (!result->GetSuccess()) {
		fprintf(stderr, "Query failed with message: %s\n",
		        result->GetErrorMessage().c_str());
		// FAIL(result->GetErrorMessage().c_str());
		return false;
	}
	if (!(result->names.size() == result->collection.types.size())) {
		// column names do not match
		result->Print();
		return false;
	}
	if (values.size() == 0) {
		if (result->size() != 0) {
			result->Print();
			// FAIL("Data size does not match value size!");
			return false;
		} else {
			return true;
		}
	}
	if (result->size() == 0) {
		// FAIL("Data size does not match value size!");
		result->Print();
		return false;
	}
	if (column_number >= result->column_count()) {
		// FAIL("Column number out of range of result!");
		result->Print();
		return false;
	}
	size_t chunk_index = 0;
	for (size_t i = 0; i < values.size();) {
		if (chunk_index >= result->size()) {
			// ran out of chunks
			result->Print();
			return false;
			// FAIL("Data size does not match value size!");
		}
		// check this vector
		auto &vector =
		    result->collection.chunks[chunk_index]->data[column_number];
		if (i + vector.count > values.size()) {
			// too many values in this vector
			// FAIL("Too many values in result!");
			result->Print();
			return false;
		}
		for (size_t j = 0; j < vector.count; j++) {
			// NULL <> NULL, hence special handling
			if (vector.GetValue(j).is_null && values[i + j].is_null) {
				continue;
			}
			if (!ValueOperations::Equals(vector.GetValue(j), values[i + j])) {
				// FAIL("Incorrect result! Got " + vector.GetValue(j).ToString()
				// +
				//      " but expected " + values[i + j].ToString());
				result->Print();
				return false;
			}
		}
		chunk_index++;
		i += vector.count;
	}
	return true;
}

string compare_csv(duckdb::DuckDBResult &result, string csv, bool header) {
	if (!result.GetSuccess()) {
		fprintf(stderr, "Query failed with message: %s\n",
		        result.GetErrorMessage().c_str());
		return result.GetErrorMessage().c_str();
	}
	string error;
	if (!compare_result(csv, result.collection, header, error)) {
		return error;
	}
	return "";
}

bool parse_datachunk(string csv, DataChunk &result, bool has_header) {
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
				value = value.CastAs(result.data[i].type);
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

static bool ValuesAreEqual(Value result_value, Value value) {
	if (result_value.is_null && value.is_null) {
		// NULL = NULL in checking code
		return true;
	}
	if (value.type == TypeId::DECIMAL) {
		// round to two decimals
		auto left = StringUtil::Format("%.2f", value.value_.decimal);
		auto right = StringUtil::Format("%.2f", result_value.value_.decimal);
		if (left != right) {
			double ldecimal = value.value_.decimal;
			double rdecimal = result_value.value_.decimal;
			if (ldecimal < 0.99 * rdecimal || ldecimal > 1.01 * rdecimal) {
				return false;
			}
		}
	} else if (value.type == TypeId::VARCHAR) {
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
	} else {
		if (!ValueOperations::Equals(value, result_value)) {
			return false;
		}
	}
	return true;
}

string show_diff(DataChunk &left, DataChunk &right) {
	if (left.column_count != right.column_count) {
		return StringUtil::Format("Different column counts: %d vs %d",
		                          (int)left.column_count,
		                          (int)right.column_count);
	}
	if (left.size() != right.size()) {
		return StringUtil::Format("Different sizes: %zu vs %zu", left.size(),
		                          right.size());
	}
	string difference;
	for (size_t i = 0; i < left.column_count; i++) {
		bool has_differences = false;
		auto &left_vector = left.data[i];
		auto &right_vector = right.data[i];
		string left_column = StringUtil::Format(
		    "Result\n------\n%s [", TypeIdToString(left_vector.type).c_str());
		string right_column = StringUtil::Format(
		    "Expect\n------\n%s [", TypeIdToString(right_vector.type).c_str());
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
	return show_diff(*collection.chunks[0], chunk);
}

//! Compares the result of a pipe-delimited CSV with the given DataChunk
//! Returns true if they are equal, and stores an error_message otherwise
bool compare_result(string csv, ChunkCollection &collection, bool has_header,
                    string &error_message) {
	auto types = collection.types;
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
		if (split.size() != types.size()) {
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
		if (split.size() != types.size()) {
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
				value = value.CastAs(types[i]);
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
	correct_result.Initialize(types);
	if (!parse_datachunk(csv, correct_result, has_header)) {
		error_message = "Incorrect answer for query!\nProvided answer:\n" +
		                collection.ToString() +
		                "\nExpected answer [could not parse]:\n" + string(csv) +
		                "\n";
	} else {
		error_message = "Incorrect answer for query!\nProvided answer:\n" +
		                collection.ToString() + "\nExpected answer:\n" +
		                correct_result.ToString() + "\n" +
		                show_diff(collection, correct_result);
	}
	return false;
}
} // namespace duckdb
