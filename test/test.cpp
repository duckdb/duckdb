
#include "duckdb.hpp"

#include "test_helpers.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

bool file_exists(string file) {
	ifstream f(file.c_str());
	return f.good();
}

void copy_file(string input, string output) {
	std::ifstream src(input, std::ios::binary);
	std::ofstream dst(output, std::ios::binary);

	dst << src.rdbuf();
}

int main() {
	unique_ptr<DuckDBResult> result = nullptr;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	string input_file = "test/test.sql";
	if (!file_exists(input_file)) {
		// copy from the .in file
		copy_file("test/test.sql.in", input_file);
	}

	ifstream infile(input_file);
	string line;
	size_t linenr = 0;
	size_t queryline = 0;
	string csv = "";
	string error_message;
	while (getline(infile, line)) {
		linenr++;
		if (line.substr(0, 2) == "Q:") {
			// query
			result = con.Query(line.substr(2));
			queryline = linenr;
		} else {
			continue;
		}
		while (getline(infile, line)) {
			linenr++;
			// create the data chunk
			if (line.substr(0, 2) == "Q:") {
				// query, compare previous results
				if (!csv.empty()) {
					DataChunk big_chunk;
					result->GatherResult(big_chunk);
					if (!compare_result(csv.c_str(), big_chunk, false,
					                    error_message)) {
						fprintf(stderr, "Failed at query on line %zu: %s\n",
						        queryline, error_message.c_str());
						return 1;
					}
				}
				// run the query
				result = con.Query(line.substr(2));
				queryline = linenr;
				csv = "";
			} else if (line.empty() && csv.empty()) {
				csv = "";
				break;
			} else {
				csv += line + "\n";
			}
		}
	}
	// final comparison
	if (!csv.empty() && result) {
		DataChunk big_chunk;
		result->GatherResult(big_chunk);
		if (!compare_result(csv.c_str(), big_chunk, false, error_message)) {
			fprintf(stderr, "Failed at query on line %zu: %s\n", queryline,
			        error_message.c_str());
			return 1;
		}
	}
	fprintf(stderr, "Passed all tests!\n");
}