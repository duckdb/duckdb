
#include "duckdb.hpp"

#include "test_helpers.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

int main() {
	unique_ptr<DuckDBResult> result = nullptr;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	string input_file = "test/test.sql";
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
					if (!compare_result(csv.c_str(), result->collection, false,
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
		if (!compare_result(csv.c_str(), result->collection, false,
		                    error_message)) {
			fprintf(stderr, "Failed at query on line %zu: %s\n", queryline,
			        error_message.c_str());
			return 1;
		}
	}
	fprintf(stderr, "Passed all tests!\n");
}