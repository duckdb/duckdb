
#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/execution/operator/persistent/parallel_csv_reader.hpp"
#endif

#include <algorithm>
#include <unordered_map>

/* COMPILATION:
python3 scripts/amalgamation.py --extended
clang++ -std=c++11 -O0 -g -Isrc/amalgamation examples/csvcli/csvcli.cpp src/amalgamation/duckdb.cpp
./a.out test/sql/copy/csv/data/real/lineitem_sample.csv
*/

using namespace duckdb;
void PrintUsage() {
	printf("Usage: csvcli [filename.csv]");
	exit(1);
}

int main(int argc, const char **argv) {
	if (argc < 2) {
		PrintUsage();
	}
	auto filename = std::string(argv[1]);

	BufferedCSVReaderOptions options;
	options.file_path = std::move(filename);
	options.compression = "none";
	options.auto_detect = true;

	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	BufferedCSVReader reader(*fs, std::move(options));

	if (reader.return_types.empty()) {
		throw std::runtime_error("Failed to auto-detect types for CSV file");
	}

	DataChunk result;
	result.Initialize(reader.return_types);

	while (true) {
		result.Reset();
		reader.ParseCSV(result);
		if (result.size() == 0) {
			break;
		}
		result.Print();
	}
}
