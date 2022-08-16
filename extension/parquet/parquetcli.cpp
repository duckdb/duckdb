
#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "parquet_reader.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#else
#include "parquet-amalgamation.hpp"
#endif

#include <algorithm>
#include <unordered_map>

/* COMPILATION:
python3 scripts/amalgamation.py --extended
python3 scripts/parquet_amalgamation.py
clang++ -std=c++11 -Isrc/amalgamation src/amalgamation/parquet-amalgamation.cpp src/amalgamation/duckdb.cpp
extension/parquet/parquetcli.cpp
./a.out data/parquet-testing/zstd.parquet --all two=bar
*/

using namespace duckdb;
void PrintUsage() {
	printf("Usage: parquetcli [filename.parquet] [column,names,to,read] ([x=y])");
	exit(1);
}

int main(int argc, const char **argv) {
	if (argc < 3) {
		PrintUsage();
	}
	auto filename = std::string(argv[1]);

	bool read_all_columns = false;
	if (string(argv[2]) == "--all") {
		read_all_columns = true;
	}
	auto column_names = StringUtil::Split(argv[2], ",");

	// the db instance and client context are not really required so we may remove them

	Allocator allocator;
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	ParquetReader reader(allocator, fs->OpenFile(filename, FileFlags::FILE_FLAGS_READ));

	// only return columns first_name and last_name
	std::vector<column_t> column_ids;
	std::vector<LogicalType> return_types;
	std::unordered_map<std::string, int> name_map;
	for (idx_t col_idx = 0; col_idx < reader.names.size(); col_idx++) {
		auto &colname = reader.names[col_idx];
		if (read_all_columns || std::find(column_names.begin(), column_names.end(), colname) != column_names.end()) {
			printf("%s\n", colname.c_str());
			name_map[colname] = column_ids.size();
			column_ids.push_back(col_idx);
			return_types.push_back(reader.return_types[col_idx]);
		}
	}
	if (column_ids.empty()) {
		printf("No columns found that matched the filter. Alternatively, use --all to read all columns");
		PrintUsage();
	}

	// read all row groups
	std::vector<idx_t> groups;
	for (idx_t i = 0; i < reader.NumRowGroups(); i++) {
		groups.push_back(i);
	}

	// apply filter if any are specified
	TableFilterSet filters;
	if (argc >= 4) {
		auto splits = StringUtil::Split(argv[3], "=");
		auto entry = name_map.find(splits[0]);
		if (entry == name_map.end()) {
			printf("Invalid filter: name not found");
			PrintUsage();
		}
		auto idx = entry->second;
		auto filter =
		    make_unique<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value(splits[1]).CastAs(return_types[idx]));
		filters.filters[idx] = move(filter);
	}

	ParquetReaderScanState state;
	// nullptr here gets the filters
	reader.InitializeScan(state, column_ids, groups, &filters);
	DataChunk output;

	output.Initialize(allocator, return_types);
	do {
		output.Reset();
		reader.Scan(state, output);
		output.Print();
	} while (output.size() > 0);
}
