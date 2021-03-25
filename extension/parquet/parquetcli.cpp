#include "parquet_reader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/table_filter.hpp"

using namespace duckdb;
int main(int argc, const char** argv) {
    auto filename = std::string(argv[1]);

	// the db instance and client context are not really required so we may remove them
	DuckDB db;
	Connection conn(db);

	ParquetReader reader(*conn.context, filename, {}, "");

	// only return columns first_name and last_name
    std::vector<column_t> column_ids;
	std::vector<LogicalType> return_types;
	for (idx_t col_idx = 0; col_idx < reader.names.size(); col_idx++) {
		auto& colname = reader.names[col_idx];
		if (colname == "first_name" || colname == "last_name" || colname == "country") {
			column_ids.push_back(col_idx);
			return_types.push_back(reader.return_types[col_idx]);
		}
	}

	// read all row groups
	std::vector<idx_t> groups;
	for (idx_t i = 0; i < reader.NumRowGroups(); i++) {
        groups.push_back(i);
	}

	// filter so we only return rows where country (column 8) == China
	TableFilterSet filters;
	TableFilter filter(Value("China"), ExpressionType::COMPARE_EQUAL, 2);
	filters.filters[2].push_back(filter);

	ParquetReaderScanState state;
	// nullptr here gets the filters
    reader.Initialize(state, column_ids, groups, &filters);
	DataChunk output;

	output.Initialize(return_types);
    do {
		output.Reset();
		reader.Scan(state, output);
		output.Print();
	} while (output.size() > 0);
}