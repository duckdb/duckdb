#include "duckdb.hpp"

#include "common/string_util.hpp"

namespace duckdb {

bool CHECK_COLUMN(std::unique_ptr<duckdb::DuckDBResult> &result,
                         size_t column_number,
                         std::vector<duckdb::Value> values);


std::string compare_csv(std::unique_ptr<duckdb::DuckDBResult> &result,
                               std::string csv, bool header = false);

bool parse_datachunk(std::string csv, DataChunk &result,
                            bool has_header);

//! Compares the result of a pipe-delimited CSV with the given DataChunk
//! Returns true if they are equal, and stores an error_message otherwise
bool compare_result(std::string csv, ChunkCollection &collection,
                           bool has_header, std::string &error_message);

} // namespace duckdb
