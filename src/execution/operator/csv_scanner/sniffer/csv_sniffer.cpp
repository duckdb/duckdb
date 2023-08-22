#include "duckdb/execution/operator/persistent/csv_scanner/csv_sniffer.hpp"

namespace duckdb {

CSVSniffer::CSVSniffer(CSVReaderOptions &options_p, shared_ptr<CSVBufferManager> buffer_manager_p,
                       const vector<LogicalType> &requested_types_p)
    : requested_types(requested_types_p), options(options_p), buffer_manager(std::move(buffer_manager_p)) {
	// Check if any type is BLOB
	for (auto &type : requested_types) {
		if (type.id() == LogicalTypeId::BLOB) {
			throw InvalidInputException(
			    "CSV auto-detect for blobs not supported: there may be invalid UTF-8 in the file");
		}
	}

	// Initialize Format Candidates
	for (const auto &t : format_template_candidates) {
		best_format_candidates[t.first].clear();
	}
}

SnifferResult CSVSniffer::SniffCSV() {
	// 1. Dialect Detection
	DetectDialect();
	// 2. Type Detection
	DetectTypes();
	// 3. Header Detection
	DetectHeader();
	D_ASSERT(best_sql_types_candidates_per_column_idx.size() == names.size());
	// 4. Type Replacement
	ReplaceTypes();
	// 5. Type Refinement
	RefineTypes();
	// We are done, construct and return the result.
	// Set the CSV Options in the reference
	options.dialect_options = best_candidate->dialect_options;
	options.has_header = best_candidate->dialect_options.header;
	options.skip_rows_set = options.dialect_options.skip_rows > 0;

	// Return the types and names
	return SnifferResult(detected_types, names);
}

} // namespace duckdb
