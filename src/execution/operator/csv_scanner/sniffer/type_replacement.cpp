#include "duckdb/execution/operator/persistent/csv_scanner/csv_sniffer.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/buffered_csv_reader.hpp"

namespace duckdb {
void CSVSniffer::ReplaceTypes() {
	if (!best_candidate->options.sql_type_list.empty()) {
		// user-defined types were supplied for certain columns
		// override the types
		if (!best_candidate->options.sql_types_per_column.empty()) {
			// types supplied as name -> value map
			idx_t found = 0;
			for (idx_t i = 0; i < names.size(); i++) {
				auto it = best_candidate->options.sql_types_per_column.find(names[i]);
				if (it != best_candidate->options.sql_types_per_column.end()) {
					best_sql_types_candidates_per_column_idx[i] = {best_candidate->options.sql_type_list[it->second]};
					found++;
					continue;
				}
			}
			if (!best_candidate->options.file_options.union_by_name &&
			    found < best_candidate->options.sql_types_per_column.size()) {
				string exception = BufferedCSVReader::ColumnTypesError(options.sql_types_per_column, names);
				if (!exception.empty()) {
					throw BinderException(exception);
				}
			}
		} else {
			// types supplied as list
			if (names.size() < best_candidate->options.sql_type_list.size()) {
				throw BinderException("read_csv: %d types were provided, but CSV file only has %d columns",
				                      best_candidate->options.sql_type_list.size(), names.size());
			}
			for (idx_t i = 0; i < best_candidate->options.sql_type_list.size(); i++) {
				best_sql_types_candidates_per_column_idx[i] = {best_candidate->options.sql_type_list[i]};
			}
		}
	}
}
} // namespace duckdb
