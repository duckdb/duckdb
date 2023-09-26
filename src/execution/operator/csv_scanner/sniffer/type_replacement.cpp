#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"

namespace duckdb {
void CSVSniffer::ReplaceTypes() {
	auto &sniffing_state_machine = best_candidate->GetStateMachineSniff();
	if (sniffing_state_machine.options.sql_type_list.empty()) {
		return;
	}
	// user-defined types were supplied for certain columns
	// override the types
	if (!sniffing_state_machine.options.sql_types_per_column.empty()) {
		// types supplied as name -> value map
		idx_t found = 0;
		for (idx_t i = 0; i < names.size(); i++) {
			auto it = sniffing_state_machine.options.sql_types_per_column.find(names[i]);
			if (it != sniffing_state_machine.options.sql_types_per_column.end()) {
				best_sql_types_candidates_per_column_idx[i] = {
				    sniffing_state_machine.options.sql_type_list[it->second]};
				found++;
			}
		}
		if (!sniffing_state_machine.options.file_options.union_by_name &&
		    found < sniffing_state_machine.options.sql_types_per_column.size()) {
			string error_msg = CSVScanner::ColumnTypesError(options.sql_types_per_column, names);
			if (!error_msg.empty()) {
				throw BinderException(error_msg);
			}
		}
		return;
	}
	// types supplied as list
	if (names.size() < sniffing_state_machine.options.sql_type_list.size()) {
		throw BinderException("read_csv: %d types were provided, but CSV file only has %d columns",
		                      sniffing_state_machine.options.sql_type_list.size(), names.size());
	}
	for (idx_t i = 0; i < sniffing_state_machine.options.sql_type_list.size(); i++) {
		best_sql_types_candidates_per_column_idx[i] = {sniffing_state_machine.options.sql_type_list[i]};
	}
}
} // namespace duckdb
