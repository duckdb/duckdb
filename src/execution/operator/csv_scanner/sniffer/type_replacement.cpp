#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"

namespace duckdb {
void CSVSniffer::ReplaceTypes() {
	auto &sniffing_state_machine = best_candidate->GetStateMachine();
	manually_set = vector<bool>(detected_types.size(), false);
	if (sniffing_state_machine.options.sql_type_list.empty() || sniffing_state_machine.options.columns_set) {
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
				detected_types[i] = sniffing_state_machine.options.sql_type_list[it->second];
				manually_set[i] = true;
				found++;
			}
		}
		if (!sniffing_state_machine.options.file_options.union_by_name &&
		    found < sniffing_state_machine.options.sql_types_per_column.size()) {
			auto error_msg = CSVError::ColumnTypesError(options.sql_types_per_column, names);
			error_handler->Error(error_msg);
		}
		return;
	}
	// types supplied as list
	if (names.size() < sniffing_state_machine.options.sql_type_list.size()) {
		throw BinderException("read_csv: %d types were provided, but CSV file only has %d columns",
		                      sniffing_state_machine.options.sql_type_list.size(), names.size());
	}
	for (idx_t i = 0; i < sniffing_state_machine.options.sql_type_list.size(); i++) {
		detected_types[i] = sniffing_state_machine.options.sql_type_list[i];
		manually_set[i] = true;
	}
}
} // namespace duckdb
