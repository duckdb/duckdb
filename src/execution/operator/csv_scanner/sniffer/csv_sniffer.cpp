#include <utility>

#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"

namespace duckdb {

CSVSniffer::CSVSniffer(CSVReaderOptions &options_p, shared_ptr<CSVBufferManager> buffer_manager_p,
                       CSVStateMachineCache &state_machine_cache_p, SetColumns set_columns_p)
    : state_machine_cache(state_machine_cache_p), options(options_p), buffer_manager(std::move(buffer_manager_p)),
      set_columns(set_columns_p) {
	// Initialize Format Candidates
	for (const auto &format_template : format_template_candidates) {
		auto &logical_type = format_template.first;
		best_format_candidates[logical_type].clear();
	}
	// Initialize max columns found to either 0 or however many were set
	max_columns_found = set_columns.Size();
}

bool SetColumns::IsSet() {
	if (!types) {
		return false;
	}
	return !types->empty();
}

idx_t SetColumns::Size() {
	if (!types) {
		return 0;
	}
	return types->size();
}

template <class T>
void MatchAndReplace(CSVOption<T> &original, CSVOption<T> &sniffed, string name) {
	if (original.IsSetByUser()) {
		// We verify that the user input matches the sniffed value
		if (original != sniffed) {
			throw InvalidInputException("CSV Sniffer: Sniffer detected value different than the user input for the %s "
			                            "options \n Set: %s Sniffed: %s",
			                            std::move(name), original.FormatValue(), sniffed.FormatValue());
		}
	} else {
		// We replace the value of original with the sniffed value
		original.Set(sniffed.GetValue(), false);
	}
}
void MatchAndRepaceUserSetVariables(DialectOptions &original, DialectOptions &sniffed) {
	MatchAndReplace(original.header, sniffed.header, "Header");
	if (sniffed.new_line.GetValue() != NewLineIdentifier::NOT_SET) {
		// Is sniffed line is not set (e.g., single-line file) , we don't try to replace and match.
		MatchAndReplace(original.new_line, sniffed.new_line, "New Line");
	}
	MatchAndReplace(original.skip_rows, sniffed.skip_rows, "Skip Rows");
	MatchAndReplace(original.state_machine_options.delimiter, sniffed.state_machine_options.delimiter, "Delimiter");
	MatchAndReplace(original.state_machine_options.quote, sniffed.state_machine_options.quote, "Quote");
	MatchAndReplace(original.state_machine_options.escape, sniffed.state_machine_options.escape, "Escape");
	MatchAndReplace(original.date_format[LogicalTypeId::DATE], sniffed.date_format[LogicalTypeId::DATE], "Date Format");
	MatchAndReplace(original.date_format[LogicalTypeId::TIMESTAMP], sniffed.date_format[LogicalTypeId::TIMESTAMP],
	                "Timestamp Format");
}
// Set the CSV Options in the reference
void CSVSniffer::SetResultOptions() {
	MatchAndRepaceUserSetVariables(options.dialect_options, best_candidate->dialect_options);
	if (options.dialect_options.header.GetValue()) {
		options.dialect_options.true_start = best_start_with_header;
	} else {
		options.dialect_options.true_start = best_start_without_header;
	}
}

SnifferResult CSVSniffer::SniffCSV() {
	// 1. Dialect Detection
	DetectDialect();
	// 2. Type Detection
	DetectTypes();
	// 3. Type Refinement
	RefineTypes();
	// 4. Header Detection
	DetectHeader();
	// 5. Type Replacement
	ReplaceTypes();
	D_ASSERT(best_sql_types_candidates_per_column_idx.size() == names.size());
	// We are done, Set the CSV Options in the reference. Construct and return the result.
	SetResultOptions();
	if (set_columns.IsSet()) {
		// Columns and their types were set, let's validate they match
		if (options.dialect_options.header.GetValue()) {
			// If the header exists it should match
			bool match = true;
			string error = "The Column names set by the user do not match the ones found by the sniffer. \n";
			auto &set_names = *set_columns.names;
			for (idx_t i = 0; i < set_columns.Size(); i++) {
				if (set_names[i] != names[i]) {
					error += "Column at position: " + to_string(i) + " Set name: " + set_names[i] +
					         " Sniffed Name: " + names[i] + "\n";
					match = false;
				}
			}
			error += "If you are sure about the column names, disable the sniffer with auto_detect = false.";
			if (!match) {
				throw InvalidInputException(error);
			}
		}

		// Check if types match
		bool match = true;
		string error = "The Column types set by the user do not match the ones found by the sniffer. \n";
		auto &set_types = *set_columns.types;
		for (idx_t i = 0; i < set_columns.Size(); i++) {
			if (set_types[i] != detected_types[i] && !(set_types[i].IsNumeric() && detected_types[i].IsNumeric())) {
				if (set_types[i].ToString() == "JSON" && detected_types[i] == LogicalType::VARCHAR) {
					// Skip check if it's json
					continue;
				}
				if ((set_types[i].id() == LogicalTypeId::ENUM || set_types[i].id() == LogicalTypeId::LIST ||
				     set_types[i].id() == LogicalTypeId::STRUCT) &&
				    detected_types[i] == LogicalType::VARCHAR) {
					// Skip check if it's an ENUM
					continue;
				}
				error += "Column at position: " + to_string(i) + " Set type: " + set_types[i].ToString() +
				         " Sniffed type: " + detected_types[i].ToString() + "\n";
				match = false;
			}
		}
		error += "If you are sure about the column types, disable the sniffer with auto_detect = false.";
		if (!match) {
			options.sniffer_user_mismatch_error = error;
		}
		// We do not need to run type refinement, since the types have been given by the user
		return SnifferResult({}, {});
	}
	return SnifferResult(detected_types, names);
}

} // namespace duckdb
