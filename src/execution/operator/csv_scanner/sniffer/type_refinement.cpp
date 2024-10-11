#include "duckdb/execution/operator/csv_scanner/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_casting.hpp"

namespace duckdb {
bool CSVSniffer::TryCastVector(Vector &parse_chunk_col, idx_t size, const LogicalType &sql_type) {
	auto &sniffing_state_machine = best_candidate->GetStateMachine();
	// try vector-cast from string to sql_type
	Vector dummy_result(sql_type, size);
	if (!sniffing_state_machine.dialect_options.date_format[LogicalTypeId::DATE].GetValue().Empty() &&
	    sql_type.id() == LogicalTypeId::DATE) {
		// use the date format to cast the chunk
		string error_message;
		CastParameters parameters(false, &error_message);
		idx_t line_error;
		return CSVCast::TryCastDateVector(sniffing_state_machine.dialect_options.date_format, parse_chunk_col,
		                                  dummy_result, size, parameters, line_error);
	}
	if (!sniffing_state_machine.dialect_options.date_format[LogicalTypeId::TIMESTAMP].GetValue().Empty() &&
	    sql_type.id() == LogicalTypeId::TIMESTAMP) {
		// use the timestamp format to cast the chunk
		string error_message;
		CastParameters parameters(false, &error_message);
		return CSVCast::TryCastTimestampVector(sniffing_state_machine.dialect_options.date_format, parse_chunk_col,
		                                       dummy_result, size, parameters);
	}
	if ((sql_type.id() == LogicalTypeId::DOUBLE || sql_type.id() == LogicalTypeId::FLOAT) &&
	    options.decimal_separator == ",") {
		string error_message;
		CastParameters parameters(false, &error_message);
		idx_t line_error;
		return CSVCast::TryCastFloatingVectorCommaSeparated(options, parse_chunk_col, dummy_result, size, parameters,
		                                                    sql_type, line_error);
	}
	if (sql_type.id() == LogicalTypeId::DECIMAL && options.decimal_separator == ",") {
		string error_message;
		CastParameters parameters(false, &error_message);
		idx_t line_error;
		return CSVCast::TryCastDecimalVectorCommaSeparated(options, parse_chunk_col, dummy_result, size, parameters,
		                                                   sql_type, line_error);
	}
	// target type is not varchar: perform a cast
	string error_message;
	return VectorOperations::DefaultTryCast(parse_chunk_col, dummy_result, size, &error_message, true);
}

void CSVSniffer::RefineTypes() {
	auto &sniffing_state_machine = best_candidate->GetStateMachine();
	// if data types were provided, exit here if number of columns does not match
	detected_types.assign(sniffing_state_machine.dialect_options.num_cols, LogicalType::VARCHAR);
	if (sniffing_state_machine.options.all_varchar) {
		// return all types varchar
		return;
	}
	for (idx_t i = 1; i < sniffing_state_machine.options.sample_size_chunks; i++) {
		bool finished_file = best_candidate->FinishedFile();
		if (finished_file) {
			// we finished the file: stop
			// set sql types
			detected_types.clear();
			for (idx_t column_idx = 0; column_idx < best_sql_types_candidates_per_column_idx.size(); column_idx++) {
				LogicalType d_type = best_sql_types_candidates_per_column_idx[column_idx].back();
				if (best_sql_types_candidates_per_column_idx[column_idx].size() ==
				    sniffing_state_machine.options.auto_type_candidates.size()) {
					d_type = LogicalType::VARCHAR;
				}
				detected_types.push_back(d_type);
			}
			return;
		}
		auto &parse_chunk = best_candidate->ParseChunk().ToChunk();

		for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
			vector<LogicalType> &col_type_candidates = best_sql_types_candidates_per_column_idx[col];
			bool is_bool_type = col_type_candidates.back() == LogicalType::BOOLEAN;
			while (col_type_candidates.size() > 1) {
				const auto &sql_type = col_type_candidates.back();
				if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
					break;
				}
				if (col_type_candidates.back() == LogicalType::BOOLEAN && is_bool_type) {
					// If we thought this was a boolean value (i.e., T,F, True, False) and it is not, we
					// immediately pop to varchar.
					while (col_type_candidates.back() != LogicalType::VARCHAR) {
						col_type_candidates.pop_back();
					}
					break;
				}
				col_type_candidates.pop_back();
			}
		}
		// reset parse chunk for the next iteration
		parse_chunk.Reset();
		parse_chunk.SetCapacity(CSVReaderOptions::sniff_size);
	}
	detected_types.clear();
	// set sql types
	for (idx_t column_idx = 0; column_idx < best_sql_types_candidates_per_column_idx.size(); column_idx++) {
		LogicalType d_type = best_sql_types_candidates_per_column_idx[column_idx].back();
		if (best_sql_types_candidates_per_column_idx[column_idx].size() ==
		        best_candidate->GetStateMachine().options.auto_type_candidates.size() &&
		    default_null_to_varchar) {
			d_type = LogicalType::VARCHAR;
		}
		detected_types.push_back(d_type);
	}
}
} // namespace duckdb
