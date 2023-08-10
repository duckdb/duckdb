#include "duckdb/execution/operator/persistent/csv_scanner/csv_sniffer.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/base_csv_reader.hpp"
namespace duckdb {
struct Parse {
	inline static void Initialize(CSVStateMachine &machine) {
		machine.state = CSVState::STANDARD;
		machine.previous_state = CSVState::STANDARD;
		machine.pre_previous_state = CSVState::STANDARD;

		machine.cur_rows = 0;
		machine.column_count = 0;
		machine.value = "";
	}

	inline static bool Process(CSVStateMachine &machine, DataChunk &parse_chunk, char current_char) {
		machine.pre_previous_state = machine.previous_state;
		machine.previous_state = machine.state;
		machine.state = static_cast<CSVState>(
		    machine.transition_array[static_cast<uint8_t>(machine.state)][static_cast<uint8_t>(current_char)]);
		bool empty_line =
		    (machine.state == CSVState::CARRIAGE_RETURN && machine.previous_state == CSVState::CARRIAGE_RETURN) ||
		    (machine.state == CSVState::RECORD_SEPARATOR && machine.previous_state == CSVState::RECORD_SEPARATOR) ||
		    (machine.state == CSVState::CARRIAGE_RETURN && machine.previous_state == CSVState::RECORD_SEPARATOR) ||
		    (machine.pre_previous_state == CSVState::RECORD_SEPARATOR &&
		     machine.previous_state == CSVState::CARRIAGE_RETURN);

		bool carriage_return = machine.previous_state == CSVState::CARRIAGE_RETURN;
		if (machine.previous_state == CSVState::FIELD_SEPARATOR ||
		    (machine.previous_state == CSVState::RECORD_SEPARATOR && !empty_line) ||
		    (machine.state != CSVState::RECORD_SEPARATOR && carriage_return)) {
			// Started a new value
			// Check if it's UTF-8 (Or not?)
			machine.VerifyUTF8();
			auto &v = parse_chunk.data[machine.column_count++];
			auto parse_data = FlatVector::GetData<string_t>(v);
			auto &validity_mask = FlatVector::Validity(v);
			if (machine.value.empty()) {
				validity_mask.SetInvalid(machine.cur_rows);
			} else {
				parse_data[machine.cur_rows] = StringVector::AddStringOrBlob(v, string_t(machine.value));
			}
			machine.value = "";
		}
		if (((machine.previous_state == CSVState::RECORD_SEPARATOR && !empty_line) ||
		     (machine.state != CSVState::RECORD_SEPARATOR && carriage_return)) &&
		    machine.options.null_padding && machine.column_count < parse_chunk.ColumnCount()) {
			// It's a new row, check if we need to pad stuff
			while (machine.column_count < parse_chunk.ColumnCount()) {
				auto &v = parse_chunk.data[machine.column_count++];
				auto &validity_mask = FlatVector::Validity(v);
				validity_mask.SetInvalid(machine.cur_rows);
			}
		}
		if (machine.state == CSVState::STANDARD) {
			machine.value += current_char;
		}
		machine.cur_rows += machine.previous_state == CSVState::RECORD_SEPARATOR && !empty_line;
		machine.column_count -= machine.column_count * (machine.previous_state == CSVState::RECORD_SEPARATOR);

		// It means our carriage return is actually a record separator
		machine.cur_rows += machine.state != CSVState::RECORD_SEPARATOR && carriage_return;
		machine.column_count -= machine.column_count * (machine.state != CSVState::RECORD_SEPARATOR && carriage_return);

		if (machine.cur_rows >= machine.options.sample_chunk_size) {
			// We sniffed enough rows
			return false;
		}
		return true;
	}

	inline static void Finalize(CSVStateMachine &machine, DataChunk &parse_chunk) {
		bool empty_line =
		    (machine.state == CSVState::CARRIAGE_RETURN && machine.previous_state == CSVState::CARRIAGE_RETURN) ||
		    (machine.state == CSVState::RECORD_SEPARATOR && machine.previous_state == CSVState::RECORD_SEPARATOR) ||
		    (machine.state == CSVState::CARRIAGE_RETURN && machine.previous_state == CSVState::RECORD_SEPARATOR) ||
		    (machine.pre_previous_state == CSVState::RECORD_SEPARATOR &&
		     machine.previous_state == CSVState::CARRIAGE_RETURN);
		if (machine.cur_rows < machine.options.sample_chunk_size && !empty_line) {
			machine.VerifyUTF8();
			auto &v = parse_chunk.data[machine.column_count++];
			auto parse_data = FlatVector::GetData<string_t>(v);
			parse_data[machine.cur_rows] = StringVector::AddStringOrBlob(v, string_t(machine.value));
		}
		parse_chunk.SetCardinality(machine.cur_rows);
	}
};

bool CSVSniffer::TryCastVector(Vector &parse_chunk_col, idx_t size, const LogicalType &sql_type) {
	// try vector-cast from string to sql_type
	Vector dummy_result(sql_type);
	if (best_candidate->has_format[LogicalTypeId::DATE] && sql_type == LogicalTypeId::DATE) {
		// use the date format to cast the chunk
		string error_message;
		idx_t line_error;
		return BaseCSVReader::TryCastDateVector(best_candidate->date_format, parse_chunk_col, dummy_result, size,
		                                        error_message, line_error);
	} else if (best_candidate->has_format[LogicalTypeId::TIMESTAMP] && sql_type == LogicalTypeId::TIMESTAMP) {
		// use the timestamp format to cast the chunk
		string error_message;
		return BaseCSVReader::TryCastTimestampVector(best_candidate->date_format, parse_chunk_col, dummy_result, size,
		                                             error_message);
	} else {
		// target type is not varchar: perform a cast
		string error_message;
		return VectorOperations::DefaultTryCast(parse_chunk_col, dummy_result, size, &error_message, true);
	}
}

void CSVSniffer::RefineTypes() {
	// if data types were provided, exit here if number of columns does not match
	detected_types.assign(best_candidate->num_cols, LogicalType::VARCHAR);
	if (!requested_types.empty()) {
		if (requested_types.size() != best_candidate->num_cols) {
			throw InvalidInputException(
			    "Error while determining column types: found %lld columns but expected %d. (%s)",
			    best_candidate->num_cols, requested_types.size(), best_candidate->options.ToString());
		} else {
			detected_types = requested_types;
		}
	} else if (best_candidate->options.all_varchar) {
		// return all types varchar
		return;
	} else {
		DataChunk parse_chunk;
		parse_chunk.Initialize(BufferAllocator::Get(buffer_manager->context), detected_types,
		                       options.sample_chunk_size);
		// FIXME: We are doing this sequentially, but we could do this by jumping samples.
		for (idx_t i = 1; i < best_candidate->options.sample_chunks; i++) {
			bool finished_file = best_candidate->csv_buffer_iterator.Finished();
			if (finished_file) {
				// we finished the file: stop
				// set sql types
				detected_types.clear();
				for (auto &best_sql_types_candidate : best_sql_types_candidates) {
					LogicalType d_type = best_sql_types_candidate.back();
					if (best_sql_types_candidate.size() == best_candidate->options.auto_type_candidates.size()) {
						d_type = LogicalType::VARCHAR;
					}
					detected_types.push_back(d_type);
				}
				return;
			}
			best_candidate->csv_buffer_iterator.Process<Parse>(*best_candidate, parse_chunk);
			for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
				vector<LogicalType> &col_type_candidates = best_sql_types_candidates[col];
				while (col_type_candidates.size() > 1) {
					const auto &sql_type = col_type_candidates.back();
					//	narrow down the date formats
					if (best_format_candidates.count(sql_type.id())) {
						auto &best_type_format_candidates = best_format_candidates[sql_type.id()];
						auto save_format_candidates = best_type_format_candidates;
						while (!best_type_format_candidates.empty()) {
							if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
								break;
							}
							//	doesn't work - move to the next one
							best_type_format_candidates.pop_back();
							best_candidate->has_format[sql_type.id()] = (!best_type_format_candidates.empty());
							if (!best_type_format_candidates.empty()) {
								SetDateFormat(*best_candidate, best_type_format_candidates.back(), sql_type.id());
							}
						}
						//	if none match, then this is not a column of type sql_type,
						if (best_type_format_candidates.empty()) {
							//	so restore the candidates that did work.
							best_type_format_candidates.swap(save_format_candidates);
							if (!best_type_format_candidates.empty()) {
								SetDateFormat(*best_candidate, best_type_format_candidates.back(), sql_type.id());
							}
						}
					}
					if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
						break;
					} else {
						col_type_candidates.pop_back();
					}
				}
			}
			// reset parse chunk for the next iteration
			parse_chunk.Reset();
		}
		detected_types.clear();
	}
	// set sql types
	for (auto &best_sql_types_candidate : best_sql_types_candidates) {
		LogicalType d_type = best_sql_types_candidate.back();
		if (best_sql_types_candidate.size() == best_candidate->options.auto_type_candidates.size()) {
			d_type = LogicalType::VARCHAR;
		}
		detected_types.push_back(d_type);
	}
}
} // namespace duckdb
