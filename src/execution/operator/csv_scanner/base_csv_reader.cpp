#include "duckdb/execution/operator/scan/csv/base_csv_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "utf8proc_wrapper.hpp"
#include "utf8proc.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/execution/operator/scan/csv/parallel_csv_reader.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/main/client_data.hpp"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>

namespace duckdb {

string BaseCSVReader::GetLineNumberStr(idx_t line_error, bool is_line_estimated, idx_t buffer_idx) {
	// If an error happens during auto-detect it is an estimated line
	string estimated = (is_line_estimated ? string(" (estimated)") : string(""));
	return to_string(GetLineError(line_error, buffer_idx)) + estimated;
}

BaseCSVReader::BaseCSVReader(ClientContext &context_p, CSVReaderOptions options_p,
                             const vector<LogicalType> &requested_types)
    : context(context_p), fs(FileSystem::GetFileSystem(context)), allocator(BufferAllocator::Get(context)),
      options(std::move(options_p)) {
}

BaseCSVReader::~BaseCSVReader() {
}

unique_ptr<CSVFileHandle> BaseCSVReader::OpenCSV(ClientContext &context, const CSVReaderOptions &options_p) {
	return CSVFileHandle::OpenFile(FileSystem::GetFileSystem(context), BufferAllocator::Get(context),
	                               options_p.file_path, options_p.compression);
}

void BaseCSVReader::InitParseChunk(idx_t num_cols) {
	// adapt not null info
	if (options.force_not_null.size() != num_cols) {
		options.force_not_null.resize(num_cols, false);
	}
	if (num_cols == parse_chunk.ColumnCount()) {
		parse_chunk.Reset();
	} else {
		parse_chunk.Destroy();

		// initialize the parse_chunk with a set of VARCHAR types
		vector<LogicalType> varchar_types(num_cols, LogicalType::VARCHAR);
		parse_chunk.Initialize(allocator, varchar_types);
	}
}

void BaseCSVReader::InitializeProjection() {
	for (idx_t i = 0; i < GetTypes().size(); i++) {
		reader_data.column_ids.push_back(i);
		reader_data.column_mapping.push_back(i);
	}
}

template <class OP, class T>
static bool TemplatedTryCastDateVector(map<LogicalTypeId, StrpTimeFormat> &options, Vector &input_vector,
                                       Vector &result_vector, idx_t count, string &error_message, idx_t &line_error) {
	D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
	bool all_converted = true;
	idx_t cur_line = 0;
	UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
		T result;
		if (!OP::Operation(options, input, result, error_message)) {
			line_error = cur_line;
			all_converted = false;
		}
		cur_line++;
		return result;
	});
	return all_converted;
}

struct TryCastDateOperator {
	static bool Operation(map<LogicalTypeId, StrpTimeFormat> &options, string_t input, date_t &result,
	                      string &error_message) {
		return options[LogicalTypeId::DATE].TryParseDate(input, result, error_message);
	}
};

struct TryCastTimestampOperator {
	static bool Operation(map<LogicalTypeId, StrpTimeFormat> &options, string_t input, timestamp_t &result,
	                      string &error_message) {
		return options[LogicalTypeId::TIMESTAMP].TryParseTimestamp(input, result, error_message);
	}
};

bool BaseCSVReader::TryCastDateVector(map<LogicalTypeId, StrpTimeFormat> &options, Vector &input_vector,
                                      Vector &result_vector, idx_t count, string &error_message, idx_t &line_error) {
	return TemplatedTryCastDateVector<TryCastDateOperator, date_t>(options, input_vector, result_vector, count,
	                                                               error_message, line_error);
}

bool BaseCSVReader::TryCastTimestampVector(map<LogicalTypeId, StrpTimeFormat> &options, Vector &input_vector,
                                           Vector &result_vector, idx_t count, string &error_message) {
	idx_t line_error;
	return TemplatedTryCastDateVector<TryCastTimestampOperator, timestamp_t>(options, input_vector, result_vector,
	                                                                         count, error_message, line_error);
}

void BaseCSVReader::VerifyLineLength(idx_t line_size, idx_t buffer_idx) {
	if (line_size > options.maximum_line_size) {
		throw InvalidInputException(
		    "Error in file \"%s\" on line %s: Maximum line size of %llu bytes exceeded!", options.file_path,
		    GetLineNumberStr(parse_chunk.size(), linenr_estimated, buffer_idx).c_str(), options.maximum_line_size);
	}
}

template <class OP, class T>
bool TemplatedTryCastFloatingVector(CSVReaderOptions &options, Vector &input_vector, Vector &result_vector, idx_t count,
                                    string &error_message, idx_t &line_error) {
	D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
	bool all_converted = true;
	idx_t row = 0;
	UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
		T result;
		if (!OP::Operation(input, result, &error_message)) {
			line_error = row;
			all_converted = false;
		} else {
			row++;
		}
		return result;
	});
	return all_converted;
}

template <class OP, class T>
bool TemplatedTryCastDecimalVector(CSVReaderOptions &options, Vector &input_vector, Vector &result_vector, idx_t count,
                                   string &error_message, uint8_t width, uint8_t scale) {
	D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
	bool all_converted = true;
	UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
		T result;
		if (!OP::Operation(input, result, &error_message, width, scale)) {
			all_converted = false;
		}
		return result;
	});
	return all_converted;
}

void BaseCSVReader::AddValue(string_t str_val, idx_t &column, vector<idx_t> &escape_positions, bool has_quotes,
                             idx_t buffer_idx) {
	auto length = str_val.GetSize();
	if (length == 0 && column == 0) {
		row_empty = true;
	} else {
		row_empty = false;
	}
	if (!return_types.empty() && column == return_types.size() && length == 0) {
		// skip a single trailing delimiter in last column
		return;
	}
	if (column >= return_types.size()) {
		if (options.ignore_errors) {
			error_column_overflow = true;
			return;
		} else {
			throw InvalidInputException(
			    "Error in file \"%s\", on line %s: expected %lld values per row, but got more. (%s)", options.file_path,
			    GetLineNumberStr(linenr, linenr_estimated, buffer_idx).c_str(), return_types.size(),
			    options.ToString());
		}
	}

	// insert the line number into the chunk
	idx_t row_entry = parse_chunk.size();

	// test against null string, but only if the value was not quoted
	if ((!(has_quotes && !options.allow_quoted_nulls) || return_types[column].id() != LogicalTypeId::VARCHAR) &&
	    !options.force_not_null[column] && Equals::Operation(str_val, string_t(options.null_str))) {
		FlatVector::SetNull(parse_chunk.data[column], row_entry, true);
	} else {
		auto &v = parse_chunk.data[column];
		auto parse_data = FlatVector::GetData<string_t>(v);
		if (!escape_positions.empty()) {
			// remove escape characters (if any)
			string old_val = str_val.GetString();
			string new_val = "";
			idx_t prev_pos = 0;
			for (idx_t i = 0; i < escape_positions.size(); i++) {
				idx_t next_pos = escape_positions[i];
				new_val += old_val.substr(prev_pos, next_pos - prev_pos);
				prev_pos = ++next_pos;
			}
			new_val += old_val.substr(prev_pos, old_val.size() - prev_pos);
			escape_positions.clear();
			parse_data[row_entry] = StringVector::AddStringOrBlob(v, string_t(new_val));
		} else {
			parse_data[row_entry] = str_val;
		}
	}

	// move to the next column
	column++;
}

bool BaseCSVReader::AddRow(DataChunk &insert_chunk, idx_t &column, string &error_message, idx_t buffer_idx) {
	linenr++;

	if (row_empty) {
		row_empty = false;
		if (return_types.size() != 1) {
			if (mode == ParserMode::PARSING) {
				FlatVector::SetNull(parse_chunk.data[0], parse_chunk.size(), false);
			}
			column = 0;
			return false;
		}
	}

	// Error forwarded by 'ignore_errors' - originally encountered in 'AddValue'
	if (error_column_overflow) {
		D_ASSERT(options.ignore_errors);
		error_column_overflow = false;
		column = 0;
		return false;
	}

	if (column < return_types.size()) {
		if (options.null_padding) {
			for (; column < return_types.size(); column++) {
				FlatVector::SetNull(parse_chunk.data[column], parse_chunk.size(), true);
			}
		} else if (options.ignore_errors) {
			column = 0;
			return false;
		} else {
			if (mode == ParserMode::SNIFFING_DATATYPES) {
				error_message = "Error when adding line";
				return false;
			} else {
				throw InvalidInputException(
				    "Error in file \"%s\" on line %s: expected %lld values per row, but got %d.\nParser options:\n%s",
				    options.file_path, GetLineNumberStr(linenr, linenr_estimated, buffer_idx).c_str(),
				    return_types.size(), column, options.ToString());
			}
		}
	}

	parse_chunk.SetCardinality(parse_chunk.size() + 1);

	if (mode == ParserMode::PARSING_HEADER) {
		return true;
	}

	if (mode == ParserMode::SNIFFING_DATATYPES && parse_chunk.size() == options.sample_chunk_size) {
		return true;
	}

	if (mode == ParserMode::PARSING && parse_chunk.size() == STANDARD_VECTOR_SIZE) {
		Flush(insert_chunk, buffer_idx);
		return true;
	}

	column = 0;
	return false;
}

void BaseCSVReader::VerifyUTF8(idx_t col_idx, idx_t row_idx, DataChunk &chunk, int64_t offset) {
	D_ASSERT(col_idx < chunk.data.size());
	D_ASSERT(row_idx < chunk.size());
	auto &v = chunk.data[col_idx];
	if (FlatVector::IsNull(v, row_idx)) {
		return;
	}

	auto parse_data = FlatVector::GetData<string_t>(chunk.data[col_idx]);
	auto s = parse_data[row_idx];
	auto utf_type = Utf8Proc::Analyze(s.GetData(), s.GetSize());
	if (utf_type == UnicodeType::INVALID) {
		string col_name = to_string(col_idx);
		if (col_idx < names.size()) {
			col_name = "\"" + names[col_idx] + "\"";
		}
		int64_t error_line = linenr - (chunk.size() - row_idx) + 1 + offset;
		D_ASSERT(error_line >= 0);
		throw InvalidInputException("Error in file \"%s\" at line %llu in column \"%s\": "
		                            "%s. Parser options:\n%s",
		                            options.file_path, error_line, col_name,
		                            ErrorManager::InvalidUnicodeError(s.GetString(), "CSV file"), options.ToString());
	}
}

void BaseCSVReader::VerifyUTF8(idx_t col_idx) {
	D_ASSERT(col_idx < parse_chunk.data.size());
	for (idx_t i = 0; i < parse_chunk.size(); i++) {
		VerifyUTF8(col_idx, i, parse_chunk);
	}
}

bool TryCastDecimalVectorCommaSeparated(CSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                        idx_t count, string &error_message, const LogicalType &result_type) {
	auto width = DecimalType::GetWidth(result_type);
	auto scale = DecimalType::GetScale(result_type);
	switch (result_type.InternalType()) {
	case PhysicalType::INT16:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int16_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
	case PhysicalType::INT32:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int32_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
	case PhysicalType::INT64:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int64_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
	case PhysicalType::INT128:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, hugeint_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
	default:
		throw InternalException("Unimplemented physical type for decimal");
	}
}

bool TryCastFloatingVectorCommaSeparated(CSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                         idx_t count, string &error_message, const LogicalType &result_type,
                                         idx_t &line_error) {
	switch (result_type.InternalType()) {
	case PhysicalType::DOUBLE:
		return TemplatedTryCastFloatingVector<TryCastErrorMessageCommaSeparated, double>(
		    options, input_vector, result_vector, count, error_message, line_error);
	case PhysicalType::FLOAT:
		return TemplatedTryCastFloatingVector<TryCastErrorMessageCommaSeparated, float>(
		    options, input_vector, result_vector, count, error_message, line_error);
	default:
		throw InternalException("Unimplemented physical type for floating");
	}
}

// Location of erroneous value in the current parse chunk
struct ErrorLocation {
	idx_t row_idx;
	idx_t col_idx;
	idx_t row_line;

	ErrorLocation(idx_t row_idx, idx_t col_idx, idx_t row_line)
	    : row_idx(row_idx), col_idx(col_idx), row_line(row_line) {
	}
};

bool BaseCSVReader::Flush(DataChunk &insert_chunk, idx_t buffer_idx, bool try_add_line) {
	if (parse_chunk.size() == 0) {
		return true;
	}

	bool conversion_error_ignored = false;

	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);
	if (reader_data.column_ids.empty() && !reader_data.empty_columns) {
		throw InternalException("BaseCSVReader::Flush called on a CSV reader that was not correctly initialized. Call "
		                        "MultiFileReader::InitializeReader or InitializeProjection");
	}
	D_ASSERT(reader_data.column_ids.size() == reader_data.column_mapping.size());
	for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
		auto col_idx = reader_data.column_ids[c];
		auto result_idx = reader_data.column_mapping[c];
		auto &parse_vector = parse_chunk.data[col_idx];
		auto &result_vector = insert_chunk.data[result_idx];
		auto &type = result_vector.GetType();
		if (type.id() == LogicalTypeId::VARCHAR) {
			// target type is varchar: no need to convert
			// just test that all strings are valid utf-8 strings
			VerifyUTF8(col_idx);
			// reinterpret rather than reference so we can deal with user-defined types
			result_vector.Reinterpret(parse_vector);
		} else {
			string error_message;
			bool success;
			idx_t line_error = 0;
			bool target_type_not_varchar = false;
			if (options.dialect_options.has_format[LogicalTypeId::DATE] && type.id() == LogicalTypeId::DATE) {
				// use the date format to cast the chunk
				success = TryCastDateVector(options.dialect_options.date_format, parse_vector, result_vector,
				                            parse_chunk.size(), error_message, line_error);
			} else if (options.dialect_options.has_format[LogicalTypeId::TIMESTAMP] &&
			           type.id() == LogicalTypeId::TIMESTAMP) {
				// use the date format to cast the chunk
				success = TryCastTimestampVector(options.dialect_options.date_format, parse_vector, result_vector,
				                                 parse_chunk.size(), error_message);
			} else if (options.decimal_separator != "." &&
			           (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE)) {
				success = TryCastFloatingVectorCommaSeparated(options, parse_vector, result_vector, parse_chunk.size(),
				                                              error_message, type, line_error);
			} else if (options.decimal_separator != "." && type.id() == LogicalTypeId::DECIMAL) {
				success = TryCastDecimalVectorCommaSeparated(options, parse_vector, result_vector, parse_chunk.size(),
				                                             error_message, type);
			} else {
				// target type is not varchar: perform a cast
				target_type_not_varchar = true;
				success =
				    VectorOperations::TryCast(context, parse_vector, result_vector, parse_chunk.size(), &error_message);
			}
			if (success) {
				continue;
			}
			if (try_add_line) {
				return false;
			}

			string col_name = to_string(col_idx);
			if (col_idx < names.size()) {
				col_name = "\"" + names[col_idx] + "\"";
			}

			// figure out the exact line number
			if (target_type_not_varchar) {
				UnifiedVectorFormat inserted_column_data;
				result_vector.ToUnifiedFormat(parse_chunk.size(), inserted_column_data);
				for (; line_error < parse_chunk.size(); line_error++) {
					if (!inserted_column_data.validity.RowIsValid(line_error) &&
					    !FlatVector::IsNull(parse_vector, line_error)) {
						break;
					}
				}
			}

			// The line_error must be summed with linenr (All lines emmited from this batch)
			// But subtracted from the parse_chunk
			D_ASSERT(line_error + linenr >= parse_chunk.size());
			line_error += linenr;
			line_error -= parse_chunk.size();

			auto error_line = GetLineError(line_error, buffer_idx);

			if (options.ignore_errors) {
				conversion_error_ignored = true;

			} else if (options.auto_detect) {
				throw InvalidInputException("%s in column %s, at line %llu.\n\nParser "
				                            "options:\n%s.\n\nConsider either increasing the sample size "
				                            "(SAMPLE_SIZE=X [X rows] or SAMPLE_SIZE=-1 [all rows]), "
				                            "or skipping column conversion (ALL_VARCHAR=1)",
				                            error_message, col_name, error_line, options.ToString());
			} else {
				throw InvalidInputException("%s at line %llu in column %s. Parser options:\n%s ", error_message,
				                            error_line, col_name, options.ToString());
			}
		}
	}
	if (conversion_error_ignored) {
		D_ASSERT(options.ignore_errors);

		SelectionVector succesful_rows(parse_chunk.size());
		idx_t sel_size = 0;

		// Keep track of failed cells
		vector<ErrorLocation> failed_cells;

		for (idx_t row_idx = 0; row_idx < parse_chunk.size(); row_idx++) {

			auto global_row_idx = row_idx + linenr - parse_chunk.size();
			auto row_line = GetLineError(global_row_idx, buffer_idx, false);

			bool row_failed = false;
			for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
				auto col_idx = reader_data.column_ids[c];
				auto result_idx = reader_data.column_mapping[c];

				auto &parse_vector = parse_chunk.data[col_idx];
				auto &result_vector = insert_chunk.data[result_idx];

				bool was_already_null = FlatVector::IsNull(parse_vector, row_idx);
				if (!was_already_null && FlatVector::IsNull(result_vector, row_idx)) {
					row_failed = true;
					failed_cells.emplace_back(row_idx, col_idx, row_line);
				}
			}
			if (!row_failed) {
				succesful_rows.set_index(sel_size++, row_idx);
			}
		}

		// Now do a second pass to produce the reject table entries
		if (!failed_cells.empty() && !options.rejects_table_name.empty()) {
			auto limit = options.rejects_limit;

			auto rejects = CSVRejectsTable::GetOrCreate(context, options.rejects_table_name);
			lock_guard<mutex> lock(rejects->write_lock);

			// short circuit if we already have too many rejects
			if (limit == 0 || rejects->count < limit) {
				auto &table = rejects->GetTable(context);
				InternalAppender appender(context, table);
				auto file_name = GetFileName();

				for (auto &cell : failed_cells) {
					if (limit != 0 && rejects->count >= limit) {
						break;
					}
					rejects->count++;

					auto row_idx = cell.row_idx;
					auto col_idx = cell.col_idx;
					auto row_line = cell.row_line;

					auto col_name = to_string(col_idx);
					if (col_idx < names.size()) {
						col_name = "\"" + names[col_idx] + "\"";
					}

					auto &parse_vector = parse_chunk.data[col_idx];
					auto parsed_str = FlatVector::GetData<string_t>(parse_vector)[row_idx];
					auto &type = insert_chunk.data[col_idx].GetType();
					auto row_error_msg = StringUtil::Format("Could not convert string '%s' to '%s'",
					                                        parsed_str.GetString(), type.ToString());

					// Add the row to the rejects table
					appender.BeginRow();
					appender.Append(string_t(file_name));
					appender.Append(row_line);
					appender.Append(col_idx);
					appender.Append(string_t(col_name));
					appender.Append(parsed_str);

					if (!options.rejects_recovery_columns.empty()) {
						child_list_t<Value> recovery_key;
						for (auto &key_idx : options.rejects_recovery_column_ids) {
							// Figure out if the recovery key is valid.
							// If not, error out for real.
							auto &component_vector = parse_chunk.data[key_idx];
							if (FlatVector::IsNull(component_vector, row_idx)) {
								throw InvalidInputException("%s at line %llu in column %s. Parser options:\n%s ",
								                            "Could not parse recovery column", row_line, col_name,
								                            options.ToString());
							}
							auto component = Value(FlatVector::GetData<string_t>(component_vector)[row_idx]);
							recovery_key.emplace_back(names[key_idx], component);
						}
						appender.Append(Value::STRUCT(recovery_key));
					}

					appender.Append(string_t(row_error_msg));
					appender.EndRow();
				}
				appender.Close();
			}
		}

		// Now slice the insert chunk to only include the succesful rows
		insert_chunk.Slice(succesful_rows, sel_size);
	}
	parse_chunk.Reset();
	return true;
}

void BaseCSVReader::SetNewLineDelimiter(bool carry, bool carry_followed_by_nl) {
	if (options.dialect_options.new_line == NewLineIdentifier::NOT_SET) {
		if (options.dialect_options.new_line == NewLineIdentifier::MIX) {
			return;
		}
		NewLineIdentifier this_line_identifier;
		if (carry) {
			if (carry_followed_by_nl) {
				this_line_identifier = NewLineIdentifier::CARRY_ON;
			} else {
				this_line_identifier = NewLineIdentifier::SINGLE;
			}
		} else {
			this_line_identifier = NewLineIdentifier::SINGLE;
		}
		if (options.dialect_options.new_line == NewLineIdentifier::NOT_SET) {
			options.dialect_options.new_line = this_line_identifier;
			return;
		}
		if (options.dialect_options.new_line != this_line_identifier) {
			options.dialect_options.new_line = NewLineIdentifier::MIX;
			return;
		}
		options.dialect_options.new_line = this_line_identifier;
	}
}
} // namespace duckdb
