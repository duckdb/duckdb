#include "duckdb/execution/operator/persistent/base_csv_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "utf8proc_wrapper.hpp"
#include "utf8proc.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/main/error_manager.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>

namespace duckdb {

string BaseCSVReader::GetLineNumberStr(idx_t linenr, bool linenr_estimated) {
	string estimated = (linenr_estimated ? string(" (estimated)") : string(""));
	return to_string(linenr + 1) + estimated;
}

BaseCSVReader::BaseCSVReader(ClientContext &context_p, BufferedCSVReaderOptions options_p,
                             const vector<LogicalType> &requested_types)
    : context(context_p), fs(FileSystem::GetFileSystem(context)), allocator(Allocator::Get(context)),
      opener(FileSystem::GetFileOpener(context)), options(std::move(options_p)) {
}

BaseCSVReader::~BaseCSVReader() {
}

unique_ptr<CSVFileHandle> BaseCSVReader::OpenCSV(const BufferedCSVReaderOptions &options_p) {
	auto file_handle = fs.OpenFile(options_p.file_path.c_str(), FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK,
	                               options_p.compression, this->opener);
	return make_unique<CSVFileHandle>(std::move(file_handle));
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

void BaseCSVReader::InitInsertChunkIdx(idx_t num_cols) {
	for (idx_t col = 0; col < num_cols; ++col) {
		insert_cols_idx.push_back(col);
	}
}

void BaseCSVReader::SetDateFormat(const string &format_specifier, const LogicalTypeId &sql_type) {
	options.has_format[sql_type] = true;
	auto &date_format = options.date_format[sql_type];
	date_format.format_specifier = format_specifier;
	StrTimeFormat::ParseFormatSpecifier(date_format.format_specifier, date_format);
}

struct TryCastDecimalOperator {
	template <class OP, class T>
	static bool Operation(string_t input, uint8_t width, uint8_t scale) {
		T result;
		string error_message;
		return OP::Operation(input, result, &error_message, width, scale);
	}
};

struct TryCastFloatingOperator {
	template <class OP, class T>
	static bool Operation(string_t input) {
		T result;
		string error_message;
		return OP::Operation(input, result, &error_message);
	}
};

bool TryCastDecimalValueCommaSeparated(const string_t &value_str, const LogicalType &sql_type) {
	auto width = DecimalType::GetWidth(sql_type);
	auto scale = DecimalType::GetScale(sql_type);
	switch (sql_type.InternalType()) {
	case PhysicalType::INT16:
		return TryCastDecimalOperator::Operation<TryCastToDecimalCommaSeparated, int16_t>(value_str, width, scale);
	case PhysicalType::INT32:
		return TryCastDecimalOperator::Operation<TryCastToDecimalCommaSeparated, int32_t>(value_str, width, scale);
	case PhysicalType::INT64:
		return TryCastDecimalOperator::Operation<TryCastToDecimalCommaSeparated, int64_t>(value_str, width, scale);
	case PhysicalType::INT128:
		return TryCastDecimalOperator::Operation<TryCastToDecimalCommaSeparated, hugeint_t>(value_str, width, scale);
	default:
		throw InternalException("Unimplemented physical type for decimal");
	}
}

bool TryCastFloatingValueCommaSeparated(const string_t &value_str, const LogicalType &sql_type) {
	switch (sql_type.InternalType()) {
	case PhysicalType::DOUBLE:
		return TryCastFloatingOperator::Operation<TryCastErrorMessageCommaSeparated, double>(value_str);
	case PhysicalType::FLOAT:
		return TryCastFloatingOperator::Operation<TryCastErrorMessageCommaSeparated, float>(value_str);
	default:
		throw InternalException("Unimplemented physical type for floating");
	}
}

bool BaseCSVReader::TryCastValue(const Value &value, const LogicalType &sql_type) {
	if (options.has_format[LogicalTypeId::DATE] && sql_type.id() == LogicalTypeId::DATE) {
		date_t result;
		string error_message;
		return options.date_format[LogicalTypeId::DATE].TryParseDate(string_t(StringValue::Get(value)), result,
		                                                             error_message);
	} else if (options.has_format[LogicalTypeId::TIMESTAMP] && sql_type.id() == LogicalTypeId::TIMESTAMP) {
		timestamp_t result;
		string error_message;
		return options.date_format[LogicalTypeId::TIMESTAMP].TryParseTimestamp(string_t(StringValue::Get(value)),
		                                                                       result, error_message);
	} else if (options.decimal_separator != "." && sql_type.id() == LogicalTypeId::DECIMAL) {
		return TryCastDecimalValueCommaSeparated(string_t(StringValue::Get(value)), sql_type);
	} else if (options.decimal_separator != "." &&
	           ((sql_type.id() == LogicalTypeId::FLOAT) || (sql_type.id() == LogicalTypeId::DOUBLE))) {
		return TryCastFloatingValueCommaSeparated(string_t(StringValue::Get(value)), sql_type);
	} else {
		Value new_value;
		string error_message;
		return value.TryCastAs(context, sql_type, new_value, &error_message, true);
	}
}

struct TryCastDateOperator {
	static bool Operation(BufferedCSVReaderOptions &options, string_t input, date_t &result, string &error_message) {
		return options.date_format[LogicalTypeId::DATE].TryParseDate(input, result, error_message);
	}
};

struct TryCastTimestampOperator {
	static bool Operation(BufferedCSVReaderOptions &options, string_t input, timestamp_t &result,
	                      string &error_message) {
		return options.date_format[LogicalTypeId::TIMESTAMP].TryParseTimestamp(input, result, error_message);
	}
};

template <class OP, class T>
static bool TemplatedTryCastDateVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                       idx_t count, string &error_message) {
	D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
	bool all_converted = true;
	UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
		T result;
		if (!OP::Operation(options, input, result, error_message)) {
			all_converted = false;
		}
		return result;
	});
	return all_converted;
}

bool TryCastDateVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector, idx_t count,
                       string &error_message) {
	return TemplatedTryCastDateVector<TryCastDateOperator, date_t>(options, input_vector, result_vector, count,
	                                                               error_message);
}

bool TryCastTimestampVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector, idx_t count,
                            string &error_message) {
	return TemplatedTryCastDateVector<TryCastTimestampOperator, timestamp_t>(options, input_vector, result_vector,
	                                                                         count, error_message);
}

template <class OP, class T>
bool TemplatedTryCastFloatingVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                    idx_t count, string &error_message) {
	D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
	bool all_converted = true;
	UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
		T result;
		if (!OP::Operation(input, result, &error_message)) {
			all_converted = false;
		}
		return result;
	});
	return all_converted;
}

template <class OP, class T>
bool TemplatedTryCastDecimalVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                   idx_t count, string &error_message, uint8_t width, uint8_t scale) {
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

bool BaseCSVReader::TryCastVector(Vector &parse_chunk_col, idx_t size, const LogicalType &sql_type) {
	// try vector-cast from string to sql_type
	Vector dummy_result(sql_type);
	if (options.has_format[LogicalTypeId::DATE] && sql_type == LogicalTypeId::DATE) {
		// use the date format to cast the chunk
		string error_message;
		return TryCastDateVector(options, parse_chunk_col, dummy_result, size, error_message);
	} else if (options.has_format[LogicalTypeId::TIMESTAMP] && sql_type == LogicalTypeId::TIMESTAMP) {
		// use the timestamp format to cast the chunk
		string error_message;
		return TryCastTimestampVector(options, parse_chunk_col, dummy_result, size, error_message);
	} else {
		// target type is not varchar: perform a cast
		string error_message;
		return VectorOperations::DefaultTryCast(parse_chunk_col, dummy_result, size, &error_message, true);
	}
}

void BaseCSVReader::AddValue(string_t str_val, idx_t &column, vector<idx_t> &escape_positions, bool has_quotes) {
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
	if (mode == ParserMode::SNIFFING_DIALECT) {
		column++;
		return;
	}
	if (column >= return_types.size()) {
		if (options.ignore_errors) {
			error_column_overflow = true;
			return;
		} else {
			throw InvalidInputException(
			    "Error in file \"%s\", on line %s: expected %lld values per row, but got more. (%s)", options.file_path,
			    GetLineNumberStr(linenr, linenr_estimated).c_str(), return_types.size(), options.ToString());
		}
	}

	// insert the line number into the chunk
	idx_t row_entry = parse_chunk.size();

	// test against null string, but only if the value was not quoted
	if ((!has_quotes || return_types[column].id() != LogicalTypeId::VARCHAR) && !options.force_not_null[column] &&
	    Equals::Operation(str_val, string_t(options.null_str))) {
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

				if (options.escape.empty() || options.escape == options.quote) {
					prev_pos = next_pos + options.quote.size();
				} else {
					prev_pos = next_pos + options.escape.size();
				}
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

bool BaseCSVReader::AddRow(DataChunk &insert_chunk, idx_t &column, string &error_message) {
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

	if (column < return_types.size() && mode != ParserMode::SNIFFING_DIALECT) {
		if (options.ignore_errors) {
			column = 0;
			return false;
		} else {
			if (mode == ParserMode::SNIFFING_DATATYPES) {
				error_message = "Error when adding line";
				return false;
			} else {
				throw InvalidInputException(
				    "Error in file \"%s\" on line %s: expected %lld values per row, but got %d.\nParser options:\n%s",
				    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), return_types.size(), column,
				    options.ToString());
			}
		}
	}

	if (mode == ParserMode::SNIFFING_DIALECT) {
		sniffed_column_counts.push_back(column);

		if (sniffed_column_counts.size() == options.sample_chunk_size) {
			return true;
		}
	} else {
		parse_chunk.SetCardinality(parse_chunk.size() + 1);
	}

	if (mode == ParserMode::PARSING_HEADER) {
		return true;
	}

	if (mode == ParserMode::SNIFFING_DATATYPES && parse_chunk.size() == options.sample_chunk_size) {
		return true;
	}

	if (mode == ParserMode::PARSING && parse_chunk.size() == STANDARD_VECTOR_SIZE) {
		Flush(insert_chunk);
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
	auto utf_type = Utf8Proc::Analyze(s.GetDataUnsafe(), s.GetSize());
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

bool TryCastDecimalVectorCommaSeparated(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                        idx_t count, string &error_message, LogicalType &result_type) {
	auto width = DecimalType::GetWidth(result_type);
	auto scale = DecimalType::GetScale(result_type);
	switch (result_type.InternalType()) {
	case PhysicalType::INT16:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int16_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
		break;
	case PhysicalType::INT32:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int32_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
		break;
	case PhysicalType::INT64:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int64_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
		break;
	case PhysicalType::INT128:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, hugeint_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
		break;
	default:
		throw InternalException("Unimplemented physical type for decimal");
	}
}

bool TryCastFloatingVectorCommaSeparated(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                         idx_t count, string &error_message, LogicalType &result_type) {
	switch (result_type.InternalType()) {
	case PhysicalType::DOUBLE:
		return TemplatedTryCastFloatingVector<TryCastErrorMessageCommaSeparated, double>(
		    options, input_vector, result_vector, count, error_message);
		break;
	case PhysicalType::FLOAT:
		return TemplatedTryCastFloatingVector<TryCastErrorMessageCommaSeparated, float>(
		    options, input_vector, result_vector, count, error_message);
		break;
	default:
		throw InternalException("Unimplemented physical type for floating");
	}
}

bool BaseCSVReader::Flush(DataChunk &insert_chunk, bool try_add_line) {
	if (parse_chunk.size() == 0) {
		return true;
	}

	bool conversion_error_ignored = false;

	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);
	for (idx_t col_idx = 0; col_idx < return_types.size(); col_idx++) {
		auto insert_idx = insert_cols_idx[col_idx];
		auto &type = return_types[col_idx];
		if (type.id() == LogicalTypeId::VARCHAR) {
			// target type is varchar: no need to convert
			// just test that all strings are valid utf-8 strings
			VerifyUTF8(col_idx);
			// reinterpret rather than reference so we can deal with user-defined types
			insert_chunk.data[insert_idx].Reinterpret(parse_chunk.data[col_idx]);

		} else {
			string error_message;
			bool success;
			if (options.has_format[LogicalTypeId::DATE] && type.id() == LogicalTypeId::DATE) {
				// use the date format to cast the chunk
				success = TryCastDateVector(options, parse_chunk.data[col_idx], insert_chunk.data[insert_idx],
				                            parse_chunk.size(), error_message);
			} else if (options.has_format[LogicalTypeId::TIMESTAMP] &&
			           return_types[col_idx].id() == LogicalTypeId::TIMESTAMP) {
				// use the date format to cast the chunk
				success = TryCastTimestampVector(options, parse_chunk.data[col_idx], insert_chunk.data[insert_idx],
				                                 parse_chunk.size(), error_message);
			} else if (options.decimal_separator != "." && (return_types[col_idx].id() == LogicalTypeId::FLOAT ||
			                                                return_types[col_idx].id() == LogicalTypeId::DOUBLE)) {
				success = TryCastFloatingVectorCommaSeparated(options, parse_chunk.data[col_idx],
				                                              insert_chunk.data[insert_idx], parse_chunk.size(),
				                                              error_message, return_types[col_idx]);
			} else if (options.decimal_separator != "." && return_types[col_idx].id() == LogicalTypeId::DECIMAL) {
				success = TryCastDecimalVectorCommaSeparated(options, parse_chunk.data[col_idx],
				                                             insert_chunk.data[insert_idx], parse_chunk.size(),
				                                             error_message, return_types[col_idx]);
			} else {
				// target type is not varchar: perform a cast
				success = VectorOperations::TryCast(context, parse_chunk.data[col_idx], insert_chunk.data[insert_idx],
				                                    parse_chunk.size(), &error_message);
			}
			if (success) {
				continue;
			}
			if (try_add_line) {
				return false;
			}
			if (options.ignore_errors) {
				conversion_error_ignored = true;
				continue;
			}
			string col_name = to_string(col_idx);
			if (col_idx < names.size()) {
				col_name = "\"" + names[col_idx] + "\"";
			}

			// figure out the exact line number
			idx_t row_idx;
			for (row_idx = 0; row_idx < parse_chunk.size(); row_idx++) {
				auto &inserted_column = insert_chunk.data[col_idx];
				auto &parsed_column = parse_chunk.data[col_idx];

				if (FlatVector::IsNull(inserted_column, row_idx) && !FlatVector::IsNull(parsed_column, row_idx)) {
					break;
				}
			}
			auto error_line = linenr - (parse_chunk.size() - row_idx) + 1;

			if (options.auto_detect) {
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
		SelectionVector succesful_rows;
		succesful_rows.Initialize(parse_chunk.size());
		idx_t sel_size = 0;

		for (idx_t row_idx = 0; row_idx < parse_chunk.size(); row_idx++) {
			bool failed = false;
			for (idx_t column_idx = 0; column_idx < return_types.size(); column_idx++) {

				auto &inserted_column = insert_chunk.data[column_idx];
				auto &parsed_column = parse_chunk.data[column_idx];

				bool was_already_null = FlatVector::IsNull(parsed_column, row_idx);
				if (!was_already_null && FlatVector::IsNull(inserted_column, row_idx)) {
					failed = true;
					break;
				}
			}
			if (!failed) {
				succesful_rows.set_index(sel_size++, row_idx);
			}
		}
		insert_chunk.Slice(succesful_rows, sel_size);
	}
	parse_chunk.Reset();
	return true;
}

void BaseCSVReader::SetNewLineDelimiter(bool carry, bool carry_followed_by_nl) {
	if ((mode == ParserMode::SNIFFING_DIALECT && !options.has_newline) ||
	    options.new_line == NewLineIdentifier::NOT_SET) {
		if (options.new_line == NewLineIdentifier::MIX) {
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
		if (options.new_line == NewLineIdentifier::NOT_SET) {
			options.new_line = this_line_identifier;
			return;
		}
		if (options.new_line != this_line_identifier) {
			options.new_line = NewLineIdentifier::MIX;
			return;
		}
		options.new_line = this_line_identifier;
	}
}
} // namespace duckdb
