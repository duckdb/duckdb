//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_reader_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/csv_buffer.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/multi_file_reader_options.hpp"

namespace duckdb {

enum NewLineIdentifier {
	SINGLE = 1,   // Either \r or \n
	CARRY_ON = 2, // \r\n
	MIX = 3,      // Hippie-Land, can't run it multithreaded
	NOT_SET = 4
};

struct BufferedCSVReaderOptions {
	//===--------------------------------------------------------------------===//
	// CommonCSVOptions
	//===--------------------------------------------------------------------===//

	//! Whether or not a delimiter was defined by the user
	bool has_delimiter = false;
	//! Delimiter to separate columns within each line
	string delimiter = ",";
	//! Whether or not a new_line was defined by the user
	bool has_newline = false;
	//! New Line separator
	NewLineIdentifier new_line = NewLineIdentifier::NOT_SET;
	//! Whether or not a quote was defined by the user
	bool has_quote = false;
	//! Quote used for columns that contain reserved characters, e.g., delimiter
	string quote = "\"";
	//! Whether or not an escape character was defined by the user
	bool has_escape = false;
	//! Escape character to escape quote character
	string escape;
	//! Whether or not a header information was given by the user
	bool has_header = false;
	//! Whether or not the file has a header line
	bool header = false;
	//! Whether or not we should ignore InvalidInput errors
	bool ignore_errors = false;
	//! Expected number of columns
	idx_t num_cols = 0;
	//! Number of samples to buffer
	idx_t buffer_sample_size = STANDARD_VECTOR_SIZE * 50;
	//! Specifies the string that represents a null value
	string null_str;
	//! Whether file is compressed or not, and if so which compression type
	//! AUTO_DETECT (default; infer from file extension)
	FileCompressionType compression = FileCompressionType::AUTO_DETECT;
	//! Option to convert quoted values to NULL values
	bool allow_quoted_nulls = true;

	//===--------------------------------------------------------------------===//
	// CSVAutoOptions
	//===--------------------------------------------------------------------===//
	//! SQL Type list mapping of name to SQL type index in sql_type_list
	case_insensitive_map_t<idx_t> sql_types_per_column;
	//! User-defined SQL type list
	vector<LogicalType> sql_type_list;
	//! User-defined name list
	vector<string> name_list;
	//! Types considered as candidates for auto detection ordered by descending specificity (~ from high to low)
	vector<LogicalType> auto_type_candidates = {LogicalType::VARCHAR, LogicalType::TIMESTAMP, LogicalType::DATE,
	                                            LogicalType::TIME,    LogicalType::DOUBLE,    LogicalType::BIGINT,
	                                            LogicalType::BOOLEAN, LogicalType::SQLNULL};

	//===--------------------------------------------------------------------===//
	// ReadCSVOptions
	//===--------------------------------------------------------------------===//

	//! How many leading rows to skip
	idx_t skip_rows = 0;
	//! Whether or not the skip_rows is set by the user
	bool skip_rows_set = false;
	//! Maximum CSV line size: specified because if we reach this amount, we likely have wrong delimiters (default: 2MB)
	//! note that this is the guaranteed line length that will succeed, longer lines may be accepted if slightly above
	idx_t maximum_line_size = 2097152;
	//! Whether or not header names shall be normalized
	bool normalize_names = false;
	//! True, if column with that index must skip null check
	vector<bool> force_not_null;
	//! Consider all columns to be of type varchar
	bool all_varchar = false;
	//! Size of sample chunk used for dialect and type detection
	idx_t sample_chunk_size = STANDARD_VECTOR_SIZE;
	//! Number of sample chunks used for type detection
	idx_t sample_chunks = 10;
	//! Whether or not to automatically detect dialect and datatypes
	bool auto_detect = false;
	//! The file path of the CSV file to read
	string file_path;
	//! Multi-file reader options
	MultiFileReaderOptions file_options;
	//! Buffer Size (Parallel Scan)
	idx_t buffer_size = CSVBuffer::INITIAL_BUFFER_SIZE_COLOSSAL;
	//! Decimal separator when reading as numeric
	string decimal_separator = ".";
	//! Whether or not to pad rows that do not have enough columns with NULL values
	bool null_padding = false;

	//! If we are running the parallel version of the CSV Reader. In general, the system should always auto-detect
	//! When it can't execute a parallel run before execution. However, there are (rather specific) situations where
	//! setting up this manually might be important
	bool run_parallel = true;
	//===--------------------------------------------------------------------===//
	// WriteCSVOptions
	//===--------------------------------------------------------------------===//

	//! True, if column with that index must be quoted
	vector<bool> force_quote;

	//! The date format to use (if any is specified)
	std::map<LogicalTypeId, StrpTimeFormat> date_format = {{LogicalTypeId::DATE, {}}, {LogicalTypeId::TIMESTAMP, {}}};
	//! The date format to use for writing (if any is specified)
	std::map<LogicalTypeId, StrfTimeFormat> write_date_format = {{LogicalTypeId::DATE, {}},
	                                                             {LogicalTypeId::TIMESTAMP, {}}};
	//! Whether or not a type format is specified
	std::map<LogicalTypeId, bool> has_format = {
	    {LogicalTypeId::DATE, false}, {LogicalTypeId::TIMESTAMP, false}, {LogicalTypeId::TIMESTAMP_TZ, false}};

	void Serialize(FieldWriter &writer) const;
	void Deserialize(FieldReader &reader);

	void SetCompression(const string &compression);
	void SetHeader(bool has_header);
	void SetEscape(const string &escape);
	void SetQuote(const string &quote);
	void SetDelimiter(const string &delimiter);

	void SetNewline(const string &input);
	//! Set an option that is supported by both reading and writing functions, called by
	//! the SetReadOption and SetWriteOption methods
	bool SetBaseOption(const string &loption, const Value &value);

	//! loption - lowercase string
	//! set - argument(s) to the option
	//! expected_names - names expected if the option is "columns"
	void SetReadOption(const string &loption, const Value &value, vector<string> &expected_names);

	void SetWriteOption(const string &loption, const Value &value);
	void SetDateFormat(LogicalTypeId type, const string &format, bool read_format);

	std::string ToString() const;
};
} // namespace duckdb
