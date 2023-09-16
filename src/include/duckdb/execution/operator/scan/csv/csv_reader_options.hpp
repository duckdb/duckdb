//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_reader_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_buffer.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/multi_file_reader_options.hpp"

namespace duckdb {

enum class NewLineIdentifier : uint8_t {
	SINGLE = 1,   // Either \r or \n
	CARRY_ON = 2, // \r\n
	MIX = 3,      // Hippie-Land, can't run it multithreaded
	NOT_SET = 4
};

enum class ParallelMode { AUTOMATIC = 0, PARALLEL = 1, SINGLE_THREADED = 2 };

//! Struct that holds the configuration of a CSV State Machine
//! Basically which char, quote and escape were used to generate it.
struct CSVStateMachineOptions {
	CSVStateMachineOptions() {};
	CSVStateMachineOptions(char delimiter_p, char quote_p, char escape_p)
	    : delimiter(delimiter_p), quote(quote_p), escape(escape_p) {};

	//! Delimiter to separate columns within each line
	char delimiter = ',';
	//! Quote used for columns that contain reserved characters, e.g '
	char quote = '\"';
	//! Escape character to escape quote character
	char escape = '\0';

	bool operator==(const CSVStateMachineOptions &other) const {
		return delimiter == other.delimiter && quote == other.quote && escape == other.escape;
	}
};

struct DialectOptions {
	CSVStateMachineOptions state_machine_options;
	//! New Line separator
	NewLineIdentifier new_line = NewLineIdentifier::NOT_SET;
	//! Expected number of columns
	idx_t num_cols = 0;
	//! Whether or not the file has a header line
	bool header = false;
	//! The date format to use (if any is specified)
	map<LogicalTypeId, StrpTimeFormat> date_format = {{LogicalTypeId::DATE, {}}, {LogicalTypeId::TIMESTAMP, {}}};
	//! Whether or not a type format is specified
	map<LogicalTypeId, bool> has_format = {{LogicalTypeId::DATE, false}, {LogicalTypeId::TIMESTAMP, false}};
	//! How many leading rows to skip
	idx_t skip_rows = 0;
	//! True start of the first CSV Buffer (After skipping empty lines, headers, notes and so on)
	idx_t true_start = 0;
};

struct CSVReaderOptions {
	//===--------------------------------------------------------------------===//
	// CommonCSVOptions
	//===--------------------------------------------------------------------===//
	//! See struct above.
	DialectOptions dialect_options;
	//! Whether or not a delimiter was defined by the user
	bool has_delimiter = false;
	//! Whether or not a new_line was defined by the user
	bool has_newline = false;
	//! Whether or not a quote was defined by the user
	bool has_quote = false;
	//! Whether or not an escape character was defined by the user
	bool has_escape = false;
	//! Whether or not a header information was given by the user
	bool has_header = false;
	//! Whether or not we should ignore InvalidInput errors
	bool ignore_errors = false;
	//! Rejects table name
	string rejects_table_name;
	//! Rejects table entry limit (0 = no limit)
	idx_t rejects_limit = 0;
	//! Columns to use as recovery key for rejected rows when reading with ignore_errors = true
	vector<string> rejects_recovery_columns;
	//! Index of the recovery columns
	vector<idx_t> rejects_recovery_column_ids;
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
	idx_t buffer_size = CSVBuffer::CSV_BUFFER_SIZE;
	//! Decimal separator when reading as numeric
	string decimal_separator = ".";
	//! Whether or not to pad rows that do not have enough columns with NULL values
	bool null_padding = false;

	//! If we are running the parallel version of the CSV Reader. In general, the system should always auto-detect
	//! When it can't execute a parallel run before execution. However, there are (rather specific) situations where
	//! setting up this manually might be important
	ParallelMode parallel_mode;
	//===--------------------------------------------------------------------===//
	// WriteCSVOptions
	//===--------------------------------------------------------------------===//
	//! True, if column with that index must be quoted
	vector<bool> force_quote;
	//! Prefix/suffix/custom newline the entire file once (enables writing of files as JSON arrays)
	string prefix;
	string suffix;
	string write_newline;

	//! The date format to use for writing (if any is specified)
	map<LogicalTypeId, StrfTimeFormat> write_date_format = {{LogicalTypeId::DATE, {}}, {LogicalTypeId::TIMESTAMP, {}}};

	void Serialize(Serializer &serializer) const;
	static CSVReaderOptions Deserialize(Deserializer &deserializer);

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

	string ToString() const;
};
} // namespace duckdb
