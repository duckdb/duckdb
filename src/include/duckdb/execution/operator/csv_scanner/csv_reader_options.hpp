//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_reader_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_buffer.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_option.hpp"
#include "duckdb/execution/operator/csv_scanner/state_machine_options.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/multi_file_reader_options.hpp"

namespace duckdb {

struct DialectOptions {
	CSVStateMachineOptions state_machine_options;
	//! Expected number of columns
	idx_t num_cols = 0;
	//! Whether or not the file has a header line
	CSVOption<bool> header = false;
	//! The date format to use (if any is specified)
	map<LogicalTypeId, CSVOption<StrpTimeFormat>> date_format = {{LogicalTypeId::DATE, {}},
	                                                             {LogicalTypeId::TIMESTAMP, {}}};
	//! How many leading rows to skip
	CSVOption<idx_t> skip_rows = 0;
	idx_t rows_until_header = 0;
};

struct CSVReaderOptions {
	//===--------------------------------------------------------------------===//
	// CommonCSVOptions
	//===--------------------------------------------------------------------===//
	//! See struct above.
	DialectOptions dialect_options;
	//! Whether we should ignore InvalidInput errors
	CSVOption<bool> ignore_errors = false;
	//! Whether we store CSV Errors in the rejects table or not
	CSVOption<bool> store_rejects = false;
	//! Rejects table name (Name of the table the store rejects errors)
	CSVOption<string> rejects_table_name = {"reject_errors"};
	//! Rejects Scan name  (Name of the table the store rejects scans)
	CSVOption<string> rejects_scan_name = {"reject_scans"};
	//! Rejects table entry limit (0 = no limit)
	idx_t rejects_limit = 0;
	//! Number of samples to buffer
	idx_t buffer_sample_size = static_cast<idx_t>(STANDARD_VECTOR_SIZE * 50);
	//! Specifies the strings that represents a null value
	vector<string> null_str = {""};
	//! Whether file is compressed or not, and if so which compression type
	//! AUTO_DETECT (default; infer from file extension)
	FileCompressionType compression = FileCompressionType::AUTO_DETECT;
	//! Option to convert quoted values to NULL values
	bool allow_quoted_nulls = true;
	char comment;

	//===--------------------------------------------------------------------===//
	// CSVAutoOptions
	//===--------------------------------------------------------------------===//
	//! SQL Type list mapping of name to SQL type index in sql_type_list
	case_insensitive_map_t<idx_t> sql_types_per_column;
	//! User-defined SQL type list
	vector<LogicalType> sql_type_list;
	//! User-defined name list
	vector<string> name_list;
	//! If the names and types were set by the columns parameter
	bool columns_set = false;
	//! Types considered as candidates for auto detection ordered by descending specificity (~ from high to low)
	vector<LogicalType> auto_type_candidates = {LogicalType::VARCHAR,   LogicalType::DOUBLE, LogicalType::BIGINT,
	                                            LogicalType::TIMESTAMP, LogicalType::DATE,   LogicalType::TIME,
	                                            LogicalType::BOOLEAN,   LogicalType::SQLNULL};
	//! In case the sniffer found a mismatch error from user defined types or dialect
	string sniffer_user_mismatch_error;
	//! In case the sniffer found a mismatch error from user defined types or dialect
	vector<bool> was_type_manually_set;
	//===--------------------------------------------------------------------===//
	// ReadCSVOptions
	//===--------------------------------------------------------------------===//
	//! Maximum CSV line size: specified because if we reach this amount, we likely have wrong delimiters (default: 2MB)
	//! note that this is the guaranteed line length that will succeed, longer lines may be accepted if slightly above
	idx_t maximum_line_size = 2097152;
	//! Whether or not header names shall be normalized
	bool normalize_names = false;
	//! True, if column with that index must skip null check
	unordered_set<string> force_not_null_names;
	//! True, if column with that index must skip null check
	vector<bool> force_not_null;
	//! Result size of sniffing phases
	static constexpr idx_t sniff_size = 2048;
	//! Number of sample chunks used in auto-detection
	idx_t sample_size_chunks = 20480 / sniff_size;
	//! Consider all columns to be of type varchar
	bool all_varchar = false;
	//! Whether or not to automatically detect dialect and datatypes
	bool auto_detect = true;
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
	//! If we should attempt to run parallel scanning over one file
	bool parallel = true;

	//! User defined parameters for the csv function concatenated on a string
	string user_defined_parameters;

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
	map<LogicalTypeId, Value> write_date_format = {{LogicalTypeId::DATE, Value()}, {LogicalTypeId::TIMESTAMP, Value()}};
	//! Whether or not a type format is specified
	map<LogicalTypeId, bool> has_format = {{LogicalTypeId::DATE, false}, {LogicalTypeId::TIMESTAMP, false}};

	void Serialize(Serializer &serializer) const;
	static CSVReaderOptions Deserialize(Deserializer &deserializer);

	void SetCompression(const string &compression);

	bool GetHeader() const;
	void SetHeader(bool has_header);

	string GetEscape() const;
	void SetEscape(const string &escape);

	idx_t GetSkipRows() const;

	void SetSkipRows(int64_t rows);

	void SetQuote(const string &quote);
	string GetQuote() const;
	void SetComment(const string &comment);
	string GetComment() const;
	void SetDelimiter(const string &delimiter);
	string GetDelimiter() const;

	//! If we can safely ignore errors (i.e., they are being ignored and not being stored in a rejects table)
	bool IgnoreErrors() const;

	string GetNewline() const;
	void SetNewline(const string &input);
	//! Set an option that is supported by both reading and writing functions, called by
	//! the SetReadOption and SetWriteOption methods
	bool SetBaseOption(const string &loption, const Value &value, bool write_option = false);

	//! loption - lowercase string
	//! set - argument(s) to the option
	//! expected_names - names expected if the option is "columns"
	void SetReadOption(const string &loption, const Value &value, vector<string> &expected_names);
	void SetWriteOption(const string &loption, const Value &value);
	void SetDateFormat(LogicalTypeId type, const string &format, bool read_format);
	void ToNamedParameters(named_parameter_map_t &out);
	void FromNamedParameters(named_parameter_map_t &in, ClientContext &context);

	string ToString(const string &current_file_path) const;
	//! If the type for column with idx i was manually set
	bool WasTypeManuallySet(idx_t i) const;

	string NewLineIdentifierToString() const {
		switch (dialect_options.state_machine_options.new_line.GetValue()) {
		case NewLineIdentifier::SINGLE_N:
			return "\\n";
		case NewLineIdentifier::SINGLE_R:
			return "\\r";
		case NewLineIdentifier::CARRY_ON:
			return "\\r\\n";
		default:
			return "";
		}
	}
};
} // namespace duckdb
