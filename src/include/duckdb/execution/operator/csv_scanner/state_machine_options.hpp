//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/state_machine_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_option.hpp"

namespace duckdb {
//! Struct that holds the configuration of a CSV State Machine
//! Basically which char, quote and escape were used to generate it.
struct CSVStateMachineOptions {
	CSVStateMachineOptions() {};
	CSVStateMachineOptions(string delimiter_p, char quote_p, char escape_p, char comment_p,
	                       NewLineIdentifier new_line_p, CSVOption<bool> unquoted_escapes_p = false,
	                       CSVOption<bool> escape_quotes_only_p = false, CSVOption<bool> doubled_quotes_p = false,
	                       CSVOption<bool> embedded_quotes_p = false, CSVOption<bool> line_records_p = false,
	                       CSVOption<bool> mixed_line_breaks_p = false,
	                       CSVOption<bool> allow_unescaped_quotes_p = false)
	    : delimiter(std::move(delimiter_p)), quote(quote_p), escape(escape_p), comment(comment_p), new_line(new_line_p),
	      unquoted_escapes(unquoted_escapes_p), escape_quotes_only(escape_quotes_only_p),
	      doubled_quotes(doubled_quotes_p), embedded_quotes(embedded_quotes_p), line_records(line_records_p),
	      mixed_line_breaks(mixed_line_breaks_p), allow_unescaped_quotes(allow_unescaped_quotes_p) {};

	//! Delimiter to separate columns within each line
	CSVOption<string> delimiter {","};
	//! Quote used for columns that contain reserved characters, e.g '
	CSVOption<char> quote = '\"';
	//! Escape character to escape quote character
	CSVOption<char> escape = '\"';
	//! Comment character to skip a line
	CSVOption<char> comment = '\0';
	//! New Line separator
	CSVOption<NewLineIdentifier> new_line = NewLineIdentifier::NOT_SET;
	//! The escape also works outside quoted values
	CSVOption<bool> unquoted_escapes = false;
	//! The escape escapes only the quote and escape characters and is inert before anything else
	CSVOption<bool> escape_quotes_only = false;
	//! A doubled quote is one quote also outside quoted fields and with an unrelated escape, as in opencsv
	CSVOption<bool> doubled_quotes = false;
	//! A lone quote in the middle of a field toggles the quote state, as opencsv's embedded quote rule reads it
	CSVOption<bool> embedded_quotes = false;
	//! Every line break ends the record even inside quotes, and a quote left open reads its field as NULL
	CSVOption<bool> line_records = false;
	//! Any line break ends a record whatever the file's line break is
	CSVOption<bool> mixed_line_breaks = false;
	//! A quote that does not close its value is data rather than an error
	CSVOption<bool> allow_unescaped_quotes = false;

	bool operator==(const CSVStateMachineOptions &other) const {
		return delimiter == other.delimiter && quote == other.quote && escape == other.escape &&
		       new_line == other.new_line && comment == other.comment && unquoted_escapes == other.unquoted_escapes &&
		       escape_quotes_only == other.escape_quotes_only && doubled_quotes == other.doubled_quotes &&
		       embedded_quotes == other.embedded_quotes && line_records == other.line_records &&
		       mixed_line_breaks == other.mixed_line_breaks && allow_unescaped_quotes == other.allow_unescaped_quotes;
	}
};
} // namespace duckdb
