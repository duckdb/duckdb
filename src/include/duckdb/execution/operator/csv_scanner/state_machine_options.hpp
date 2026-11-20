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
	                       NewLineIdentifier new_line_p, bool strict_mode_p,
	                       CSVOption<CSVEscapeMode> escape_mode_p = CSVEscapeMode::ALL,
	                       CSVOption<CSVQuoteMode> quote_mode_p = CSVQuoteMode::STANDARD,
	                       CSVOption<bool> doubled_quotes_p = false,
	                       CSVOption<CSVEmbeddedQuote> embedded_quote_p = CSVEmbeddedQuote::DATA,
	                       CSVOption<bool> mixed_line_breaks_p = false,
	                       CSVOption<bool> allow_unescaped_quotes_p = false)
	    : delimiter(std::move(delimiter_p)), quote(quote_p), escape(escape_p), comment(comment_p), new_line(new_line_p),
	      strict_mode(strict_mode_p), escape_mode(escape_mode_p), quote_mode(quote_mode_p),
	      doubled_quotes(doubled_quotes_p), mixed_line_breaks(mixed_line_breaks_p),
	      allow_unescaped_quotes(allow_unescaped_quotes_p), embedded_quote(embedded_quote_p) {};

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
	//! How Strict the parser should be
	CSVOption<bool> strict_mode = true;
	//! Which characters the escape character can escape, following strict_mode when not set
	CSVOption<CSVEscapeMode> escape_mode = CSVEscapeMode::ALL;
	//! What quotes mean at record boundaries
	CSVOption<CSVQuoteMode> quote_mode = CSVQuoteMode::STANDARD;
	//! A doubled quote is one quote also outside quoted fields and with an unrelated escape, as in opencsv
	CSVOption<bool> doubled_quotes = false;
	//! Any line break ends a record whatever the file's line break is, following strict_mode when not set
	CSVOption<bool> mixed_line_breaks = false;
	//! A quote that does not close its value is data rather than an error, following strict_mode when not set
	CSVOption<bool> allow_unescaped_quotes = false;
	//! What a quote in the middle of a field means
	CSVOption<CSVEmbeddedQuote> embedded_quote = CSVEmbeddedQuote::DATA;

	bool operator==(const CSVStateMachineOptions &other) const {
		return delimiter == other.delimiter && quote == other.quote && escape == other.escape &&
		       new_line == other.new_line && comment == other.comment && strict_mode == other.strict_mode &&
		       escape_mode == other.escape_mode && quote_mode == other.quote_mode &&
		       doubled_quotes == other.doubled_quotes && embedded_quote == other.embedded_quote &&
		       mixed_line_breaks == other.mixed_line_breaks && allow_unescaped_quotes == other.allow_unescaped_quotes;
	}
};
} // namespace duckdb
