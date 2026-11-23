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
	                       NewLineIdentifier new_line_p, CSVOption<CSVEscapeMode> escape_mode_p = CSVEscapeMode::QUOTED,
	                       CSVOption<CSVEmbeddedQuotes> embedded_quotes_p = CSVEmbeddedQuotes::NONE,
	                       CSVOption<CSVUnterminatedQuote> unterminated_quote_p = CSVUnterminatedQuote::ERROR,
	                       CSVOption<idx_t> new_line_terminators_p = 0,
	                       CSVOption<bool> allow_unescaped_quotes_p = false)
	    : delimiter(std::move(delimiter_p)), quote(quote_p), escape(escape_p), comment(comment_p), new_line(new_line_p),
	      escape_mode(escape_mode_p), embedded_quotes(embedded_quotes_p), unterminated_quote(unterminated_quote_p),
	      new_line_terminators(new_line_terminators_p), allow_unescaped_quotes(allow_unescaped_quotes_p) {};

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
	//! Where the escape works, inside quoted values alone, anywhere, or anywhere escaping only quotes
	CSVOption<CSVEscapeMode> escape_mode = CSVEscapeMode::QUOTED;
	//! What a quote inside an unquoted field means
	CSVOption<CSVEmbeddedQuotes> embedded_quotes = CSVEmbeddedQuotes::NONE;
	//! What a quote that never closes means
	CSVOption<CSVUnterminatedQuote> unterminated_quote = CSVUnterminatedQuote::ERROR;
	//! The record terminators the reading accepts, as bits for \n, \r and \r\n, with zero meaning new_line alone
	CSVOption<idx_t> new_line_terminators = 0;
	//! A quote that does not close its value is data rather than an error
	CSVOption<bool> allow_unescaped_quotes = false;

	//! Whether the escape works outside quoted values
	bool UnquotedEscapes() const {
		return escape_mode.GetValue() != CSVEscapeMode::QUOTED;
	}
	//! Whether the escape only escapes the quote and escape characters
	bool EscapeQuotesOnly() const {
		return escape_mode.GetValue() == CSVEscapeMode::QUOTES_ONLY;
	}
	//! Whether a doubled quote inside a field is one literal quote
	bool DoubledQuotes() const {
		return embedded_quotes.GetValue() == CSVEmbeddedQuotes::DOUBLED ||
		       embedded_quotes.GetValue() == CSVEmbeddedQuotes::DOUBLED_TOGGLE;
	}
	//! Whether a quote inside a field toggles the quote state
	bool ToggleQuotes() const {
		return embedded_quotes.GetValue() == CSVEmbeddedQuotes::TOGGLE ||
		       embedded_quotes.GetValue() == CSVEmbeddedQuotes::DOUBLED_TOGGLE;
	}
	//! Whether every line break ends the record, reading a field left open as NULL
	bool LineRecords() const {
		return unterminated_quote.GetValue() == CSVUnterminatedQuote::NULL_AT_LINE_END;
	}
	//! Whether a quote that never closes reads as a value
	bool UnterminatedAsValue() const {
		return unterminated_quote.GetValue() == CSVUnterminatedQuote::VALUE;
	}

	bool operator==(const CSVStateMachineOptions &other) const {
		return delimiter == other.delimiter && quote == other.quote && escape == other.escape &&
		       new_line == other.new_line && comment == other.comment && escape_mode == other.escape_mode &&
		       embedded_quotes == other.embedded_quotes && unterminated_quote == other.unterminated_quote &&
		       new_line_terminators == other.new_line_terminators &&
		       allow_unescaped_quotes == other.allow_unescaped_quotes;
	}
};
} // namespace duckdb
