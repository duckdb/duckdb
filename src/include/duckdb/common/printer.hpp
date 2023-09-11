//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/printer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

enum class OutputStream : uint8_t { STREAM_STDOUT = 1, STREAM_STDERR = 2 };

//! Printer is a static class that allows printing to logs or stdout/stderr
class Printer {
public:
	//! Print the object to the stream
	DUCKDB_API static void Print(OutputStream stream, const string &str);
	//! Print the object to stderr
	DUCKDB_API static void Print(const string &str);
	//! Print the formatted object to the stream
	template <typename... Args>
	static void PrintF(OutputStream stream, const string &str, Args... params) {
		Printer::Print(stream, StringUtil::Format(str, params...));
	}
	//! Print the formatted object to stderr
	template <typename... Args>
	static void PrintF(const string &str, Args... params) {
		Printer::PrintF(OutputStream::STREAM_STDERR, str, std::forward<Args>(params)...);
	}
	//! Directly prints the string to stdout without a newline
	DUCKDB_API static void RawPrint(OutputStream stream, const string &str);
	//! Flush an output stream
	DUCKDB_API static void Flush(OutputStream stream);
	//! Whether or not we are printing to a terminal
	DUCKDB_API static bool IsTerminal(OutputStream stream);
	//! The terminal width
	DUCKDB_API static idx_t TerminalWidth();
};
} // namespace duckdb
