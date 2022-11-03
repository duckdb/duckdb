//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/printer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class OutputStream : uint8_t { STREAM_STDOUT = 1, STREAM_STDERR = 2 };

//! Printer is a static class that allows printing to logs or stdout/stderr
class Printer {
public:
	//! Print the object to stderr
	DUCKDB_API static void Print(OutputStream stream, const string &str);
	//! Print the object to stderr
	DUCKDB_API static void Print(const string &str);
	//! Prints Progress
	DUCKDB_API static void PrintProgress(int percentage, const char *pbstr, int pbwidth);
	//! Prints an empty line when progress bar is done
	DUCKDB_API static void FinishProgressBarPrint(const char *pbstr, int pbwidth);
	//! Whether or not we are printing to a terminal
	DUCKDB_API static bool IsTerminal(OutputStream stream);
	//! The terminal width
	DUCKDB_API static idx_t TerminalWidth();
};
} // namespace duckdb
