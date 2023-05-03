#include "duckdb/common/printer.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/windows_util.hpp"
#include "duckdb/common/windows.hpp"
#include <stdio.h>

#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
#include <io.h>
#else
#include <sys/ioctl.h>
#include <stdio.h>
#include <unistd.h>
#endif
#endif

namespace duckdb {

void Printer::RawPrint(OutputStream stream, const string &str) {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
	if (IsTerminal(stream)) {
		// print utf8 to terminal
		auto unicode = WindowsUtil::UTF8ToMBCS(str.c_str());
		fprintf(stream == OutputStream::STREAM_STDERR ? stderr : stdout, "%s", unicode.c_str());
		return;
	}
#endif
	fprintf(stream == OutputStream::STREAM_STDERR ? stderr : stdout, "%s", str.c_str());
#endif
}

// LCOV_EXCL_START
void Printer::Print(OutputStream stream, const string &str) {
	Printer::RawPrint(stream, str);
	Printer::RawPrint(stream, "\n");
}
void Printer::Flush(OutputStream stream) {
#ifndef DUCKDB_DISABLE_PRINT
	fflush(stream == OutputStream::STREAM_STDERR ? stderr : stdout);
#endif
}

void Printer::Print(const string &str) {
	Printer::Print(OutputStream::STREAM_STDERR, str);
}

bool Printer::IsTerminal(OutputStream stream) {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
	auto stream_handle = stream == OutputStream::STREAM_STDERR ? STD_ERROR_HANDLE : STD_OUTPUT_HANDLE;
	return GetFileType(GetStdHandle(stream_handle)) == FILE_TYPE_CHAR;
#else
	return isatty(stream == OutputStream::STREAM_STDERR ? 2 : 1);
#endif
#else
	throw InternalException("IsTerminal called while printing is disabled");
#endif
}

idx_t Printer::TerminalWidth() {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
	CONSOLE_SCREEN_BUFFER_INFO csbi;
	int columns, rows;

	GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi);
	rows = csbi.srWindow.Right - csbi.srWindow.Left + 1;
	return rows;
#else
	struct winsize w;
	ioctl(0, TIOCGWINSZ, &w);
	return w.ws_col;
#endif
#else
	throw InternalException("TerminalWidth called while printing is disabled");
#endif
}
// LCOV_EXCL_STOP

} // namespace duckdb
