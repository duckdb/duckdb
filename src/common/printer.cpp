#include "duckdb/common/printer.hpp"
#include "duckdb/common/progress_bar.hpp"
#include "duckdb/common/windows_util.hpp"
#include "duckdb/common/windows.hpp"
#include <stdio.h>

#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
#include <io.h>
#include "duckdb/common/windows.hpp"
#else
#include <sys/ioctl.h>
#include <stdio.h>
#include <unistd.h>
#endif
#endif

namespace duckdb {

// LCOV_EXCL_START
void Printer::Print(const string &str) {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
	if (IsTerminal()) {
		// print utf8 to terminal
		auto unicode = WindowsUtil::UTF8ToMBCS(str.c_str());
		fprintf(stdout, "%s\n", unicode.c_str());
		return;
	}
#endif
	fprintf(stdout, "%s\n", str.c_str());
#endif
}

void Printer::PrintProgress(int percentage, const char *pbstr, int pbwidth) {
#ifndef DUCKDB_DISABLE_PRINT
	int lpad = (int)(percentage / 100.0 * pbwidth);
	int rpad = pbwidth - lpad;
	printf("\r%3d%% [%.*s%*s]", percentage, lpad, pbstr, rpad, "");
	fflush(stdout);
#endif
}

void Printer::FinishProgressBarPrint(const char *pbstr, int pbwidth) {
#ifndef DUCKDB_DISABLE_PRINT
	PrintProgress(100, pbstr, pbwidth);
	printf(" \n");
	fflush(stdout);
#endif
}

bool Printer::IsTerminal() {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
	return GetFileType(GetStdHandle(STD_OUTPUT_HANDLE)) == FILE_TYPE_CHAR;
#else
	return isatty(1);
#endif
#endif
}

idx_t Printer::TerminalWidth() {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
    CONSOLE_SCREEN_BUFFER_INFO csbi;
    int columns, rows;

    GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi);
    rows = csbi.srWindow.Bottom - csbi.srWindow.Top + 1;
	return rows;
#else
	struct winsize w;
	ioctl(0, TIOCGWINSZ, &w);
	return w.ws_col;
#endif
#endif
}
// LCOV_EXCL_STOP

} // namespace duckdb
