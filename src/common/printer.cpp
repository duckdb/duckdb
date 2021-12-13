#include "duckdb/common/printer.hpp"
#include "duckdb/common/progress_bar.hpp"
#include "duckdb/common/windows_util.hpp"
#include "duckdb/common/windows.hpp"
#include <stdio.h>

#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
#include <io.h>
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
		fprintf(stderr, "%s\n", unicode.c_str());
		return;
	}
#endif
	fprintf(stderr, "%s\n", str.c_str());
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
	return GetFileType(GetStdHandle(STD_ERROR_HANDLE)) == FILE_TYPE_CHAR;
#else
	throw InternalException("IsTerminal is only implemented for Windows");
#endif
#endif
	return false;
}
// LCOV_EXCL_STOP

} // namespace duckdb
