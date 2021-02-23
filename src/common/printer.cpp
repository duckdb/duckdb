#include "duckdb/common/printer.hpp"
#include "duckdb/common/progress_bar.hpp"
#include <stdio.h>

namespace duckdb {

void Printer::Print(const string &str) {
#ifndef DUCKDB_DISABLE_PRINT
	fprintf(stderr, "%s\n", str.c_str());
#endif
}
void ProgressBar::PrintProgress(int percentage) {
#ifndef DUCKDB_DISABLE_PRINT
	int lpad = (int)(percentage / 100.0 * pbwidth);
	int rpad = pbwidth - lpad;
	printf("\r%3d%% [%.*s%*s]", percentage, lpad, pbstr.c_str(), rpad, "");
	fflush(stdout);
#endif
}

void ProgressBar::FinishPrint() {
#ifndef DUCKDB_DISABLE_PRINT
	PrintProgress(100);
	printf(" \n");
	fflush(stdout);
#endif
}

} // namespace duckdb
