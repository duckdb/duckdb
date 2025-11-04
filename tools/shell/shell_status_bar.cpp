#include "shell_status_bar.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb_shell {
using duckdb::OutputStream;
using duckdb::Printer;

void ShellStatusBarDisplay::Update(double percentage) {
	duckdb::TerminalProgressBarDisplay::Update(percentage);
}

void ShellStatusBarDisplay::Finish() {
	Printer::RawPrint(OutputStream::STREAM_STDOUT, "\r\x1b[0K");
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

void ShellStatusBarDisplay::PrintProgressInternal(int32_t percentage, double estimated_remaining_seconds,
                                                  bool is_finished) {
	duckdb::TerminalProgressBarDisplay::PrintProgressInternal(percentage, estimated_remaining_seconds, is_finished);
}

} // namespace duckdb_shell
