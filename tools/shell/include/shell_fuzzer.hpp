#pragma once

namespace duckdb_shell {

#ifdef DUCKDB_FUZZER
int RunFuzzer();
#endif

} // namespace duckdb_shell
