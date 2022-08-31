#if (!defined(_WIN32) && !defined(WIN32)) || defined(__MINGW32__)
#include <unistd.h>
#define GETPID ::getpid
#else
#include <windows.h>
#define GETPID (int)GetCurrentProcessId
#endif

namespace duckdb {

int getpid() {
	return GETPID();
}

} // namespace duckdb
