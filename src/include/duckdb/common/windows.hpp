#ifdef _WIN32

#ifndef DUCKDB_WINDOWS_H
#define DUCKDB_WINDOWS_H

#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <winsock2.h>
#include <windows.h>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory

#endif

#endif
