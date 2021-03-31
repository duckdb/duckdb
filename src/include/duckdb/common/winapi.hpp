#pragma once

#ifndef DUCKDB_API
#ifdef _WIN32
#ifdef DUCKDB_BUILD_LIBRARY
#define DUCKDB_API __declspec(dllexport)
#else
#define DUCKDB_API __declspec(dllimport)
#endif
#else
#define DUCKDB_API
#endif
#endif
