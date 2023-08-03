//---------------------------------------------------------------------------
//	@filename:
//		types.h
//
//	@doc:
//		Type definitions for gpos to avoid using native types directly;
//
//---------------------------------------------------------------------------
#ifndef GPOS_types_H
#define GPOS_types_H

#include <limits.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <iostream>
#include "duckdb/optimizer/cascade/assert.h"

#define GPOS_SIZEOF(x) ((gpos::ULONG) sizeof(x))
#define GPOS_ARRAY_SIZE(x) (GPOS_SIZEOF(x) / GPOS_SIZEOF(x[0]))
#define GPOS_OFFSET(T, M) ((gpos::ULONG)(SIZE_T) &(((T *) 0x8)->M) - 8)

/* wide character string literate */
#define GPOS_WSZ_LIT(x) L##x

namespace gpos
{
// Basic types to be used instead of built-ins
// Add types as needed;
typedef unsigned char BYTE;

typedef char CHAR;
// ignore signed char for the moment

// wide character type
typedef wchar_t WCHAR;

typedef bool BOOL;

// numeric types
typedef size_t SIZE_T;
typedef ssize_t SSIZE_T;
typedef mode_t MODE_T;

// define ULONG,ULLONG as types which implement standard's
// requirements for ULONG_MAX and ULLONG_MAX; eliminate standard's slack
// by fixed sizes rather than min requirements

typedef uint32_t ULONG;
GPOS_CPL_ASSERT(4 == sizeof(ULONG));
enum
{
	ulong_max = ((::gpos::ULONG) -1)
};

typedef uint64_t ULLONG;
GPOS_CPL_ASSERT(8 == sizeof(ULLONG));
enum
{
	ullong_max = ((::gpos::ULLONG) -1)
};

typedef uintptr_t ULONG_PTR;
#ifdef GPOS_32BIT
#define ULONG_PTR_MAX (gpos::ulong_max)
#else
#define ULONG_PTR_MAX (gpos::ullong_max)
#endif

typedef uint16_t USINT;
typedef int16_t SINT;
typedef int32_t INT;
typedef int64_t LINT;
typedef intptr_t INT_PTR;

GPOS_CPL_ASSERT(2 == sizeof(USINT));
GPOS_CPL_ASSERT(2 == sizeof(SINT));
GPOS_CPL_ASSERT(4 == sizeof(INT));
GPOS_CPL_ASSERT(8 == sizeof(LINT));

enum
{
	int_max = ((::gpos::INT)(gpos::ulong_max >> 1)),
	int_min = (-gpos::int_max - 1)
};

enum
{
	lint_max = ((::gpos::LINT)(gpos::ullong_max >> 1)),
	lint_min = (-gpos::lint_max - 1)
};

enum
{
	usint_max = ((::gpos::USINT) -1)
};

enum
{
	sint_max = ((::gpos::SINT)(gpos::usint_max >> 1)),
	sint_min = (-gpos::sint_max - 1)
};

typedef double DOUBLE;

typedef void *VOID_PTR;

// holds for all platforms
GPOS_CPL_ASSERT(sizeof(ULONG_PTR) == sizeof(void *));

// variadic parameter list type
typedef va_list VA_LIST;

// wide char ostream
typedef std::basic_ostream<WCHAR, std::char_traits<WCHAR> > WOSTREAM;
typedef std::ios_base IOS_BASE;

// bad allocation exception
typedef std::bad_alloc BAD_ALLOC;

// no throw type
typedef std::nothrow_t NO_THROW;

// enum for results on OS level (instead of using a global error variable)
enum GPOS_RESULT
{
	GPOS_OK = 0,
	GPOS_FAILED = 1,
	GPOS_OOM = 2,
	GPOS_NOT_FOUND = 3,
	GPOS_TIMEOUT = 4
};

// enum for locale encoding
enum ELocale
{
	ELocInvalid,  // invalid key for hashtable iteration
	ElocEnUS_Utf8,
	ElocGeDE_Utf8,
	ElocSentinel
};
}  // namespace gpos
#endif