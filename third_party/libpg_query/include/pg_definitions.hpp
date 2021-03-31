// this is a bit of a mess from c.h, port.h and some others. Upside is it makes the parser compile with minimal
// dependencies.

#pragma once

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string>

#ifdef ERROR
#undef ERROR
#endif

typedef uintptr_t PGDatum;
typedef uint64_t PGSize;

typedef uint32_t PGIndex;
typedef uint32_t PGOid;

#define InvalidOid ((PGOid)0)

#ifndef _MSC_VER
#include <assert.h>
#define Assert(a) assert(a);
#define AssertMacro(p) ((void)assert(p))
#else
#define Assert(a) (a);
#define AssertMacro(p) ((void)(p))
#endif
#define _(a) (a)

#define lengthof(array) (sizeof(array) / sizeof((array)[0]))
#define CppConcat(x, y) x##y

#define HIGHBIT (0x80)
#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch)&HIGHBIT)

#define FUNC_MAX_ARGS 100
#define FLEXIBLE_ARRAY_MEMBER

#define DEFAULT_INDEX_TYPE "art"
#define INTERVAL_MASK(b) (1 << (b))

#ifdef _MSC_VER
#define __thread __declspec(thread)
#endif


//typedef struct {
//	int32_t vl_len_;    /* these fields must match ArrayType! */
//	int ndim;         /* always 1 for PGint2vector */
//	int32_t dataoffset; /* always 0 for PGint2vector */
//	PGOid elemtype;
//	int dim1;
//	int lbound1;
//	int16_t values[];
//} PGint2vector;

struct pg_varlena {
	char vl_len_[4];                    /* Do not touch this field directly! */
	char vl_dat[1]; /* Data content is here */
};

typedef struct pg_varlena bytea;

typedef int PGMemoryContext;

namespace duckdb_libpgquery {

typedef enum PGPostgresParserErrors {
	PG_ERRCODE_SYNTAX_ERROR,
	PG_ERRCODE_FEATURE_NOT_SUPPORTED,
	PG_ERRCODE_INVALID_PARAMETER_VALUE,
	PG_ERRCODE_WINDOWING_ERROR,
	PG_ERRCODE_RESERVED_NAME,
	PG_ERRCODE_INVALID_ESCAPE_SEQUENCE,
	PG_ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER,
	ERRCODE_NAME_TOO_LONG
} PGPostgresParserErrors;

typedef enum PGPostgresRelPersistence {
	PG_RELPERSISTENCE_TEMP,
	PG_RELPERSISTENCE_UNLOGGED,
	RELPERSISTENCE_PERMANENT
} PGPostgresRelPersistence;

typedef enum PGPostgresErrorLevel {
	PGUNDEFINED,
	PGNOTICE,
	PGWARNING,
	ERROR
} PGPostgresErrorLevel;

typedef enum PGPostgresAttributIdentityTypes {
	PG_ATTRIBUTE_IDENTITY_ALWAYS,
	ATTRIBUTE_IDENTITY_BY_DEFAULT
} PGPostgresAttributIdentityTypes;

}
