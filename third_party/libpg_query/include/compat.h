// this is a bit of a mess from c.h, port.h and some others. Upside is it makes the parser compile with minimal dependencies.

#ifndef COMPAT_H
#define COMPAT_H

#include <stddef.h>
#include <stdint.h>
#include <limits.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>

typedef uintptr_t Datum;
typedef uint64_t Size;

typedef int64_t int64;

typedef uint32_t uint32;
typedef uint32_t Index;
typedef uint32_t bits32;
typedef uint32_t Oid;

#define InvalidOid		((Oid) 0)


typedef int32_t LOCKMODE;
typedef int32_t int32;

typedef int16_t int16;
typedef uint16_t uint16;

typedef uint8_t uint8;

#ifndef __cplusplus
typedef uint8_t bool;
#endif
#define true 1
#define false 0
#define TRUE 1
#define FALSE 0

#define PG_INT32_MAX INT_MAX
#define PGDLLIMPORT
#define Assert(a) assert(a);
#define _(a) (a)

#define AssertMacro(p)	((void) assert(p))
#define pg_attribute_noreturn() __attribute__((noreturn))
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define CppConcat(x, y)			x##y

#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

#define NAMEDATALEN 64
#define FUNC_MAX_ARGS		100
#define FLEXIBLE_ARRAY_MEMBER

#define PG_USE_INLINE
#define STATIC_IF_INLINE static inline


typedef struct
{
	int32		vl_len_;		/* these fields must match ArrayType! */
	int			ndim;			/* always 1 for int2vector */
	int32		dataoffset;		/* always 0 for int2vector */
	Oid			elemtype;
	int			dim1;
	int			lbound1;
	int16		values[];
} int2vector;

typedef struct nameData
{
	char		data[NAMEDATALEN];
} NameData;
typedef NameData *Name;


struct varlena
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];	/* Data content is here */
};

typedef struct varlena bytea;

typedef int MemoryContext;

enum PostgresErrorLevel {
	NOTICE,
	WARNING,
	ERROR
};

enum PostgresParserErrors {
	ERRCODE_SYNTAX_ERROR,
	ERRCODE_FEATURE_NOT_SUPPORTED,
	ERRCODE_INVALID_PARAMETER_VALUE,
	ERRCODE_WINDOWING_ERROR,
	ERRCODE_RESERVED_NAME,
	ERRCODE_INVALID_ESCAPE_SEQUENCE,
	ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER
};

#define ereport_domain(elevel, rest)	\
	do { \
		const int elevel_ = (elevel); \
		/* something */ \
		errcode(elevel); \
	} while(0)

#define ereport(elevel, rest)	\
	ereport_domain(elevel, rest)


#define		  REPLICA_IDENTITY_DEFAULT	'd'
#define		  REPLICA_IDENTITY_NOTHING	'n'
#define		  REPLICA_IDENTITY_FULL		'f'
#define		  REPLICA_IDENTITY_INDEX	'i'

#define		  RELPERSISTENCE_TEMP		't'
#define		  RELPERSISTENCE_UNLOGGED	'u'
#define		  RELPERSISTENCE_PERMANENT	'p'

#define		  ATTRIBUTE_IDENTITY_ALWAYS		'a'
#define		  ATTRIBUTE_IDENTITY_BY_DEFAULT 'd'

#define AMTYPE_INDEX					'i'

#define DEFAULT_INDEX_TYPE	"btree"

#define TRIGGER_FIRES_ON_ORIGIN				'O'
#define TRIGGER_FIRES_ALWAYS				'A'
#define TRIGGER_FIRES_ON_REPLICA			'R'
#define TRIGGER_DISABLED					'D'

#define INTERVAL_MASK(b) (1 << (b))


enum PostgresLockTypes {
	NoLock,
	AccessShareLock,
	RowShareLock,
	RowExclusiveLock,
	ShareUpdateExclusiveLock,
	ShareLock,
	ShareRowExclusiveLock,
	ExclusiveLock,
	AccessExclusiveLock
};

typedef enum
{
	XML_STANDALONE_YES,
	XML_STANDALONE_NO,
	XML_STANDALONE_NO_VALUE,
	XML_STANDALONE_OMITTED
}	XmlStandaloneType;

extern  bool operator_precedence_warning;

#include "pg_compat.h"


#endif
