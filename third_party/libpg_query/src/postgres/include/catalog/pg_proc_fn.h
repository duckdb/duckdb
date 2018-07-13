/*-------------------------------------------------------------------------
 *
 * pg_proc_fn.h
 *	 prototypes for functions in catalog/pg_proc.c
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_proc_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROC_FN_H
#define PG_PROC_FN_H

#include "catalog/objectaddress.h"
#include "nodes/pg_list.h"

extern ObjectAddress ProcedureCreate(const char *procedureName,
				Oid procNamespace,
				bool replace,
				bool returnsSet,
				Oid returnType,
				Oid proowner,
				Oid languageObjectId,
				Oid languageValidator,
				const char *prosrc,
				const char *probin,
				bool isAgg,
				bool isWindowFunc,
				bool security_definer,
				bool isLeakProof,
				bool isStrict,
				char volatility,
				oidvector *parameterTypes,
				Datum allParameterTypes,
				Datum parameterModes,
				Datum parameterNames,
				List *parameterDefaults,
				Datum trftypes,
				Datum proconfig,
				float4 procost,
				float4 prorows);

extern bool function_parse_error_transpose(const char *prosrc);

extern List *oid_array_to_list(Datum datum);

#endif   /* PG_PROC_FN_H */
