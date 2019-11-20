/*-------------------------------------------------------------------------
 *
 * fmgr.h
 *	  Definitions for the Postgres function manager and function-call
 *	  interface.
 *
 * This file must be included by all Postgres modules that either define
 * or call fmgr-callable functions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/fmgr.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "pg_definitions.hpp"

typedef struct PGFunctionCallInfoData *PGFunctionCallInfo;

/* Standard parameter list for fmgr-compatible functions */
#define PG_FUNCTION_ARGS	PGFunctionCallInfo fcinfo
