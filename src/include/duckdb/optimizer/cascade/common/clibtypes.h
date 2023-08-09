//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		clibtypes.h
//
//	@doc:
//		clib definitions for GPOS;
//---------------------------------------------------------------------------
#ifndef GPOS_clibtypes_H
#define GPOS_clibtypes_H

#include <dlfcn.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>

#include "duckdb/optimizer/cascade/types.h"

namespace gpos
{
// container for user and system time
typedef struct rusage RUSAGE;

// represent an elapsed time
typedef struct timeval TIMEVAL;

// hold minimal information about the local time zone
typedef struct timezone TIMEZONE;

// represents an elapsed time
typedef struct timespec TIMESPEC;

// store system time values
typedef time_t TIME_T;

// containing a calendar date and time broken down into its components.
typedef struct tm TIME;

// store information of a calling process
typedef Dl_info DL_INFO;
}  // namespace gpos

#endif
