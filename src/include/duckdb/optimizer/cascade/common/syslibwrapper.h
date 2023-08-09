//---------------------------------------------------------------------------
//	@filename:
//	       	syslibwrapper.h
//
//	@doc:
//	       	Wrapper for functions in system library
//
//---------------------------------------------------------------------------
#ifndef GPOS_syslibwrapper_H
#define GPOS_syslibwrapper_H

#include "duckdb/optimizer/cascade/common/clibtypes.h"
#include "duckdb/optimizer/cascade/types.h"

namespace gpos
{
namespace syslib
{
// get the date and time
void GetTimeOfDay(TIMEVAL *tv, TIMEZONE *tz);

// get system and user time
void GetRusage(RUSAGE *usage);

// yield the processor
void SchedYield();

// open a connection to the system logger for a program
void OpenLog(const CHAR *ident, INT option, INT facility);

// generate a log message
void SysLog(INT priority, const CHAR *format);

// close the descriptor being used to write to the system logger
void CloseLog();


}  //namespace syslib
}  // namespace gpos

#endif