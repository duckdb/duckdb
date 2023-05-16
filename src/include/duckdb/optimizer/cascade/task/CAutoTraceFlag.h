//---------------------------------------------------------------------------
//	@filename:
//		CAutoTraceFlag.h
//
//	@doc:
//		Auto wrapper to set/reset a traceflag for a scope
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoTraceFlag_H
#define GPOS_CAutoTraceFlag_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CStackObject.h"
#include "duckdb/optimizer/cascade/task/ITask.h"
#include "duckdb/optimizer/cascade/task/traceflags.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CAutoTraceFlag
//
//	@doc:
//		Auto wrapper;
//
//---------------------------------------------------------------------------
class CAutoTraceFlag : public CStackObject
{
private:
	// traceflag id
	ULONG m_trace;
	// original value
	BOOL m_orig;

	// no copy ctor
	CAutoTraceFlag(const CAutoTraceFlag &);

public:
	// ctor
	CAutoTraceFlag(ULONG trace, BOOL orig);

	// dtor
	virtual ~CAutoTraceFlag();

};	// class CAutoTraceFlag

}  // namespace gpos

#endif