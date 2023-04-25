//---------------------------------------------------------------------------
//	@filename:
//		CAutoSuspendAbort.h
//
//	@doc:
//		Auto object for suspending and resuming task cancellation
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoSuspendAbort_H
#define GPOS_CAutoSuspendAbort_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CStackObject.h"

namespace gpos
{
class CTask;

//---------------------------------------------------------------------------
//	@class:
//		CAutoSuspendAbort
//
//	@doc:
//		Auto object for suspending and resuming task cancellation
//
//---------------------------------------------------------------------------
class CAutoSuspendAbort : public CStackObject
{
private:
	// pointer to task in current execution context
	CTask *m_task;

public:
	// ctor - suspends CFA
	CAutoSuspendAbort();

	// dtor - resumes CFA
	virtual ~CAutoSuspendAbort();

};	// class CAutoSuspendAbort

}  // namespace gpos

#endif
