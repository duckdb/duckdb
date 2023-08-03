//---------------------------------------------------------------------------
//	@filename:
//		CSchedulerContext.h
//
//	@doc:
//		Container for objects associated with scheduling context of a job
//---------------------------------------------------------------------------
#ifndef GPOPT_CSchedulerContext_H
#define GPOPT_CSchedulerContext_H

#include "duckdb/optimizer/cascade/base.h"

#define GPOPT_SCHED_CTXT_MEM_POOL_SIZE (64 * 1024 * 1024)

namespace gpopt
{
using namespace gpos;

// prototypes
class CJobFactory;
class CScheduler;
class CEngine;

//---------------------------------------------------------------------------
//	@class:
//		CSchedulerContext
//
//	@doc:
//		Scheduling context
//
//---------------------------------------------------------------------------
class CSchedulerContext
{
public:
	// job factory
	CJobFactory* m_pjf;

	// scheduler
	CScheduler* m_psched;

	// optimization engine
	CEngine* m_peng;

	// flag indicating if context has been initialized
	bool m_fInit;

public:
	// ctor
	CSchedulerContext();

	// no copy ctor
	CSchedulerContext(const CSchedulerContext &) = delete;

	// dtor
	~CSchedulerContext();

	// initialization
	void Init(CJobFactory* pjf, CScheduler* psched, CEngine* peng);
};	// class CSchedulerContext
}  // namespace gpopt
#endif