//---------------------------------------------------------------------------
//	@filename:
//		CJobQueue.h
//
//	@doc:
//		Class controlling unique execution of an operation that is
//		potentially assigned to many jobs.
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobQueue_H
#define GPOPT_CJobQueue_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/search/CJob.h"

using namespace gpos;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CJobQueue
//
//	@doc:
//		Forces unique execution of an operation assigned to many jobs.
//
//---------------------------------------------------------------------------
class CJobQueue
{
private:
	// main job
	CJob* m_pj;

	// flag indicating if main job has completed
	bool m_fCompleted;

	// list of jobs waiting for main job to complete
	CList<CJob> m_listjQueued;

public:
	// enum indicating job queueing result
	enum EJobQueueResult
	{ EjqrMain = 0, EjqrQueued, EjqrCompleted };

	// ctor
	CJobQueue()
		: m_pj(NULL), m_fCompleted(false)
	{
		CJob* tmp = new CJob();
		SIZE_T ptr = (SIZE_T)(&(tmp->m_linkQueue)) - (SIZE_T)tmp;
		m_listjQueued.Init((gpos::ULONG)ptr);
		delete tmp;
	}

	// dtor
	~CJobQueue()
	{
	}

	// reset job queue
	void Reset()
	{
		m_pj = nullptr;
		m_fCompleted = false;
	}

	// add job as a waiter;
	EJobQueueResult EjqrAdd(CJob* pj);

	// notify waiting jobs of job completion
	void NotifyCompleted(CSchedulerContext* psc);
};	// class CJobQueue
}  // namespace gpopt
#endif