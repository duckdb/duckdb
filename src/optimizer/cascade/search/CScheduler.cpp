//---------------------------------------------------------------------------
//	@filename:
//		CScheduler.cpp
//
//	@doc:
//		Implementation of optimizer job scheduler
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include <assert.h>

namespace gpopt
{
//---------------------------------------------------------------------------
//	@function:
//		CScheduler::CScheduler
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScheduler::CScheduler(ULONG ulJobs)
	: m_spjl(ulJobs), m_ulpTotal(0), m_ulpRunning(0), m_ulpQueued(0), m_ulpStatsQueued(0), m_ulpStatsDequeued(0), m_ulpStatsSuspended(0), m_ulpStatsCompleted(0), m_ulpStatsCompletedQueued(0), m_ulpStatsResumed(0)
{
	SJobLink* jl = new SJobLink();
	SIZE_T id_offset = (SIZE_T)(&(jl->m_id)) - (SIZE_T)jl;
	SIZE_T link_offset = (SIZE_T)(&(jl->m_link)) - (SIZE_T)jl;
	delete jl;
	// initialize pool of job links
	m_spjl.Init((gpos::ULONG)id_offset);
	// initialize list of waiting new jobs
	m_listjlWaiting.Init((gpos::ULONG)link_offset);
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::~CScheduler
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScheduler::~CScheduler()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Run
//
//	@doc:
//		Main job processing task
//
//---------------------------------------------------------------------------
void* CScheduler::Run(void *pv)
{
	CSchedulerContext* psc = reinterpret_cast<CSchedulerContext*>(pv);
	psc->m_psched->ExecuteJobs(psc);
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::ExecuteJobs
//
//	@doc:
// 		Job processing loop;
//		keeps executing jobs as long as there is work queued;
//
//---------------------------------------------------------------------------
void CScheduler::ExecuteJobs(CSchedulerContext* psc)
{
	CJob* pj;
	ULONG count = 0;
	// keep retrieving jobs
	while (NULL != (pj = PjRetrieve()))
	{
		// prepare for job execution
		PreExecute(pj);
		// execute job
		bool fCompleted = FExecute(pj, psc);
		// process job result
		switch (EjrPostExecute(pj, fCompleted))
		{
			case EjrCompleted:
				// job is completed
				Complete(pj);
				psc->m_pjf->Release(pj);
				break;
			case EjrRunnable:
				// child jobs have completed, job can immediately resume
				Resume(pj);
				continue;
			case EjrSuspended:
				// job is suspended until child jobs complete
				Suspend(pj);
				break;
			default:
				assert(false);
		}
		if (++count == OPT_SCHED_CFA)
		{
			count = 0;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Add
//
//	@doc:
//		Add new job for execution
//
//---------------------------------------------------------------------------
void CScheduler::Add(CJob* pj, CJob* pjParent)
{
	// increment ref counter for parent job
	if (nullptr != pjParent)
	{
		pjParent->IncRefs();
	}
	// set current job as parent of its child
	pj->SetParent(pjParent);
	// increment total number of jobs
	m_ulpTotal++;
	Schedule(pj);
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Resume
//
//	@doc:
//		Resume suspended job
//
//---------------------------------------------------------------------------
void CScheduler::Resume(CJob* pj)
{
	Schedule(pj);
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Schedule
//
//	@doc:
//		Schedule job for execution
//
//---------------------------------------------------------------------------
void CScheduler::Schedule(CJob* pj)
{
	// get job link
	SJobLink* pjl = m_spjl.PtRetrieve();
	pjl->Init(pj);
	// add to waiting list
	m_listjlWaiting.Push(pjl);
	// increment number of queued jobs
	m_ulpQueued++;
	// update statistics
	m_ulpStatsQueued++;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::PreExecute
//
//	@doc:
// 		Prepare for job execution
//
//---------------------------------------------------------------------------
void CScheduler::PreExecute(CJob* pj)
{
	// increment number of running jobs
	m_ulpRunning++;
	// increment job ref counter
	pj->IncRefs();
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::FExecute
//
//	@doc:
//		Execution function using job queue
//
//---------------------------------------------------------------------------
bool CScheduler::FExecute(CJob* pj, CSchedulerContext* psc)
{
	bool fCompleted = true;
	CJobQueue* pjq = pj->Pjq();
	// check if job is associated to a job queue
	if (NULL == pjq)
	{
		fCompleted = pj->FExecute(psc);
	}
	else
	{
		switch (pjq->EjqrAdd(pj))
		{
			case CJobQueue::EjqrMain:
				// main job, runs job operation
				fCompleted = pj->FExecute(psc);
				if (fCompleted)
				{
					// notify queued jobs
					pjq->NotifyCompleted(psc);
				}
				else
				{
					// task is suspended
					(void) pj->UlpDecrRefs();
				}
				break;
			case CJobQueue::EjqrQueued:
				// queued job
				fCompleted = false;
				break;
			case CJobQueue::EjqrCompleted:
				break;
		}
	}
	return fCompleted;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::EjrPostExecute
//
//	@doc:
// 		Process job execution outcome
//
//---------------------------------------------------------------------------
CScheduler::EJobResult CScheduler::EjrPostExecute(CJob* pj, bool fCompleted)
{
	// decrement job ref counter
	ULONG_PTR ulRefs = pj->UlpDecrRefs();
	// decrement number of running jobs
	m_ulpRunning--;
	// check if job completed
	if (fCompleted)
	{
		return EjrCompleted;
	}
	// check if all children have completed
	if (1 == ulRefs)
	{
		return EjrRunnable;
	}
	return EjrSuspended;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::PjRetrieve
//
//	@doc:
//		Retrieve next runnable job from queue
//
//---------------------------------------------------------------------------
CJob* CScheduler::PjRetrieve()
{
	// retrieve runnable job from lists of waiting jobs
	SJobLink* pjl = m_listjlWaiting.Pop();
	CJob* pj = nullptr;
	if (NULL != pjl)
	{
		pj = pjl->m_pj;
		// decrement number of queued jobs
		m_ulpQueued--;
		// update statistics
		m_ulpStatsDequeued++;
		// recycle job link
		m_spjl.Recycle(pjl);
	}
	return pj;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Suspend
//
//	@doc:
//		Transition job to suspended
//
//---------------------------------------------------------------------------
void CScheduler::Suspend(CJob* )
{
	m_ulpStatsSuspended++;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Complete
//
//	@doc:
//		Transition job to completed
//
//---------------------------------------------------------------------------
void CScheduler::Complete(CJob* pj)
{
	ResumeParent(pj);
	// update statistics
	m_ulpTotal--;
	m_ulpStatsCompleted++;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::CompleteQueued
//
//	@doc:
//		Transition queued job to completed
//
//---------------------------------------------------------------------------
void CScheduler::CompleteQueued(CJob* pj)
{
	ResumeParent(pj);
	// update statistics
	m_ulpTotal--;
	m_ulpStatsCompleted++;
	m_ulpStatsCompletedQueued++;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::ResumeParent
//
//	@doc:
//		Resume parent job
//
//---------------------------------------------------------------------------
void CScheduler::ResumeParent(CJob* pj)
{
	CJob* pjParent = pj->PjParent();
	if (NULL != pjParent)
	{
		// notify parent job
		if (pj->FResumeParent())
		{
			// reschedule parent
			Resume(pjParent);
			// update statistics
			m_ulpStatsResumed++;
		}
	}
}
}