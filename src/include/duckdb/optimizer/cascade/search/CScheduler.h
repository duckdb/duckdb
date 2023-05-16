//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2011 Greenplum, Inc.
//
//	@filename:
//		CScheduler.h
//
//	@doc:
//		Scheduler interface for execution of optimization jobs
//---------------------------------------------------------------------------
#ifndef GPOPT_CScheduler_H
#define GPOPT_CScheduler_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CSyncList.h"
#include "duckdb/optimizer/cascade/common/CSyncPool.h"

#include "duckdb/optimizer/cascade/search/CJob.h"

#define OPT_SCHED_QUEUED_RUNNING_RATIO 10
#define OPT_SCHED_CFA 100

namespace gpopt
{
using namespace gpos;

// prototypes
class CSchedulerContext;

//---------------------------------------------------------------------------
//	@class:
//		CScheduler
//
//	@doc:
//		MT-scheduler for optimization jobs
//
//		Maintaining job dependencies and controlling the order of job execution
//		are the main responsibilities of job scheduler.
//		The scheduler maintains a list of jobs to be run. These are the jobs
//		with no dependencies.  Iteratively, the scheduler picks a runnable
//		(with no dependencies) job and calls CJob::FExecute() function using
//		that job object.
//		The function CJob::FExecute() hides the complexity of job execution
//		from the job scheduler. The expectation is that CJob::FExecute()
//		returns TRUE only when job execution is finished.  Otherwise, job
//		execution is not finished and the scheduler moves the job to a pending
//		state.
//
//		On completion of job J1, the scheduler also takes care of notifying all
//		jobs in the job queue attached to J1 that the execution of J1 is now
//		complete. At this point, a queued job can be terminated if it does not
//		have any further dependencies.
//
//---------------------------------------------------------------------------
class CScheduler
{
	// friend classes
	friend class CJob;

public:
	// enum for job execution result
	enum EJobResult
	{
		EjrRunnable = 0,
		EjrSuspended,
		EjrCompleted,

		EjrSentinel
	};

private:
	// job wrapper; used for inserting job to waiting list (lock-free)
	struct SJobLink
	{
		// link id, set by sync set
		ULONG m_id;

		// pointer to job
		CJob *m_pj;

		// slink for list of waiting jobs
		SLink m_link;

		// initialize link
		void
		Init(CJob *pj)
		{
			m_pj = pj;
			m_link.m_prev = m_link.m_next = NULL;
		}
	};

	// list of jobs waiting to execute
	CSyncList<SJobLink> m_listjlWaiting;

	// pool of job link objects
	CSyncPool<SJobLink> m_spjl;

	// current job counters
	ULONG_PTR m_ulpTotal;
	ULONG_PTR m_ulpRunning;
	ULONG_PTR m_ulpQueued;

	// stats
	ULONG_PTR m_ulpStatsQueued;
	ULONG_PTR m_ulpStatsDequeued;
	ULONG_PTR m_ulpStatsSuspended;
	ULONG_PTR m_ulpStatsCompleted;
	ULONG_PTR m_ulpStatsCompletedQueued;
	ULONG_PTR m_ulpStatsResumed;

#ifdef GPOS_DEBUG
	// list of running jobs
	CList<CJob> m_listjRunning;

	// list of suspended jobs
	CList<CJob> m_listjSuspended;

	// flag indicating if scheduler keeps track
	// of running and suspended jobs
	const BOOL m_fTrackingJobs;
#endif	// GPOS_DEBUG

	// keep executing jobs (if any)
	void ExecuteJobs(CSchedulerContext *psc);

	// process job execution results
	void ProcessJobResult(CJob *pj, CSchedulerContext *psc, BOOL fCompleted);

	// retrieve next job to run
	CJob *PjRetrieve();

	// schedule job for execution
	void Schedule(CJob *pj);

	// prepare for job execution
	void PreExecute(CJob *pj);

	// execute job
	BOOL FExecute(CJob *pj, CSchedulerContext *psc);

	// process job execution outcome
	EJobResult EjrPostExecute(CJob *pj, BOOL fCompleted);

	// resume parent job
	void ResumeParent(CJob *pj);

	// check if all jobs have completed
	BOOL
	IsEmpty() const
	{
		return (0 == m_ulpTotal);
	}

	// no copy ctor
	CScheduler(const CScheduler &);

public:
	// ctor
	CScheduler(CMemoryPool *mp, ULONG ulJobs
#ifdef GPOS_DEBUG
			   ,
			   BOOL fTrackingJobs = true
#endif	// GPOS_DEBUG
	);

	// dtor
	virtual ~CScheduler();

	// main job processing task
	static void *Run(void *);

	// transition job to completed
	void Complete(CJob *pj);

	// transition queued job to completed
	void CompleteQueued(CJob *pj);

	// transition job to suspended
	void Suspend(CJob *pj);

	// add new job for scheduling
	void Add(CJob *pj, CJob *pjParent);

	// resume suspended job
	void Resume(CJob *pj);

	// print statistics
	void PrintStats() const;

#ifdef GPOS_DEBUG
	// get flag for tracking jobs
	BOOL
	FTrackingJobs() const
	{
		return m_fTrackingJobs;
	}

	// print queue
	IOstream &OsPrintActiveJobs(IOstream &);

#endif	// GPOS_DEBUG

};	// class CScheduler

// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CScheduler &
#ifdef GPOS_DEBUG
							 sched
#endif	// GPOS_DEBUG
)
{
#ifdef GPOS_DEBUG
	return sched.OsPrintActiveJobs(os);
#else
	return os;
#endif	// GPOS_DEBUG
}
}  // namespace gpopt

#endif