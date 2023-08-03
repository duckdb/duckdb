//---------------------------------------------------------------------------
//	@filename:
//		CJob.h
//
//	@doc:
//		Interface class for optimization job abstraction
//---------------------------------------------------------------------------
#ifndef GPOPT_CJob_H
#define GPOPT_CJob_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CList.h"

namespace gpopt
{
using namespace gpos;

// prototypes
class CJobQueue;
class CScheduler;
class CSchedulerContext;

//---------------------------------------------------------------------------
//	@class:
//		CJob
//
//	@doc:
//		Superclass of all optimization jobs
//
//		The creation of different types of jobs happens inside CJobFactory
//		class such that each job is given a unique id.
//
//		Job Dependencies:
//		Each job has one parent given by the member variable CJob::m_pjParent.
//		Thus, the dependency graph that defines dependencies is effectively a
//		tree.	The root optimization is scheduled in CEngine::ScheduleMainJob()
//
//		A job can have any number of dependent (child) jobs. Execution within a
//		job cannot proceed as long as there is one or more dependent jobs that
//		are not finished yet.  Pausing a child job does not also allow the
//		parent job to proceed. The number of job dependencies (children) is
//		maintained by the member variable CJob::m_ulpRefs. The increment and
//		decrement of number of children are atomic operations performed by
//		CJob::IncRefs() and CJob::UlpDecrRefs() functions, respectively.
//
//		Job Queue:
//		Each job maintains a job queue CJob::m_pjq of other identical jobs that
//		are created while a given job is executing. For example, when exploring
//		a group, a group exploration job J1 would be executing. Concurrently,
//		another group exploration job J2 (for the same group) may be triggered
//		by another worker. The job J2 would be added in a pending state to the
//		job queue of J1. When J1 terminates, all jobs in its queue are notified
//		to pick up J1 results.
//
//		Job reentrance:
//		All optimization jobs are designed to be reentrant. This means that
//		there is a mechanism to support pausing a given job J1, moving
//		execution to another job J2, and then possibly returning to J1 to
//		resume execution. The re-entry point to J1 must be the same as the
//		point where J1 was paused. This mechanism is implemented using a state
//		machine.
//
//		Job Execution:
//		Each job defines two enumerations: EState to define the different
//		states during job execution and EEvent to define the different events
//		that cause moving from one state to another. These two enumerations are
//		used to define job state machine m_jsm, which is an object of
//		CJobStateMachine class. Note that the states, events and state machines
//		are job-specific. This is why each job class has its own definitions of
//		the job states, events & state machine.
//
//		See CJobStateMachine.h for more information about how jobs are executed
//		using the state machine. Also see CScheduler.h for more information
//		about how job are scheduled.
//
//---------------------------------------------------------------------------
class CJob
{
	// friends
	friend class CJobFactory;
	friend class CJobQueue;
	friend class CScheduler;

public:
	// job type
	enum EJobType
	{ EjtTest = 0, EjtGroupOptimization, EjtGroupImplementation, EjtGroupExploration, EjtGroupExpressionOptimization, EjtGroupExpressionImplementation, EjtGroupExpressionExploration, EjtTransformation, EjtInvalid, EjtSentinel = EjtInvalid };

public:
	// parent job
	CJob* m_pjParent;

	// assigned job queue
	CJobQueue* m_pjq;

	// reference counter
	ULONG_PTR m_ulpRefs;

	// job id - set by job factory
	ULONG m_id;

	// job type
	EJobType m_ejt;

	// flag indicating if job is initialized
	bool m_fInit;

public:
	// ctor
	CJob()
		: m_pjParent(NULL), m_pjq(NULL), m_ulpRefs(0), m_id(0), m_fInit(false)
	{
	}
	
	// private copy ctor
	CJob(const CJob &) = delete;
	
	// dtor
	virtual ~CJob()
	{
	}

public:
	//-------------------------------------------------------------------
	// Interface for CJobFactory
	//-------------------------------------------------------------------
	// set type
	void SetJobType(EJobType ejt)
	{
		m_ejt = ejt;
	}

	//-------------------------------------------------------------------
	// Interface for CScheduler
	//-------------------------------------------------------------------
	// parent accessor
	CJob* PjParent() const
	{
		return m_pjParent;
	}

	// set parent
	void SetParent(CJob* pj)
	{
		m_pjParent = pj;
	}

	// increment reference counter
	void IncRefs()
	{
		m_ulpRefs++;
	}

	// decrement reference counter
	ULONG_PTR UlpDecrRefs()
	{
		return m_ulpRefs--;
	}

	// notify parent of job completion;
	// return true if parent is runnable;
	bool FResumeParent() const;
	
	// id accessor
	ULONG Id() const
	{
		return m_id;
	}

	// reset job
	virtual void Reset();

	// check if job is initialized
	bool FInit() const
	{
		return m_fInit;
	}

	// mark job as initialized
	void SetInit()
	{
		m_fInit = true;
	}

public:
	// actual job execution given a scheduling context
	// returns true if job completes, false if it is suspended
	virtual bool FExecute(CSchedulerContext* psc)
	{
		return true;
	}

	// type accessor
	EJobType Ejt() const
	{
		return m_ejt;
	}

	// job queue accessor
	CJobQueue* Pjq() const
	{
		return m_pjq;
	}

	// set job queue
	void SetJobQueue(CJobQueue* pjq)
	{
		m_pjq = pjq;
	}

	// cleanup internal state
	virtual void Cleanup()
	{
	}

	// link for job queueing
	SLink m_linkQueue;
};	// class CJob
}  // namespace gpopt
#endif