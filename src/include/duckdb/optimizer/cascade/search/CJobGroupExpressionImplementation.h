//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionImplementation.h
//
//	@doc:
//		Explore group expression job
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroupExpressionImplementation_H
#define GPOPT_CJobGroupExpressionImplementation_H

#include "duckdb/optimizer/cascade/search/CJobGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

using namespace gpos;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CJobGroupExpressionImplementation
//
//	@doc:
//		Implement group expression optimization job
//
//		Responsible for creating the physical implementations of a given group
//		expression. Note that a group implementation job entails running a
//		group expression implementation job for each group expression in the
//		underlying group.
//
//---------------------------------------------------------------------------
class CJobGroupExpressionImplementation : public CJobGroupExpression
{
public:
	// transition events of group expression implementation
	enum EEvent
	{ eevImplementingChildren, eevChildrenImplemented, eevImplementingSelf, eevSelfImplemented, eevFinalized, eevSentinel };

	// states of group expression implementation
	enum EState
	{ estInitialized = 0, estChildrenImplemented, estSelfImplemented, estCompleted, estSentinel };

public:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;
	
public:
	// ctor
	CJobGroupExpressionImplementation();
	
	// no copy ctor
	CJobGroupExpressionImplementation(const CJobGroupExpressionImplementation &) = delete;
	
	// dtor
	virtual ~CJobGroupExpressionImplementation();

public:
	// schedule transformation jobs for applicable xforms
	virtual void ScheduleApplicableTransformations(CSchedulerContext* psc);

	// schedule implementation jobs for all child groups
	virtual void ScheduleChildGroupsJobs(CSchedulerContext* psc);
	
	// implement child groups action
	static EEvent EevtImplementChildren(CSchedulerContext* psc, CJob* pj);

	// implement group expression action
	static EEvent EevtImplementSelf(CSchedulerContext* psc, CJob* pj);

	// finalize action
	static EEvent EevtFinalize(CSchedulerContext* psc, CJob* pj);

	// initialize job
	void Init(CGroupExpression* pgexpr);

	// schedule a new group expression implementation job
	static void ScheduleJob(CSchedulerContext* psc, CGroupExpression* pgexpr, CJob* pjParent);

	// job's function
	bool FExecute(CSchedulerContext* psc);

	// conversion function
	static CJobGroupExpressionImplementation* PjConvert(CJob* pj)
	{
		return dynamic_cast<CJobGroupExpressionImplementation*>(pj);
	}
};	// class CJobGroupExpressionImplementation
}  // namespace gpopt
#endif