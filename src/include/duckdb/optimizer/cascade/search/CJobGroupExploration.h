//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExploration.h
//
//	@doc:
//		Group exploration job
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroupExploration_H
#define GPOPT_CJobGroupExploration_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJobGroup.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJobGroupExploration
//
//	@doc:
//		Group exploration job
//
//		Responsible for creating the logical rewrites of all expressions in a
//		given group. This happens by firing exploration transformations that
//		perform logical rewriting (e.g., rewriting InnerJoin(A,B) as
//		InnerJoin(B,A))
//
//---------------------------------------------------------------------------
class CJobGroupExploration : public CJobGroup
{
public:
	// transition events of group exploration
	enum EEvent
	{
		eevStartedExploration, eevNewChildren, eevExplored, eevSentinel
	};

	// states of group exploration job
	enum EState
	{
		estInitialized = 0, estExploringChildren, estCompleted, estSentinel
	};

public:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;

public:
	// ctor
	CJobGroupExploration();

	// no copy ctor
	CJobGroupExploration(const CJobGroupExploration &) = delete;

	// dtor
	~CJobGroupExploration();

	// initialize job
	void Init(CGroup* pgroup);

	// get first unscheduled expression
	virtual list<CGroupExpression*>::iterator PgexprFirstUnsched()
	{
		return CJobGroup::PgexprFirstUnschedLogical();
	}

	// schedule exploration jobs for of all new group expressions
	virtual bool FScheduleGroupExpressions(CSchedulerContext* psc);

	// schedule a new group exploration job
	static void ScheduleJob(CSchedulerContext* psc, CGroup* pgroup, CJob* pjParent);

	// job's function
	virtual bool FExecute(CSchedulerContext* psc);

	// conversion function
	static CJobGroupExploration* PjConvert(CJob* pj)
	{
		return dynamic_cast<CJobGroupExploration*>(pj);
	}

	// start exploration action
	static EEvent EevtStartExploration(CSchedulerContext* psc, CJob* pj);

	// explore child group expressions action
	static EEvent EevtExploreChildren(CSchedulerContext* psc, CJob* pj);
};	// class CJobGroupExploration
}  // namespace gpopt
#endif