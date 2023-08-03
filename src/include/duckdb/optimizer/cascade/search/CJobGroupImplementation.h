//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupImplementation.h
//
//	@doc:
//		Implement group job
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroupImplementation_H
#define GPOPT_CJobGroupImplementation_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJobGroup.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

using namespace gpos;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CJobGroupImplementation
//
//	@doc:
//		Group implementation job
//
//		Responsible for creating the physical implementations of all
//		expressions in a given group. This happens by firing implementation
//		transformations that perform physical implementation (e.g.,
//		implementing InnerJoin as HashJoin)
//
//---------------------------------------------------------------------------
class CJobGroupImplementation : public CJobGroup
{
public:
	// transition events of group implementation
	enum EEvent
	{ eevExploring, eevExplored, eevImplementing, eevImplemented, eevSentinel };

	// states of group implementation job
	enum EState
	{ estInitialized = 0, estImplementingChildren, estCompleted, estSentinel };

	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

public:
	// job state machine
	JSM m_jsm;

public:
	// ctor
	CJobGroupImplementation();
	
	// private copy ctor
	CJobGroupImplementation(const CJobGroupImplementation &) = delete;
	
	// dtor
	~CJobGroupImplementation();

public:
	// initialize job
	void Init(CGroup* pgroup);

	// get first unscheduled expression
	virtual list<CGroupExpression*>::iterator PgexprFirstUnsched()
	{
		return CJobGroup::PgexprFirstUnschedLogical();
	}

	// schedule implementation jobs for of all new group expressions
	virtual bool FScheduleGroupExpressions(CSchedulerContext* psc);

	// schedule a new group implementation job
	static void ScheduleJob(CSchedulerContext* psc, CGroup* pgroup, CJob* pjParent);

	// job's function
	virtual bool FExecute(CSchedulerContext* psc);

	// conversion function
	static CJobGroupImplementation* PjConvert(CJob* pj)
	{
		return dynamic_cast<CJobGroupImplementation*>(pj);
	}

	// start implementation action
	static EEvent EevtStartImplementation(CSchedulerContext* psc, CJob* pj);

	// implement child group expressions action
	static EEvent EevtImplementChildren(CSchedulerContext* psc, CJob* pj);
};	// class CJobGroupImplementation
}  // namespace gpopt
#endif