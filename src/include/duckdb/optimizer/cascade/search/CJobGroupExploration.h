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
		eevStartedExploration,	// started group exploration
		eevNewChildren,			// new children have been added to group
		eevExplored,			// group exploration is complete

		eevSentinel
	};

	// states of group exploration job
	enum EState
	{
		estInitialized = 0,	   // initial state
		estExploringChildren,  // exploring group expressions
		estCompleted,		   // done exploration

		estSentinel
	};

private:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;

	// start exploration action
	static EEvent EevtStartExploration(CSchedulerContext *psc, CJob *pj);

	// explore child group expressions action
	static EEvent EevtExploreChildren(CSchedulerContext *psc, CJob *pj);

	// private copy ctor
	CJobGroupExploration(const CJobGroupExploration &);

public:
	// ctor
	CJobGroupExploration();

	// dtor
	~CJobGroupExploration();

	// initialize job
	void Init(CGroup *pgroup);

	// get first unscheduled expression
	virtual CGroupExpression *
	PgexprFirstUnsched()
	{
		return CJobGroup::PgexprFirstUnschedLogical();
	}

	// schedule exploration jobs for of all new group expressions
	virtual BOOL FScheduleGroupExpressions(CSchedulerContext *psc);

	// schedule a new group exploration job
	static void ScheduleJob(CSchedulerContext *psc, CGroup *pgroup,
							CJob *pjParent);

	// job's function
	virtual BOOL FExecute(CSchedulerContext *psc);

#ifdef GPOS_DEBUG

	// print function
	virtual IOstream &OsPrint(IOstream &os);

	// dump state machine diagram in graphviz format
	virtual IOstream &
	OsDiagramToGraphviz(CMemoryPool *mp, IOstream &os,
						const WCHAR *wszTitle) const
	{
		(void) m_jsm.OsDiagramToGraphviz(mp, os, wszTitle);

		return os;
	}

	// compute unreachable states
	void
	Unreachable(CMemoryPool *mp, EState **ppestate, ULONG *pulSize) const
	{
		m_jsm.Unreachable(mp, ppestate, pulSize);
	}


#endif	// GPOS_DEBUG

	// conversion function
	static CJobGroupExploration *
	PjConvert(CJob *pj)
	{
		GPOS_ASSERT(NULL != pj);
		GPOS_ASSERT(EjtGroupExploration == pj->Ejt());

		return dynamic_cast<CJobGroupExploration *>(pj);
	}


};	// class CJobGroupExploration

}  // namespace gpopt

#endif