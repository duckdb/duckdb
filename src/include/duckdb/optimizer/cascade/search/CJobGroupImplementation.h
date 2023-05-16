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


namespace gpopt
{
using namespace gpos;


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
	{
		eevExploring,	  // exploration is in progress
		eevExplored,	  // exploration is complete
		eevImplementing,  // implementation is in progress
		eevImplemented,	  // implementation is complete

		eevSentinel
	};

	// states of group implementation job
	enum EState
	{
		estInitialized = 0,		  // initial state
		estImplementingChildren,  // implementing group expressions
		estCompleted,			  // done implementation

		estSentinel
	};

private:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;

	// start implementation action
	static EEvent EevtStartImplementation(CSchedulerContext *psc, CJob *pj);

	// implement child group expressions action
	static EEvent EevtImplementChildren(CSchedulerContext *psc, CJob *pj);

	// private copy ctor
	CJobGroupImplementation(const CJobGroupImplementation &);

public:
	// ctor
	CJobGroupImplementation();

	// dtor
	~CJobGroupImplementation();

	// initialize job
	void Init(CGroup *pgroup);

	// get first unscheduled expression
	virtual CGroupExpression *
	PgexprFirstUnsched()
	{
		return CJobGroup::PgexprFirstUnschedLogical();
	}

	// schedule implementation jobs for of all new group expressions
	virtual BOOL FScheduleGroupExpressions(CSchedulerContext *psc);

	// schedule a new group implementation job
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
	static CJobGroupImplementation *
	PjConvert(CJob *pj)
	{
		GPOS_ASSERT(NULL != pj);
		GPOS_ASSERT(EjtGroupImplementation == pj->Ejt());

		return dynamic_cast<CJobGroupImplementation *>(pj);
	}


};	// class CJobGroupImplementation

}  // namespace gpopt

#endif