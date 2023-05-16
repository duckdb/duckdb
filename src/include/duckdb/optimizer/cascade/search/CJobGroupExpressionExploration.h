//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionExploration.h
//
//	@doc:
//		Explore group expression job
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroupExpressionExploration_H
#define GPOPT_CJobGroupExpressionExploration_H

#include "duckdb/optimizer/cascade/search/CJobGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJobGroupExpressionExploration
//
//	@doc:
//		Explore group expression optimization job
//
//		Responsible for creating the logical rewrites of a given group
//		expression. Note that a group exploration job entails running a group
//		expression exploration job for each group expression in the underlying
//		group.
//
//---------------------------------------------------------------------------
class CJobGroupExpressionExploration : public CJobGroupExpression
{
public:
	// transition events of group expression exploration
	enum EEvent
	{
		eevExploringChildren,  // child groups exploration is in progress
		eevChildrenExplored,   // done with children exploration
		eevExploringSelf,	   // self exploration is in progress
		eevSelfExplored,	   // done with exploring group expression
		eevFinalized,		   // done with exploration

		eevSentinel
	};

	// states of group expression exploration
	enum EState
	{
		estInitialized = 0,	  // initial state
		estChildrenExplored,  // child groups explored
		estSelfExplored,	  // group expression explored
		estCompleted,		  // done exploration

		estSentinel
	};

private:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;

	// explore child groups action
	static EEvent EevtExploreChildren(CSchedulerContext *psc, CJob *pj);

	// explore group expression action
	static EEvent EevtExploreSelf(CSchedulerContext *psc, CJob *pj);

	// finalize action
	static EEvent EevtFinalize(CSchedulerContext *psc, CJob *pj);

	// private copy ctor
	CJobGroupExpressionExploration(const CJobGroupExpressionExploration &);

protected:
	// schedule transformation jobs for applicable xforms
	virtual void ScheduleApplicableTransformations(CSchedulerContext *psc);

	// schedule exploration jobs for all child groups
	virtual void ScheduleChildGroupsJobs(CSchedulerContext *psc);

public:
	// ctor
	CJobGroupExpressionExploration();

	// dtor
	virtual ~CJobGroupExpressionExploration();

	// initialize job
	void Init(CGroupExpression *pgexpr);

	// schedule a new group expression exploration job
	static void ScheduleJob(CSchedulerContext *psc, CGroupExpression *pgexpr,
							CJob *pjParent);

	// job's main function
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
	static CJobGroupExpressionExploration *
	PjConvert(CJob *pj)
	{
		GPOS_ASSERT(NULL != pj);
		GPOS_ASSERT(EjtGroupExpressionExploration == pj->Ejt());

		return dynamic_cast<CJobGroupExpressionExploration *>(pj);
	}

};	// class CJobGroupExpressionExploration

}  // namespace gpopt

#endif