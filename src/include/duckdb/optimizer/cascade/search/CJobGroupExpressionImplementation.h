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

namespace gpopt
{
using namespace gpos;


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
	{
		eevImplementingChildren,  // child groups implementation is in progress
		eevChildrenImplemented,	  // done with children implementation
		eevImplementingSelf,	  // self implementation is in progress
		eevSelfImplemented,		  // done with implementing group expression
		eevFinalized,			  // done with implementation

		eevSentinel
	};

	// states of group expression implementation
	enum EState
	{
		estInitialized = 0,		 // initial state
		estChildrenImplemented,	 // child groups implemented
		estSelfImplemented,		 // group expression implemented
		estCompleted,			 // done implementation

		estSentinel
	};


private:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;

	// implement child groups action
	static EEvent EevtImplementChildren(CSchedulerContext *psc, CJob *pj);

	// implement group expression action
	static EEvent EevtImplementSelf(CSchedulerContext *psc, CJob *pj);

	// finalize action
	static EEvent EevtFinalize(CSchedulerContext *psc, CJob *pj);

	// private copy ctor
	CJobGroupExpressionImplementation(
		const CJobGroupExpressionImplementation &);

protected:
	// schedule transformation jobs for applicable xforms
	virtual void ScheduleApplicableTransformations(CSchedulerContext *psc);

	// schedule implementation jobs for all child groups
	virtual void ScheduleChildGroupsJobs(CSchedulerContext *psc);

public:
	// ctor
	CJobGroupExpressionImplementation();

	// dtor
	virtual ~CJobGroupExpressionImplementation();

	// initialize job
	void Init(CGroupExpression *pgexpr);

	// schedule a new group expression implementation job
	static void ScheduleJob(CSchedulerContext *psc, CGroupExpression *pgexpr,
							CJob *pjParent);

	// job's function
	BOOL FExecute(CSchedulerContext *psc);

#ifdef GPOS_DEBUG

	// print function
	IOstream &OsPrint(IOstream &os);

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
	static CJobGroupExpressionImplementation *
	PjConvert(CJob *pj)
	{
		GPOS_ASSERT(NULL != pj);
		GPOS_ASSERT(EjtGroupExpressionImplementation == pj->Ejt());

		return dynamic_cast<CJobGroupExpressionImplementation *>(pj);
	}

};	// class CJobGroupExpressionImplementation

}  // namespace gpopt

#endif