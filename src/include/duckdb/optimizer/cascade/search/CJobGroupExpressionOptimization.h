//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionOptimization.h
//
//	@doc:
//		Explore group expression job
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroupExpressionOptimization_H
#define GPOPT_CJobGroupExpressionOptimization_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

namespace gpopt
{
using namespace gpos;

// prototypes
class CCostContext;

//---------------------------------------------------------------------------
//	@class:
//		CJobGroupExpressionOptimization
//
//	@doc:
//		Group expression optimization job
//
//		Responsible for finding the best plan rooted by a given group
//		expression, such that the identified plan satisfies given required plan
//		properties. Note that a group optimization job entails running a group
//		expression optimization job for each group expression in the underlying
//		group.
//
//---------------------------------------------------------------------------
class CJobGroupExpressionOptimization : public CJobGroupExpression
{
public:
	// transition events of group expression optimization
	enum EEvent
	{
		eevOptimizingChildren,	// child groups optimization is in progress
		eevChildrenOptimized,	// done with children optimization
		eevCheckingEnfdProps,	// check enforceable properties
		eevOptimizingSelf,		// cost computation is in progress
		eevSelfOptimized,		// done with costing group expression
		eevFinalized,			// done with optimization

		eevSentinel
	};

	// states of group expression optimization
	enum EState
	{
		estInitialized = 0,		// initial state
		estOptimizingChildren,	// child groups are under optimization
		estChildrenOptimized,	// child groups are optimized
		estEnfdPropsChecked,	// enforceable properties are checked
		estSelfOptimized,		// group expression cost is computed
		estCompleted,			// done optimization

		estSentinel
	};


private:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;

	// optimization context of the job
	COptimizationContext *m_poc;

	// optimization request number
	ULONG m_ulOptReq;

	// array of child groups optimization contexts
	COptimizationContextArray *m_pdrgpoc;

	// stats context to be used during costing
	IStatisticsArray *m_pdrgpstatCurrentCtxt;

	// array of derived properties of optimal implementations of child groups
	CDrvdPropArray *m_pdrgpdp;

	// optimization order of children
	CPhysical::EChildExecOrder m_eceo;

	// counter of next child group to be optimized
	ULONG m_ulChildIndex;

	// number of children
	ULONG m_ulArity;

	// flag to indicate if optimizing a child has failed
	BOOL m_fChildOptimizationFailed;

	// flag to indicate if current job optimizes a Sequence operator that captures a CTE
	BOOL m_fOptimizeCTESequence;

	// plan properties required from CTE producer based on consumer derived plan properties
	CReqdPropPlan *m_prppCTEProducer;

	// flag to indicate if a child job for optimizing CTE has been scheduled
	BOOL m_fScheduledCTEOptimization;

	// a handle object for required plan properties computation
	CExpressionHandle *m_pexprhdlPlan;

	// a handle object for required relational property computation
	CExpressionHandle *m_pexprhdlRel;

	// initialization routine for child groups optimization
	void InitChildGroupsOptimization(CSchedulerContext *psc);

	// derive plan properties and stats of the child previous to the one being optimized
	void DerivePrevChildProps(CSchedulerContext *psc);

	// compute required plan properties for current child
	void ComputeCurrentChildRequirements(CSchedulerContext *psc);

	// private copy ctor
	CJobGroupExpressionOptimization(const CJobGroupExpressionOptimization &);

	// initialize action
	static EEvent EevtInitialize(CSchedulerContext *psc, CJob *pj);

	// optimize child groups action
	static EEvent EevtOptimizeChildren(CSchedulerContext *psc, CJob *pj);

	// add enforcers to the owning group
	static EEvent EevtAddEnforcers(CSchedulerContext *psc, CJob *pj);

	// optimize group expression action
	static EEvent EevtOptimizeSelf(CSchedulerContext *psc, CJob *pj);

	// finalize action
	static EEvent EevtFinalize(CSchedulerContext *psc, CJob *pj);

	// schedule a new group expression optimization job for CTE optimization
	static BOOL FScheduleCTEOptimization(CSchedulerContext *psc,
										 CGroupExpression *pgexpr,
										 COptimizationContext *poc,
										 ULONG ulOptReq, CJob *pjParent);

protected:
	// schedule transformation jobs for applicable xforms
	virtual void
	ScheduleApplicableTransformations(CSchedulerContext *  // psc
	)
	{
		// no transformations are applicable to this job
	}

	// schedule optimization jobs for all child groups
	virtual void ScheduleChildGroupsJobs(CSchedulerContext *psc);

public:
	// ctor
	CJobGroupExpressionOptimization();

	// dtor
	virtual ~CJobGroupExpressionOptimization();

	// initialize job
	void Init(CGroupExpression *pgexpr, COptimizationContext *poc,
			  ULONG ulOptReq, CReqdPropPlan *prppCTEProducer = NULL);

	// cleanup internal state
	virtual void Cleanup();

	// schedule a new group expression optimization job
	static void ScheduleJob(CSchedulerContext *psc, CGroupExpression *pgexpr,
							COptimizationContext *poc, ULONG ulOptReq,
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
	static CJobGroupExpressionOptimization *
	PjConvert(CJob *pj)
	{
		GPOS_ASSERT(NULL != pj);
		GPOS_ASSERT(EjtGroupExpressionOptimization == pj->Ejt());

		return dynamic_cast<CJobGroupExpressionOptimization *>(pj);
	}

};	// class CJobGroupExpressionOptimization

}  // namespace gpopt

#endif