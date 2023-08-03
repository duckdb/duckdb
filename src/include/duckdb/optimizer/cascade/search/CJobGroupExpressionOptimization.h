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
#include "duckdb/common/vector.hpp"

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

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
	{ eevOptimizingChildren, eevChildrenOptimized, eevCheckingEnfdProps, eevOptimizingSelf, eevSelfOptimized, eevFinalized, eevSentinel };

	// states of group expression optimization
	enum EState
	{ estInitialized = 0, estOptimizingChildren, estChildrenOptimized, estEnfdPropsChecked, estSelfOptimized, estCompleted, estSentinel };

public:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;

	// optimization context of the job
	COptimizationContext* m_poc;

	// optimization request number
	ULONG m_ulOptReq;

	// array of child groups optimization contexts
	duckdb::vector<COptimizationContext*> m_pdrgpoc;

	// array of derived properties of optimal implementations of child groups
	duckdb::vector<CDrvdProp*> m_pdrgpdp;

	// counter of next child group to be optimized
	ULONG m_ulChildIndex;

	// number of children
	ULONG m_ulArity;

	// flag to indicate if optimizing a child has failed
	BOOL m_fChildOptimizationFailed;

	// flag to indicate if current job optimizes a Sequence operator that captures a CTE
	BOOL m_fOptimizeCTESequence;

	// a handle object for required plan properties computation
	CExpressionHandle* m_pexprhdlPlan;

	// a handle object for required relational property computation
	CExpressionHandle* m_pexprhdlRel;

public:
	// initialization routine for child groups optimization
	void InitChildGroupsOptimization(CSchedulerContext* psc);

	// derive plan properties and stats of the child previous to the one being optimized
	void DerivePrevChildProps(CSchedulerContext* psc);

	// compute required plan properties for current child
	void ComputeCurrentChildRequirements(CSchedulerContext* psc);

	// no copy ctor
	CJobGroupExpressionOptimization(const CJobGroupExpressionOptimization &) = delete;

	// initialize action
	static EEvent EevtInitialize(CSchedulerContext* psc, CJob* pj);

	// optimize child groups action
	static EEvent EevtOptimizeChildren(CSchedulerContext* psc, CJob* pj);

	// add enforcers to the owning group
	static EEvent EevtAddEnforcers(CSchedulerContext* psc, CJob* pj);

	// optimize group expression action
	static EEvent EevtOptimizeSelf(CSchedulerContext* psc, CJob* pj);

	// finalize action
	static EEvent EevtFinalize(CSchedulerContext* psc, CJob* pj);

public:
	// schedule transformation jobs for applicable xforms
	virtual void ScheduleApplicableTransformations(CSchedulerContext* psc)
	{
	}

	// schedule optimization jobs for all child groups
	virtual void ScheduleChildGroupsJobs(CSchedulerContext* psc);

public:
	// ctor
	CJobGroupExpressionOptimization();

	// dtor
	virtual ~CJobGroupExpressionOptimization();

	// initialize job
	void Init(CGroupExpression* pgexpr, COptimizationContext* poc, ULONG ulOptReq);

	// cleanup internal state
	virtual void Cleanup();

	// schedule a new group expression optimization job
	static void ScheduleJob(CSchedulerContext* psc, CGroupExpression* pgexpr, COptimizationContext* poc, ULONG ulOptReq, CJob* pjParent);

	// job's function
	BOOL FExecute(CSchedulerContext*psc);

	// conversion function
	static CJobGroupExpressionOptimization* PjConvert(CJob* pj)
	{
		return dynamic_cast<CJobGroupExpressionOptimization*>(pj);
	}
};	// class CJobGroupExpressionOptimization
}  // namespace gpopt

#endif