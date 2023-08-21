//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionOptimization.cpp
//
//	@doc:
//		Implementation of group expression optimization job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionOptimization.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"

using namespace gpopt;

// State transition diagram for group expression optimization job state machine;
//
//                 +------------------------+
//                 |    estInitialized:     |
//  +------------- |    EevtInitialize()    |
//  |              +------------------------+
//  |                |
//  |                | eevOptimizingChildren
//  |                v
//  |              +------------------------+   eevOptimizingChildren
//  |              | estOptimizingChildren: | ------------------------+
//  |              | EevtOptimizeChildren() |                         |
//  +------------- |                        | <-----------------------+
//  |              +------------------------+
//  |                |
//  | eevFinalized   | eevChildrenOptimized
//  |                v
//  |              +------------------------+
//  |              | estChildrenOptimized:  |
//  +------------- |   EevtAddEnforcers()   |
//  |              +------------------------+
//  |                |
//  |                | eevOptimizingSelf
//  |                v
//  |              +------------------------+   eevOptimizingSelf
//  |              |  estEnfdPropsChecked:  | ------------------------+
//  |              |   EevtOptimizeSelf()   |                         |
//  +------------- |                        | <-----------------------+
//  |              +------------------------+
//  |                |
//  |                | eevSelfOptimized
//  |                v
//  |              +------------------------+
//  |              |   estSelfOptimized:    |
//  | eevFinalized |     EevtFinalize()     |
//  |              +------------------------+
//  |                |
//  |                |
//  |                |
//  |                |
//  +----------------+
//                   |
//                   |
//                   | eevFinalized
//                   v
//                 +------------------------+
//                 |      estCompleted      |
//                 +------------------------+
//
const CJobGroupExpressionOptimization::EEvent rgeev[CJobGroupExpressionOptimization::estSentinel][CJobGroupExpressionOptimization::estSentinel] = {
			 {// estInitialized
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevOptimizingChildren,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevFinalized},
			 {// estOptimizingChildren
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevOptimizingChildren,
			  CJobGroupExpressionOptimization::eevChildrenOptimized,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevFinalized},
			 {// estChildrenOptimized
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevOptimizingSelf,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevFinalized},
			 {// estEnfdPropsChecked
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevOptimizingSelf,
			  CJobGroupExpressionOptimization::eevSelfOptimized,
			  CJobGroupExpressionOptimization::eevFinalized},
			 {// estSelfOptimized
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevFinalized},
			 {// estCompleted
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel,
			  CJobGroupExpressionOptimization::eevSentinel},
};

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::CJobGroupExpressionOptimization
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::CJobGroupExpressionOptimization()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::~CJobGroupExpressionOptimization
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::~CJobGroupExpressionOptimization()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::Init(CGroupExpression* pgexpr, COptimizationContext* poc, ULONG ulOptReq)
{
	CJobGroupExpression::Init(pgexpr);
	m_jsm.Init(rgeev);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtInitialize);
	m_jsm.SetAction(estOptimizingChildren, EevtOptimizeChildren);
	m_jsm.SetAction(estChildrenOptimized, EevtAddEnforcers);
	m_jsm.SetAction(estEnfdPropsChecked, EevtOptimizeSelf);
	m_jsm.SetAction(estSelfOptimized, EevtFinalize);
	m_pexprhdlPlan = NULL;
	m_pexprhdlRel = NULL;
	m_ulArity = pgexpr->Arity();
	m_ulChildIndex = gpos::ulong_max;
	m_poc = poc;
	m_ulOptReq = ulOptReq;
	m_fChildOptimizationFailed = false;
	CJob::SetInit();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::Cleanup
//
//	@doc:
//		Cleanup allocated memory
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::Cleanup()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::InitChildGroupsOptimization
//
//	@doc:
//		Initialization routine for child groups optimization
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::InitChildGroupsOptimization(CSchedulerContext* psc)
{
	// initialize required plan properties computation
	m_pexprhdlPlan = new CExpressionHandle();
	m_pexprhdlPlan->Attach(m_pgexpr);
	if (0 < m_ulArity)
	{
		m_ulChildIndex = m_pexprhdlPlan->UlFirstOptimizedChildIndex();
	}
	m_pexprhdlPlan->DeriveProps(NULL);
	m_pexprhdlPlan->InitReqdProps(m_poc->m_prpp);
	// initialize required relational properties computation
	m_pexprhdlRel = new CExpressionHandle();
	CGroupExpression* pgexprForStats = m_pgexpr->m_pgroup->PgexprBestPromise(m_pgexpr);
	if (NULL != pgexprForStats)
	{
		m_pexprhdlRel->Attach(pgexprForStats);
		m_pexprhdlRel->DeriveProps(NULL);
		m_pexprhdlRel->ComputeReqdProps((CReqdProp*)m_poc->m_prprel, 0);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::EevtInitialize
//
//	@doc:
//		Initialize internal data structures;
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::EEvent CJobGroupExpressionOptimization::EevtInitialize(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionOptimization* pjgeo = PjConvert(pjOwner);
	CExpressionHandle exprhdl;
	exprhdl.Attach(pjgeo->m_pgexpr);
	exprhdl.DeriveProps(NULL);
	if (!psc->m_peng->FCheckReqdProps(exprhdl, pjgeo->m_poc->m_prpp, pjgeo->m_ulOptReq))
	{
		return eevFinalized;
	}
	// check if job can be early terminated without optimizing any child
	double costLowerBound = GPOPT_INVALID_COST;
	if (psc->m_peng->FSafeToPrune(pjgeo->m_pgexpr, pjgeo->m_poc->m_prpp, NULL, gpos::ulong_max, &costLowerBound))
	{
		duckdb::vector<COptimizationContext*> v;
		(void) pjgeo->m_pgexpr->PccComputeCost(pjgeo->m_poc, pjgeo->m_ulOptReq, v, true, costLowerBound);
		return eevFinalized;
	}
	pjgeo->InitChildGroupsOptimization(psc);
	return eevOptimizingChildren;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::DerivePrevChildProps
//
//	@doc:
//		Derive plan properties and stats of the child previous to
//		the one being optimized
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::DerivePrevChildProps(CSchedulerContext* psc)
{
	ULONG ulPrevChildIndex = m_pexprhdlPlan->UlPreviousOptimizedChildIndex(m_ulChildIndex);
	// retrieve plan properties of the optimal implementation of previous child group
	CGroup* pgroupChild = (*m_pgexpr)[ulPrevChildIndex];
	if (pgroupChild->m_fScalar)
	{
		// exit if previous child is a scalar group
		return;
	}
	COptimizationContext* pocChild = pgroupChild->PocLookupBest(psc->m_peng->UlSearchStages(), m_pexprhdlPlan->Prpp(ulPrevChildIndex));
	CCostContext* pccChildBest = pocChild->m_pccBest;
	if (NULL == pccChildBest)
	{
		// failed to optimize child
		m_fChildOptimizationFailed = true;
		return;
	}
	// check if job can be early terminated after previous children have been optimized
	double costLowerBound = GPOPT_INVALID_COST;
	if (psc->m_peng->FSafeToPrune(m_pgexpr, m_poc->m_prpp, pccChildBest, ulPrevChildIndex, &costLowerBound))
	{
		duckdb::vector<COptimizationContext*> v;
		// failed to optimize child due to cost bounding
		(void) m_pgexpr->PccComputeCost(m_poc, m_ulOptReq, v, true, costLowerBound);
		m_fChildOptimizationFailed = true;
		return;
	}
	CExpressionHandle exprhdl;
	exprhdl.Attach(pccChildBest);
	exprhdl.DerivePlanPropsForCostContext();
	m_pdrgpdp.emplace_back(exprhdl.Pdp());
	/* I comment here */
	// copy stats of child's best cost context to current stats context
	// IStatistics *pstat = pccChildBest->Pstats();
	// pstat->AddRef();
	// m_pdrgpstatCurrentCtxt->Append(pstat);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::ComputeCurrentChildRequirements
//
//	@doc:
//		Compute required plan properties for current child
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::ComputeCurrentChildRequirements(CSchedulerContext* psc)
{
	// derive plan properties of previous child group
	if (m_ulChildIndex != m_pexprhdlPlan->UlFirstOptimizedChildIndex())
	{
		DerivePrevChildProps(psc);
		if (m_fChildOptimizationFailed)
		{
			return;
		}
	}
	// compute required plan properties of current child group
	m_pexprhdlPlan->ComputeChildReqdProps(m_ulChildIndex, m_pdrgpdp, m_ulOptReq);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::ScheduleChildGroupsJobs
//
//	@doc:
//		Schedule optimization job for the next child group; skip child groups
//		as they do not require optimization
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::ScheduleChildGroupsJobs(CSchedulerContext* psc)
{
	CGroup* pgroupChild = (*m_pgexpr)[m_ulChildIndex];
	if (pgroupChild->m_fScalar)
	{
		if (!m_pexprhdlPlan->FNextChildIndex(&m_ulChildIndex))
		{
			// child group optimization is complete
			SetChildrenScheduled();
		}
		return;
	}
	ComputeCurrentChildRequirements(psc);
	if (m_fChildOptimizationFailed)
	{
		return;
	}
	// compute required relational properties
	CReqdPropRelational* prprel = new CReqdPropRelational(); // m_pexprhdlRel->GetReqdRelationalProps(m_ulChildIndex);
	// schedule optimization job for current child group
	COptimizationContext* pocChild = new COptimizationContext(pgroupChild, m_pexprhdlPlan->Prpp(m_ulChildIndex), prprel, psc->m_peng->UlCurrSearchStage());
	if (pgroupChild == m_pgexpr->m_pgroup && pocChild->Matches(m_poc))
	{
		// this is to prevent deadlocks, child context cannot be the same as parent context
		m_fChildOptimizationFailed = true;
		return;
	}
	CJobGroupOptimization::ScheduleJob(psc, pgroupChild, m_pgexpr, pocChild, this);
	// advance to next child
	if (!m_pexprhdlPlan->FNextChildIndex(&m_ulChildIndex))
	{
		// child group optimization is complete
		SetChildrenScheduled();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::EevtOptimizeChildren
//
//	@doc:
//		Optimize child groups
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::EEvent CJobGroupExpressionOptimization::EevtOptimizeChildren(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionOptimization* pjgeo = PjConvert(pjOwner);
	if (0 < pjgeo->m_ulArity && !pjgeo->FChildrenScheduled())
	{
		pjgeo->ScheduleChildGroupsJobs(psc);
		if (pjgeo->m_fChildOptimizationFailed)
		{
			// failed to optimize child, terminate job
			pjgeo->Cleanup();
			return eevFinalized;
		}
		return eevOptimizingChildren;
	}
	return eevChildrenOptimized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::EevtAddEnforcers
//
//	@doc:
//		Add required enforcers to owning group
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::EEvent CJobGroupExpressionOptimization::EevtAddEnforcers(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionOptimization* pjgeo = PjConvert(pjOwner);
	// build child contexts array
	pjgeo->m_pdrgpoc = psc->m_peng->PdrgpocChildren(*pjgeo->m_pexprhdlPlan);
	// enforce physical properties
	BOOL fCheckEnfdProps = psc->m_peng->FCheckEnfdProps(pjgeo->m_pgexpr, pjgeo->m_poc, pjgeo->m_ulOptReq, pjgeo->m_pdrgpoc);
	if (fCheckEnfdProps)
	{
		// No new enforcers group expressions were added because they were either
		// optional or unnecessary. So, move on to optimize the current group
		// expression.
		return eevOptimizingSelf;
	}
	// Either adding enforcers was prohibited or at least one enforcer was added
	// because it was required. In any case, this job can be finalized, since
	// optimizing the current group expression is not needed (because of the
	// prohibition) or the newly created enforcer group expression job will get
	// to it later on.
	pjgeo->Cleanup();
	return eevFinalized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::EevtOptimizeSelf
//
//	@doc:
//		Optimize group expression
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::EEvent CJobGroupExpressionOptimization::EevtOptimizeSelf(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionOptimization* pjgeo = PjConvert(pjOwner);
	// compute group expression cost under current context
	COptimizationContext* poc = pjgeo->m_poc;
	CGroupExpression* pgexpr = pjgeo->m_pgexpr;
	duckdb::vector<COptimizationContext*> pdrgpoc = pjgeo->m_pdrgpoc;
	ULONG ulOptReq = pjgeo->m_ulOptReq;
	CCostContext* pcc = pgexpr->PccComputeCost(poc, ulOptReq, pdrgpoc, false, 0.0);
	if (nullptr == pcc)
	{
		pjgeo->Cleanup();
		// failed to create cost context, terminate optimization job
		return eevFinalized;
	}
	pgexpr->m_pgroup->UpdateBestCost(poc, pcc);
	return eevSelfOptimized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::EevtFinalize
//
//	@doc:
//		Finalize optimization
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::EEvent CJobGroupExpressionOptimization::EevtFinalize(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionOptimization* pjgeo = PjConvert(pjOwner);
	pjgeo->Cleanup();
	return eevFinalized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
BOOL CJobGroupExpressionOptimization::FExecute(CSchedulerContext* psc)
{
	return m_jsm.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::ScheduleJob
//
//	@doc:
//		Schedule a new group expression optimization job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::ScheduleJob(CSchedulerContext* psc, CGroupExpression* pgexpr, COptimizationContext* poc, ULONG ulOptReq, CJob* pjParent)
{
	CJob* pj = psc->m_pjf->PjCreate(CJob::EjtGroupExpressionOptimization);
	// initialize job
	CJobGroupExpressionOptimization* pjgeo = PjConvert(pj);
	pjgeo->Init(pgexpr, poc, ulOptReq);
	psc->m_psched->Add(pjgeo, pjParent);
}