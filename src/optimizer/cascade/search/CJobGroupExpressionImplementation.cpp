//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionImplementation.cpp
//
//	@doc:
//		Implementation of group expression implementation job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionImplementation.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

using namespace gpopt;

// State transition diagram for group expression implementation job state machine;
//
// +-------------------------+   eevImplementingChildren
// |     estInitialized:     | --------------------------+
// | EevtImplementChildren() |                           |
// |                         | <-------------------------+
// +-------------------------+
//   |
//   | eevChildrenImplemented
//   v
// +-------------------------+   eevImplementingSelf
// | estChildrenImplemented: | --------------------------+
// |   EevtImplementSelf()   |                           |
// |                         | <-------------------------+
// +-------------------------+
//   |
//   | eevSelfImplemented
//   v
// +-------------------------+
// |   estSelfImplemented:   |
// |     EevtFinalize()      |
// +-------------------------+
//   |
//   | eevFinalized
//   v
// +-------------------------+
// |      estCompleted       |
// +-------------------------+
//
const CJobGroupExpressionImplementation::EEvent rgeev6[CJobGroupExpressionImplementation::estSentinel][CJobGroupExpressionImplementation::estSentinel] =
{
	{CJobGroupExpressionImplementation::eevImplementingChildren, CJobGroupExpressionImplementation::eevChildrenImplemented, CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevSentinel},
	{CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevImplementingSelf, CJobGroupExpressionImplementation::eevSelfImplemented, CJobGroupExpressionImplementation::eevSentinel},
	{CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevFinalized},
	{CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevSentinel},
};

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::CJobGroupExpressionImplementation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupExpressionImplementation::CJobGroupExpressionImplementation()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::~CJobGroupExpressionImplementation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupExpressionImplementation::~CJobGroupExpressionImplementation()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionImplementation::Init(CGroupExpression* pgexpr)
{
	CJobGroupExpression::Init(pgexpr);
	GPOS_ASSERT(pgexpr->Pop()->FLogical());
	m_jsm.Init(rgeev6);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtImplementChildren);
	m_jsm.SetAction(estChildrenImplemented, EevtImplementSelf);
	m_jsm.SetAction(estSelfImplemented, EevtFinalize);
	CJob::SetInit();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::ScheduleApplicableTransformations
//
//	@doc:
//		Schedule transformation jobs for all applicable xforms
//
//---------------------------------------------------------------------------
void CJobGroupExpressionImplementation::ScheduleApplicableTransformations(CSchedulerContext* psc)
{
	// get all applicable xforms
	CXformSet* xform_set = ((LogicalOperator*)m_pgexpr->m_pop.get())->PxfsCandidates();
	// intersect them with required xforms and schedule jobs
	*xform_set &= *(CXformFactory::Pxff()->PxfsImplementation());
	*xform_set &= *(psc->m_peng->PxfsCurrentStage());
	ScheduleTransformations(psc, xform_set);
	SetXformsScheduled();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::ScheduleChildGroupsJobs
//
//	@doc:
//		Schedule implementation jobs for all child groups
//
//---------------------------------------------------------------------------
void CJobGroupExpressionImplementation::ScheduleChildGroupsJobs(CSchedulerContext* psc)
{
	ULONG arity = m_pgexpr->Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		CJobGroupImplementation::ScheduleJob(psc, (*(m_pgexpr))[i], this);
	}
	SetChildrenScheduled();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::EevtImplementChildren
//
//	@doc:
//		Implement child groups
//
//---------------------------------------------------------------------------
CJobGroupExpressionImplementation::EEvent CJobGroupExpressionImplementation::EevtImplementChildren(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionImplementation* pjgei = PjConvert(pjOwner);
	if (!pjgei->FChildrenScheduled())
	{
		pjgei->m_pgexpr->SetState(CGroupExpression::estImplementing);
		pjgei->ScheduleChildGroupsJobs(psc);
		return eevImplementingChildren;
	}
	else
	{
		return eevChildrenImplemented;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::EevtImplementSelf
//
//	@doc:
//		Implement group expression
//
//---------------------------------------------------------------------------
CJobGroupExpressionImplementation::EEvent CJobGroupExpressionImplementation::EevtImplementSelf(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionImplementation* pjgei = PjConvert(pjOwner);
	if (!pjgei->FXformsScheduled())
	{
		pjgei->ScheduleApplicableTransformations(psc);
		return eevImplementingSelf;
	}
	else
	{
		return eevSelfImplemented;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::EevtFinalize
//
//	@doc:
//		Finalize implementation
//
//---------------------------------------------------------------------------
CJobGroupExpressionImplementation::EEvent CJobGroupExpressionImplementation::EevtFinalize(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionImplementation* pjgei = PjConvert(pjOwner);
	pjgei->m_pgexpr->SetState(CGroupExpression::estImplemented);
	return eevFinalized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
BOOL CJobGroupExpressionImplementation::FExecute(CSchedulerContext* psc)
{
	return m_jsm.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::ScheduleJob
//
//	@doc:
//		Schedule a new group expression implementation job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionImplementation::ScheduleJob(CSchedulerContext* psc, CGroupExpression* pgexpr, CJob* pjParent)
{
	CJob* pj = psc->m_pjf->PjCreate(CJob::EjtGroupExpressionImplementation);
	// initialize job
	CJobGroupExpressionImplementation* pjige = PjConvert(pj);
	pjige->Init(pgexpr);
	psc->m_psched->Add(pjige, pjParent);
}