//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionExploration.cpp
//
//	@doc:
//		Implementation of group expression exploration job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionExploration.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExploration.h"
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

namespace gpopt
{
// State transition diagram for group expression exploration job state machine;
//
// +-----------------------+   eevExploringChildren
// |    estInitialized:    | -----------------------+
// | EevtExploreChildren() |                        |
// |                       | <----------------------+
// +-----------------------+
//   |
//   | eevChildrenExplored
//   v
// +-----------------------+   eevExploringSelf
// | estChildrenExplored:  | -----------------------+
// |   EevtExploreSelf()   |                        |
// |                       | <----------------------+
// +-----------------------+
//   |
//   | eevSelfExplored
//   v
// +-----------------------+
// |   estSelfExplored:    |
// |    EevtFinalize()     |
// +-----------------------+
//   |
//   | eevFinalized
//   v
// +-----------------------+
// |     estCompleted      |
// +-----------------------+
//
const CJobGroupExpressionExploration::EEvent rgeev3[CJobGroupExpressionExploration::estSentinel][CJobGroupExpressionExploration::estSentinel] =
{
	{CJobGroupExpressionExploration::eevExploringChildren, CJobGroupExpressionExploration::eevChildrenExplored, CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevSentinel},
	{CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevExploringSelf, CJobGroupExpressionExploration::eevSelfExplored, CJobGroupExpressionExploration::eevSentinel},
	{CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevFinalized},
	{CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevSentinel},
};

#ifdef GPOS_DEBUG
// names for states
const WCHAR rgwszStates[CJobGroupExpressionExploration::estSentinel]
					   [GPOPT_FSM_NAME_LENGTH] = {
						   GPOS_WSZ_LIT("initialized"),
						   GPOS_WSZ_LIT("children explored"),
						   GPOS_WSZ_LIT("self explored"),
						   GPOS_WSZ_LIT("completed")};

// names for events
const WCHAR rgwszEvents[CJobGroupExpressionExploration::eevSentinel]
					   [GPOPT_FSM_NAME_LENGTH] = {
						   GPOS_WSZ_LIT("exploring children groups"),
						   GPOS_WSZ_LIT("explored children groups"),
						   GPOS_WSZ_LIT("applying exploration xforms"),
						   GPOS_WSZ_LIT("applied exploration xforms"),
						   GPOS_WSZ_LIT("finalized")};
#endif	// GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::CJobGroupExpressionExploration
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupExpressionExploration::CJobGroupExpressionExploration()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::~CJobGroupExpressionExploration
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupExpressionExploration::~CJobGroupExpressionExploration()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionExploration::Init(CGroupExpression* pgexpr)
{
	CJobGroupExpression::Init(pgexpr);
	m_jsm.Init(rgeev3);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtExploreChildren);
	m_jsm.SetAction(estChildrenExplored, EevtExploreSelf);
	m_jsm.SetAction(estSelfExplored, EevtFinalize);
	CJob::SetInit();
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::ScheduleApplicableTransformations
//
//	@doc:
//		Schedule transformation jobs for all applicable xforms
//
//---------------------------------------------------------------------------
void CJobGroupExpressionExploration::ScheduleApplicableTransformations(CSchedulerContext* psc)
{
	// get all applicable xforms
	CXformSet* xform_set = ((LogicalOperator*)m_pgexpr->m_pop.get())->PxfsCandidates();
	// intersect them with required xforms and schedule jobs
	*xform_set &= *(CXformFactory::Pxff()->PxfsExploration());
	*xform_set &= *(psc->m_peng->PxfsCurrentStage());
	ScheduleTransformations(psc, xform_set);
	xform_set->reset();
	SetXformsScheduled();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::ScheduleChildGroupsJobs
//
//	@doc:
//		Schedule exploration jobs for all child groups
//
//---------------------------------------------------------------------------
void CJobGroupExpressionExploration::ScheduleChildGroupsJobs(CSchedulerContext* psc)
{
	ULONG arity = m_pgexpr->Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		CJobGroupExploration::ScheduleJob(psc, (*(m_pgexpr))[i], this);
	}
	SetChildrenScheduled();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::EevtExploreChildren
//
//	@doc:
//		Explore child groups
//
//---------------------------------------------------------------------------
CJobGroupExpressionExploration::EEvent CJobGroupExpressionExploration::EevtExploreChildren(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionExploration* pjgee = PjConvert(pjOwner);
	if (!pjgee->FChildrenScheduled())
	{
		pjgee->m_pgexpr->SetState(CGroupExpression::estExploring);
		pjgee->ScheduleChildGroupsJobs(psc);
		return eevExploringChildren;
	}
	else
	{
		return eevChildrenExplored;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::EevtExploreSelf
//
//	@doc:
//		Explore group expression
//
//---------------------------------------------------------------------------
CJobGroupExpressionExploration::EEvent CJobGroupExpressionExploration::EevtExploreSelf(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionExploration* pjgee = PjConvert(pjOwner);
	if (!pjgee->FXformsScheduled())
	{
		pjgee->ScheduleApplicableTransformations(psc);
		return eevExploringSelf;
	}
	else
	{
		return eevSelfExplored;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::EevtFinalize
//
//	@doc:
//		Finalize exploration
//
//---------------------------------------------------------------------------
CJobGroupExpressionExploration::EEvent CJobGroupExpressionExploration::EevtFinalize(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupExpressionExploration* pjgee = PjConvert(pjOwner);
	pjgee->m_pgexpr->SetState(CGroupExpression::estExplored);
	return eevFinalized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
bool CJobGroupExpressionExploration::FExecute(CSchedulerContext* psc)
{
	return m_jsm.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::ScheduleJob
//
//	@doc:
//		Schedule a new group expression exploration job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionExploration::ScheduleJob(CSchedulerContext* psc, CGroupExpression* pgexpr, CJob* pjParent)
{
	CJob* pj = psc->m_pjf->PjCreate(CJob::EjtGroupExpressionExploration);
	// initialize job
	CJobGroupExpressionExploration* pjege = PjConvert(pj);
	pjege->Init(pgexpr);
	psc->m_psched->Add(pjege, pjParent);
}
}