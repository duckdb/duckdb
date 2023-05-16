//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionExploration.cpp
//
//	@doc:
//		Implementation of group expression exploration job
//---------------------------------------------------------------------------

#include "duckdb/optimizer/cascade/search/CJobGroupExpressionExploration.h"

#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExploration.h"
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"


using namespace gpopt;

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
const CJobGroupExpressionExploration::EEvent
	rgeev[CJobGroupExpressionExploration::estSentinel]
		 [CJobGroupExpressionExploration::estSentinel] = {
			 {// estInitialized
			  CJobGroupExpressionExploration::eevExploringChildren,
			  CJobGroupExpressionExploration::eevChildrenExplored,
			  CJobGroupExpressionExploration::eevSentinel,
			  CJobGroupExpressionExploration::eevSentinel},
			 {// estChildrenExplored
			  CJobGroupExpressionExploration::eevSentinel,
			  CJobGroupExpressionExploration::eevExploringSelf,
			  CJobGroupExpressionExploration::eevSelfExplored,
			  CJobGroupExpressionExploration::eevSentinel},
			 {// estSelfExplored
			  CJobGroupExpressionExploration::eevSentinel,
			  CJobGroupExpressionExploration::eevSentinel,
			  CJobGroupExpressionExploration::eevSentinel,
			  CJobGroupExpressionExploration::eevFinalized},
			 {// estCompleted
			  CJobGroupExpressionExploration::eevSentinel,
			  CJobGroupExpressionExploration::eevSentinel,
			  CJobGroupExpressionExploration::eevSentinel,
			  CJobGroupExpressionExploration::eevSentinel},
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
void
CJobGroupExpressionExploration::Init(CGroupExpression *pgexpr)
{
	CJobGroupExpression::Init(pgexpr);
	GPOS_ASSERT(pgexpr->Pop()->FLogical());


	m_jsm.Init(rgeev
#ifdef GPOS_DEBUG
			   ,
			   rgwszStates, rgwszEvents
#endif	// GPOS_DEBUG
	);

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
void
CJobGroupExpressionExploration::ScheduleApplicableTransformations(
	CSchedulerContext *psc)
{
	GPOS_ASSERT(!FXformsScheduled());

	// get all applicable xforms
	COperator *pop = m_pgexpr->Pop();
	CXformSet *xform_set =
		CLogical::PopConvert(pop)->PxfsCandidates(psc->GetGlobalMemoryPool());

	// intersect them with required xforms and schedule jobs
	xform_set->Intersection(CXformFactory::Pxff()->PxfsExploration());
	xform_set->Intersection(psc->Peng()->PxfsCurrentStage());
	ScheduleTransformations(psc, xform_set);
	xform_set->Release();

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
void
CJobGroupExpressionExploration::ScheduleChildGroupsJobs(CSchedulerContext *psc)
{
	GPOS_ASSERT(!FChildrenScheduled());

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
CJobGroupExpressionExploration::EEvent
CJobGroupExpressionExploration::EevtExploreChildren(CSchedulerContext *psc,
													CJob *pjOwner)
{
	// get a job pointer
	CJobGroupExpressionExploration *pjgee = PjConvert(pjOwner);
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
CJobGroupExpressionExploration::EEvent
CJobGroupExpressionExploration::EevtExploreSelf(CSchedulerContext *psc,
												CJob *pjOwner)
{
	// get a job pointer
	CJobGroupExpressionExploration *pjgee = PjConvert(pjOwner);
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
CJobGroupExpressionExploration::EEvent
CJobGroupExpressionExploration::EevtFinalize(CSchedulerContext *,  //psc
											 CJob *pjOwner)
{
	// get a job pointer
	CJobGroupExpressionExploration *pjgee = PjConvert(pjOwner);
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
BOOL
CJobGroupExpressionExploration::FExecute(CSchedulerContext *psc)
{
	GPOS_ASSERT(FInit());

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
void
CJobGroupExpressionExploration::ScheduleJob(CSchedulerContext *psc,
											CGroupExpression *pgexpr,
											CJob *pjParent)
{
	CJob *pj = psc->Pjf()->PjCreate(CJob::EjtGroupExpressionExploration);

	// initialize job
	CJobGroupExpressionExploration *pjege = PjConvert(pj);
	pjege->Init(pgexpr);
	psc->Psched()->Add(pjege, pjParent);
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CJobGroupExpressionExploration::OsPrint(IOstream &os)
{
	return m_jsm.OsHistory(os);
}

#endif