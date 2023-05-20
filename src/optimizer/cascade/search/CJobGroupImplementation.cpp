//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupImplementation.cpp
//
//	@doc:
//		Implementation of group implementation job
//---------------------------------------------------------------------------

#include "duckdb/optimizer/cascade/search/CJobGroupImplementation.h"

#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExploration.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobQueue.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/traceflags/traceflags.h"

using namespace gpopt;


// State transition diagram for group implementation job state machine;
//
// +---------------------------+   eevExploring
// |      estInitialized:      | ------------------+
// | EevtStartImplementation() |                   |
// |                           | <-----------------+
// +---------------------------+
//   |
//   | eevExplored
//   v
// +---------------------------+   eevImplementing
// | estImplementingChildren:  | ------------------+
// |  EevtImplementChildren()  |                   |
// |                           | <-----------------+
// +---------------------------+
//   |
//   | eevImplemented
//   v
// +---------------------------+
// |       estCompleted        |
// +---------------------------+
//
const CJobGroupImplementation::EEvent
	rgeev4[CJobGroupImplementation::estSentinel]
		 [CJobGroupImplementation::estSentinel] = {
			 {// estInitialized
			  CJobGroupImplementation::eevExploring,
			  CJobGroupImplementation::eevExplored,
			  CJobGroupImplementation::eevSentinel},
			 {// estImplementingChildren
			  CJobGroupImplementation::eevSentinel,
			  CJobGroupImplementation::eevImplementing,
			  CJobGroupImplementation::eevImplemented},
			 {// estCompleted
			  CJobGroupImplementation::eevSentinel,
			  CJobGroupImplementation::eevSentinel,
			  CJobGroupImplementation::eevSentinel},
};

#ifdef GPOS_DEBUG

// names for states
const WCHAR
	rgwszStates[CJobGroupImplementation::estSentinel][GPOPT_FSM_NAME_LENGTH] = {
		GPOS_WSZ_LIT("initialized"), GPOS_WSZ_LIT("implementing children"),
		GPOS_WSZ_LIT("completed")};

// names for events
const WCHAR
	rgwszEvents[CJobGroupImplementation::eevSentinel][GPOPT_FSM_NAME_LENGTH] = {
		GPOS_WSZ_LIT("ongoing exploration"), GPOS_WSZ_LIT("done exploration"),
		GPOS_WSZ_LIT("ongoing implementation"), GPOS_WSZ_LIT("finalizing")};

#endif	// GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::CJobGroupImplementation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupImplementation::CJobGroupImplementation()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::~CJobGroupImplementation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupImplementation::~CJobGroupImplementation()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroupImplementation::Init(CGroup *pgroup)
{
	CJobGroup::Init(pgroup);
	m_jsm.Init(rgeev4
#ifdef GPOS_DEBUG
			   ,
			   rgwszStates, rgwszEvents
#endif	// GPOS_DEBUG
	);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtStartImplementation);
	m_jsm.SetAction(estImplementingChildren, EevtImplementChildren);
	SetJobQueue(pgroup->PjqImplementation());
	CJob::SetInit();
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::FScheduleGroupExpressions
//
//	@doc:
//		Schedule implementation jobs for all unimplemented group expressions;
//		the function returns true if it could schedule any new jobs
//
//---------------------------------------------------------------------------
BOOL CJobGroupImplementation::FScheduleGroupExpressions(CSchedulerContext *psc)
{
	CGroupExpression *pgexprLast = m_pgexprLastScheduled;

	// iterate on expression and schedule them as needed
	CGroupExpression *pgexpr = PgexprFirstUnsched();
	while (NULL != pgexpr)
	{
		if (!pgexpr->FTransitioned(CGroupExpression::estImplemented) &&
			!pgexpr->ContainsCircularDependencies())
		{
			CJobGroupExpressionImplementation::ScheduleJob(psc, pgexpr, this);
			pgexprLast = pgexpr;
		}

		// move to next expression
		{
			CGroupProxy gp(m_pgroup);
			pgexpr = gp.PgexprNext(pgexpr);
		}
	}

	BOOL fNewJobs = (m_pgexprLastScheduled != pgexprLast);

	// set last scheduled expression
	m_pgexprLastScheduled = pgexprLast;

	return fNewJobs;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::EevtStartImplementation
//
//	@doc:
//		Start group implementation
//
//---------------------------------------------------------------------------
CJobGroupImplementation::EEvent
CJobGroupImplementation::EevtStartImplementation(CSchedulerContext *psc,
												 CJob *pjOwner)
{
	// get a job pointer
	CJobGroupImplementation *pjgi = PjConvert(pjOwner);
	CGroup *pgroup = pjgi->m_pgroup;

	if (!pgroup->FExplored())
	{
		// schedule a child exploration job
		CJobGroupExploration::ScheduleJob(psc, pgroup, pjgi);
		return eevExploring;
	}
	else
	{
		// move group to implementation state
		{
			CGroupProxy gp(pgroup);
			gp.SetState(CGroup::estImplementing);
		}

		// if this is the root, release exploration jobs
		if (psc->Peng()->FRoot(pgroup))
		{
			psc->Pjf()->Truncate(EjtGroupExploration);
			psc->Pjf()->Truncate(EjtGroupExpressionExploration);
		}

		return eevExplored;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::EevtImplementChildren
//
//	@doc:
//		Implement child group expressions
//
//---------------------------------------------------------------------------
CJobGroupImplementation::EEvent
CJobGroupImplementation::EevtImplementChildren(CSchedulerContext *psc,
											   CJob *pjOwner)
{
	// get a job pointer
	CJobGroupImplementation *pjgi = PjConvert(pjOwner);
	if (pjgi->FScheduleGroupExpressions(psc))
	{
		// implementation is in progress
		return eevImplementing;
	}
	else
	{
		// move group to implemented state
		{
			CGroupProxy gp(pjgi->m_pgroup);
			gp.SetState(CGroup::estImplemented);
		}

		// if this is the root, complete implementation phase
		if (psc->Peng()->FRoot(pjgi->m_pgroup))
		{
			psc->Peng()->FinalizeImplementation();
		}

		return eevImplemented;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
BOOL
CJobGroupImplementation::FExecute(CSchedulerContext *psc)
{
	GPOS_ASSERT(FInit());

	return m_jsm.FRun(psc, this);
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::ScheduleJob
//
//	@doc:
//		Schedule a new group implementation job
//
//---------------------------------------------------------------------------
void
CJobGroupImplementation::ScheduleJob(CSchedulerContext *psc, CGroup *pgroup,
									 CJob *pjParent)
{
	CJob *pj = psc->Pjf()->PjCreate(CJob::EjtGroupImplementation);

	// initialize job
	CJobGroupImplementation *pjgi = PjConvert(pj);
	pjgi->Init(pgroup);
	psc->Psched()->Add(pjgi, pjParent);
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CJobGroupImplementation::OsPrint(IOstream &os)
{
	return m_jsm.OsHistory(os);
}

#endif