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
const CJobGroupImplementation::EEvent rgeev4[CJobGroupImplementation::estSentinel][CJobGroupImplementation::estSentinel] =
{
	{CJobGroupImplementation::eevExploring, CJobGroupImplementation::eevExplored, CJobGroupImplementation::eevSentinel},
	{CJobGroupImplementation::eevSentinel, CJobGroupImplementation::eevImplementing, CJobGroupImplementation::eevImplemented},
	{CJobGroupImplementation::eevSentinel, CJobGroupImplementation::eevSentinel, CJobGroupImplementation::eevSentinel},
};

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
void CJobGroupImplementation::Init(CGroup* pgroup)
{
	CJobGroup::Init(pgroup);
	m_jsm.Init(rgeev4);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtStartImplementation);
	m_jsm.SetAction(estImplementingChildren, EevtImplementChildren);
	SetJobQueue(&pgroup->m_jqImplementation);
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
bool CJobGroupImplementation::FScheduleGroupExpressions(CSchedulerContext* psc)
{
	auto pgexprLast = m_pgexprLastScheduled;
	// iterate on expressions and schedule them as needed
	auto itr = PgexprFirstUnsched();
	while (m_pgroup->m_listGExprs.end() != itr)
	{
		CGroupExpression* pgexpr = *itr;
		if (!pgexpr->FTransitioned(CGroupExpression::estImplemented) && !pgexpr->ContainsCircularDependencies())
		{
			CJobGroupExpressionImplementation::ScheduleJob(psc, pgexpr, this);
			pgexprLast = itr;
		}
		// move to next expression
		{
			CGroupProxy gp(m_pgroup);
			++itr;
		}
	}
	bool fNewJobs = (m_pgexprLastScheduled != pgexprLast);
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
CJobGroupImplementation::EEvent CJobGroupImplementation::EevtStartImplementation(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupImplementation* pjgi = PjConvert(pjOwner);
	CGroup* pgroup = pjgi->m_pgroup;
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
		if (psc->m_peng->FRoot(pgroup))
		{
			psc->m_pjf->Truncate(EjtGroupExploration);
			psc->m_pjf->Truncate(EjtGroupExpressionExploration);
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
CJobGroupImplementation::EEvent CJobGroupImplementation::EevtImplementChildren(CSchedulerContext* psc, CJob* pjOwner)
{
	// get a job pointer
	CJobGroupImplementation* pjgi = PjConvert(pjOwner);
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
		if (psc->m_peng->FRoot(pjgi->m_pgroup))
		{
			psc->m_peng->FinalizeImplementation();
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
bool CJobGroupImplementation::FExecute(CSchedulerContext* psc)
{
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
void CJobGroupImplementation::ScheduleJob(CSchedulerContext* psc, CGroup* pgroup, CJob* pjParent)
{
	CJob* pj = psc->m_pjf->PjCreate(CJob::EjtGroupImplementation);
	// initialize job
	CJobGroupImplementation* pjgi = PjConvert(pj);
	pjgi->Init(pgroup);
	psc->m_psched->Add(pjgi, pjParent);
}