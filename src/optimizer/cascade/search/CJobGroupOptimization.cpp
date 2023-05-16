//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupOptimization.cpp
//
//	@doc:
//		Implementation of group optimization job
//---------------------------------------------------------------------------

#include "duckdb/optimizer/cascade/search/CJobGroupOptimization.h"

#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionOptimization.h"
#include "duckdb/optimizer/cascade/search/CJobGroupImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobQueue.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/traceflags/traceflags.h"

using namespace gpopt;


// State transition diagram for group optimization job state machine:
//
//     eevImplementing   +------------------------------+
//  +------------------ |       estInitialized:        |
//  |                   |   EevtStartOptimization()    |
//  +-----------------> |                              | -+
//                      +------------------------------+  |
//                        |                               |
//                        | eevImplemented                |
//                        v                               |
//                      +------------------------------+  |
//      eevOptimizing   |                              |  |
//  +------------------ |                              |  |
//  |                   |    estOptimizingChildren:    |  |
//  +-----------------> |    EevtOptimizeChildren()    |  |
//                      |                              |  |
//  +-----------------> |                              |  |
//  |                   +------------------------------+  |
//  |                     |                               |
//  | eevOptimizing       | eevOptimizedCurrentLevel      |
//  |                     v                               |
//  |                   +------------------------------+  |
//  |                   | estDampingOptimizationLevel: |  |
//  +------------------ |  EevtCompleteOptimization()  |  |
//                      +------------------------------+  |
//                        |                               |
//                        | eevOptimized                  | eevOptimized
//                        v                               |
//                      +------------------------------+  |
//                      |         estCompleted         | <+
//                      +------------------------------+
//
const CJobGroupOptimization::EEvent
	rgeev[CJobGroupOptimization::estSentinel]
		 [CJobGroupOptimization::estSentinel] = {
			 {// estInitialized
			  CJobGroupOptimization::eevImplementing,
			  CJobGroupOptimization::eevImplemented,
			  CJobGroupOptimization::eevSentinel,
			  CJobGroupOptimization::eevOptimized},
			 {// estOptimizingChildren
			  CJobGroupOptimization::eevSentinel,
			  CJobGroupOptimization::eevOptimizing,
			  CJobGroupOptimization::eevOptimizedCurrentLevel,
			  CJobGroupOptimization::eevSentinel},
			 {// estDampingOptimizationLevel
			  CJobGroupOptimization::eevSentinel,
			  CJobGroupOptimization::eevOptimizing,
			  CJobGroupOptimization::eevSentinel,
			  CJobGroupOptimization::eevOptimized},
			 {// estCompleted
			  CJobGroupOptimization::eevSentinel,
			  CJobGroupOptimization::eevSentinel,
			  CJobGroupOptimization::eevSentinel,
			  CJobGroupOptimization::eevSentinel},
};

#ifdef GPOS_DEBUG

// names for states
const WCHAR
	rgwszStates[CJobGroupOptimization::estSentinel][GPOPT_FSM_NAME_LENGTH] = {
		GPOS_WSZ_LIT("initialized"), GPOS_WSZ_LIT("optimizing children"),
		GPOS_WSZ_LIT("damping optimization level"), GPOS_WSZ_LIT("completed")};

// names for events
const WCHAR rgwszEvents[CJobGroupOptimization::eevSentinel]
					   [GPOPT_FSM_NAME_LENGTH] = {
						   GPOS_WSZ_LIT("ongoing implementation"),
						   GPOS_WSZ_LIT("done implementation"),
						   GPOS_WSZ_LIT("ongoing optimization"),
						   GPOS_WSZ_LIT("optimization level complete"),
						   GPOS_WSZ_LIT("finalizing")};

#endif	// GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::CJobGroupOptimization
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupOptimization::CJobGroupOptimization()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::~CJobGroupOptimization
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupOptimization::~CJobGroupOptimization()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void
CJobGroupOptimization::Init(
	CGroup *pgroup,
	CGroupExpression
		*pgexprOrigin,	// group expression that triggered optimization job,
						// NULL if this is the Root group
	COptimizationContext *poc)
{
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(pgroup == poc->Pgroup());

	CJobGroup::Init(pgroup);
	m_jsm.Init(rgeev
#ifdef GPOS_DEBUG
			   ,
			   rgwszStates, rgwszEvents
#endif	// GPOS_DEBUG
	);

	// set job actions
	m_jsm.SetAction(estInitialized, EevtStartOptimization);
	m_jsm.SetAction(estOptimizingChildren, EevtOptimizeChildren);
	m_jsm.SetAction(estDampingOptimizationLevel, EevtCompleteOptimization);

	m_pgexprOrigin = pgexprOrigin;
	m_poc = m_pgroup->PocInsert(poc);
	if (poc == m_poc)
	{
		// pin down context in hash table
		m_poc->AddRef();
	}
	SetJobQueue(m_poc->PjqOptimization());

	// initialize current optimization level as low
	m_eolCurrent = EolLow;

	CJob::SetInit();
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::FScheduleGroupExpressions
//
//	@doc:
//		Schedule optimization jobs for all unoptimized group expressions with
//		the current optimization priority;
//		the function returns true if it could schedule any new jobs
//
//---------------------------------------------------------------------------
BOOL
CJobGroupOptimization::FScheduleGroupExpressions(CSchedulerContext *psc)
{
	CGroupExpression *pgexprLast = m_pgexprLastScheduled;

	// iterate on expressions and schedule them as needed
	CGroupExpression *pgexpr = PgexprFirstUnsched();
	while (NULL != pgexpr)
	{
		// we consider only group expressions matching current optimization level,
		// other group expressions will be optimized when damping current
		// optimization level
		if (psc->Peng()->FOptimizeChild(m_pgexprOrigin, pgexpr, m_poc,
										EolCurrent()))
		{
			const ULONG ulOptRequests =
				CPhysical::PopConvert(pgexpr->Pop())->UlOptRequests();
			for (ULONG ul = 0; ul < ulOptRequests; ul++)
			{
				// schedule an optimization job for each request
				CJobGroupExpressionOptimization::ScheduleJob(psc, pgexpr, m_poc,
															 ul, this);
			}
		}
		pgexprLast = pgexpr;

		// move to next expression
		{
			CGroupProxy gp(m_pgroup);
			pgexpr = gp.PgexprSkipLogical(pgexpr);
		}
	}

	BOOL fNewJobs = (m_pgexprLastScheduled != pgexprLast);

	// set last scheduled expression
	m_pgexprLastScheduled = pgexprLast;

	return fNewJobs;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::EevtStartOptimization
//
//	@doc:
//		Start group optimization
//
//---------------------------------------------------------------------------
CJobGroupOptimization::EEvent
CJobGroupOptimization::EevtStartOptimization(CSchedulerContext *psc,
											 CJob *pjOwner)
{
	// get a job pointer
	CJobGroupOptimization *pjgo = PjConvert(pjOwner);
	CGroup *pgroup = pjgo->m_pgroup;
	GPOS_ASSERT(COptimizationContext::estUnoptimized == pjgo->m_poc->Est() &&
				"Group is already optimized under this context");

	if (!pgroup->FImplemented())
	{
		// schedule a group implementation child job
		CJobGroupImplementation::ScheduleJob(psc, pgroup, pjgo);
		return eevImplementing;
	}

	// move optimization context to optimizing state
	pjgo->m_poc->SetState(COptimizationContext::estOptimizing);

	// if this is the root, release implementation jobs
	if (psc->Peng()->FRoot(pgroup))
	{
		psc->Pjf()->Truncate(EjtGroupImplementation);
		psc->Pjf()->Truncate(EjtGroupExpressionImplementation);
	}

	// at this point all group expressions have been added to group,
	// we set current job optimization level as the max group optimization level
	pjgo->m_eolCurrent = pgroup->EolMax();

	return eevImplemented;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::EevtOptimizeChildren
//
//	@doc:
//		Optimize child group expressions
//
//---------------------------------------------------------------------------
CJobGroupOptimization::EEvent
CJobGroupOptimization::EevtOptimizeChildren(CSchedulerContext *psc,
											CJob *pjOwner)
{
	// get a job pointer
	CJobGroupOptimization *pjgo = PjConvert(pjOwner);

	if (pjgo->FScheduleGroupExpressions(psc))
	{
		// optimization is in progress
		return eevOptimizing;
	}

	// optimization of current level is complete
	return eevOptimizedCurrentLevel;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::EevtCompleteOptimization
//
//	@doc:
//		Complete optimization action
//
//---------------------------------------------------------------------------
CJobGroupOptimization::EEvent
CJobGroupOptimization::EevtCompleteOptimization(CSchedulerContext *,  // psc
												CJob *pjOwner)
{
	// get a job pointer
	CJobGroupOptimization *pjgo = PjConvert(pjOwner);

	// move to next optimization level
	pjgo->DampOptimizationLevel();
	if (EolSentinel != pjgo->EolCurrent())
	{
		// we need to optimize group expressions matching current level
		pjgo->m_pgexprLastScheduled = NULL;

		return eevOptimizing;
	}

	// move optimization context to optimized state
	pjgo->m_poc->SetState(COptimizationContext::estOptimized);

	return eevOptimized;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
BOOL
CJobGroupOptimization::FExecute(CSchedulerContext *psc)
{
	GPOS_ASSERT(FInit());

	return m_jsm.FRun(psc, this);
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::ScheduleJob
//
//	@doc:
//		Schedule a new group optimization job
//
//---------------------------------------------------------------------------
void
CJobGroupOptimization::ScheduleJob(CSchedulerContext *psc, CGroup *pgroup,
								   CGroupExpression *pgexprOrigin,
								   COptimizationContext *poc, CJob *pjParent)
{
	CJob *pj = psc->Pjf()->PjCreate(CJob::EjtGroupOptimization);

	// initialize job
	CJobGroupOptimization *pjgo = PjConvert(pj);
	pjgo->Init(pgroup, pgexprOrigin, poc);
	psc->Psched()->Add(pjgo, pjParent);
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CJobGroupOptimization::OsPrint(IOstream &os)
{
	return m_jsm.OsHistory(os);
}

#endif