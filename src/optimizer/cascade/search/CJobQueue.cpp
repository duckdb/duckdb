//---------------------------------------------------------------------------
//	@filename:
//		CJobQueue.cpp
//
//	@doc:
//		Implementation of class controlling unique execution of an operation
//		that is potentially assigned to many jobs.

//	@owner:
//
//
//	@test:
//
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobQueue.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"

using namespace gpos;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CJobQueue::EjqrAdd
//
//	@doc:
//		Add job as a waiter;
//
//---------------------------------------------------------------------------
CJobQueue::EJobQueueResult CJobQueue::EjqrAdd(CJob* pj)
{
	EJobQueueResult ejer = EjqrCompleted;
	// check if job has completed before getting the lock
	if (!m_fCompleted)
	{
		// check if this is the main job
		if (pj == m_pj)
		{
			ejer = EjqrMain;
			pj->IncRefs();
		}
		else
		{
			// check if job is completed
			if (!m_fCompleted)
			{
				m_listjQueued.Append(pj);
				bool fOwner = (pj == m_listjQueued.First());
				// first caller becomes the owner
				if (fOwner)
				{
					m_pj = pj;
					ejer = EjqrMain;
				}
				else
				{
					ejer = EjqrQueued;
				}
				pj->IncRefs();
			}
		}
	}
	return ejer;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobQueue::NotifyCompleted
//
//	@doc:
//		Notify waiting jobs of job completion
//
//---------------------------------------------------------------------------
void CJobQueue::NotifyCompleted(CSchedulerContext* psc)
{
	m_fCompleted = true;
	while (!m_listjQueued.IsEmpty())
	{
		CJob*  pj = m_listjQueued.RemoveHead();
		// check if job execution has completed
		if (1 == pj->UlpDecrRefs())
		{
			// update job as completed
			psc->m_psched->CompleteQueued(pj);
			// recycle job
			psc->m_pjf->Release(pj);
		}
	}
}