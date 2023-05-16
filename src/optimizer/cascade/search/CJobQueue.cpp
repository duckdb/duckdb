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
CJobQueue::EJobQueueResult
CJobQueue::EjqrAdd(CJob *pj)
{
	GPOS_ASSERT(NULL != pj);

	EJobQueueResult ejer = EjqrCompleted;

	// check if job has completed before getting the lock
	if (!m_fCompleted)
	{
		// check if this is the main job
		if (pj == m_pj)
		{
			GPOS_ASSERT(!m_fCompleted);
			ejer = EjqrMain;
			pj->IncRefs();
		}
		else
		{
			// check if job is completed
			if (!m_fCompleted)
			{
				m_listjQueued.Append(pj);
				BOOL fOwner = (pj == m_listjQueued.First());

				// first caller becomes the owner
				if (fOwner)
				{
					GPOS_ASSERT(NULL == m_pj);

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
void
CJobQueue::NotifyCompleted(CSchedulerContext *psc)
{
	GPOS_ASSERT(!m_fCompleted);
	m_fCompleted = true;

	GPOS_ASSERT(!m_listjQueued.IsEmpty());
	while (!m_listjQueued.IsEmpty())
	{
		CJob *pj = m_listjQueued.RemoveHead();

		// check if job execution has completed
		if (1 == pj->UlpDecrRefs())
		{
			// update job as completed
			psc->Psched()->CompleteQueued(pj);

			// recycle job
			psc->Pjf()->Release(pj);
		}
	}
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CJobQueue::OsPrintQueuedJobs
//
//	@doc:
//		Print queue - not thread-safe
//
//---------------------------------------------------------------------------
IOstream &
CJobQueue::OsPrintQueuedJobs(IOstream &os)
{
	os << "Job queue: " << std::endl;

	CJob *pj = m_listjQueued.First();
	while (NULL != pj)
	{
		pj->OsPrint(os);
		pj = m_listjQueued.Next(pj);
	}

	return os;
}

#endif