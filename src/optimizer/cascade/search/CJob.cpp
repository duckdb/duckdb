//---------------------------------------------------------------------------
//	@filename:
//		CJob.cpp
//
//	@doc:
//		Implementation of optimizer job base class
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJob.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJobQueue.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CJob::Reset
//
//	@doc:
//		Reset job
//
//---------------------------------------------------------------------------
void CJob::Reset()
{
	m_pjParent = NULL;
	m_pjq = NULL;
	m_ulpRefs = 0;
	m_fInit = false;
}

//---------------------------------------------------------------------------
//	@function:
//		CJob::FResumeParent
//
//	@doc:
//		Resume parent jobs after job completion
//
//---------------------------------------------------------------------------
BOOL CJob::FResumeParent() const
{
	// decrement parent's ref counter
	ULONG_PTR ulpRefs = m_pjParent->UlpDecrRefs();
	// check if job should be resumed
	return (1 == ulpRefs);
}