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
void
CJob::Reset()
{
	m_pjParent = NULL;
	m_pjq = NULL;
	m_ulpRefs = 0;
	m_fInit = false;
#ifdef GPOS_DEBUG
	m_ejs = EjsInit;
#endif	// GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJob::FResumeParent
//
//	@doc:
//		Resume parent jobs after job completion
//
//---------------------------------------------------------------------------
BOOL
CJob::FResumeParent() const
{
	GPOS_ASSERT(0 == UlpRefs());
	GPOS_ASSERT(NULL != m_pjParent);
	GPOS_ASSERT(0 < m_pjParent->UlpRefs());

	// decrement parent's ref counter
	ULONG_PTR ulpRefs = m_pjParent->UlpDecrRefs();

	// check if job should be resumed
	return (1 == ulpRefs);
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CJob::OsPrint
//
//	@doc:
//		Print job description
//
//---------------------------------------------------------------------------
IOstream &
CJob::OsPrint(IOstream &os)
{
	os << "ID=" << Id();

	if (NULL != PjParent())
	{
		os << " parent=" << PjParent()->Id() << std::endl;
	}
	else
	{
		os << " ROOT" << std::endl;
	}
	return os;
}

#endif