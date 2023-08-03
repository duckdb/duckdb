//---------------------------------------------------------------------------
//	@filename:
//		CSchedulerContext.cpp
//
//	@doc:
//		Implementation of optimizer job scheduler
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"

using namespace gpos;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CSchedulerContext::CSchedulerContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSchedulerContext::CSchedulerContext()
	:m_psched(NULL), m_fInit(false)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerContext::~CSchedulerContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSchedulerContext::~CSchedulerContext()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CSchedulerContext::Init
//
//	@doc:
//		Initialize scheduling context
//
//---------------------------------------------------------------------------
void CSchedulerContext::Init(CJobFactory* pjf, CScheduler* psched, CEngine* peng)
{
	m_pjf = pjf;
	m_psched = psched;
	m_peng = peng;
	m_fInit = true;
}