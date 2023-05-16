//---------------------------------------------------------------------------
//	@filename:
//		CMiniDumper.cpp
//
//	@doc:
//		Partial implementation of interface for minidump handler
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/error/CMiniDumper.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/error/CErrorContext.h"
#include "duckdb/optimizer/cascade/string/CWStringConst.h"
#include "duckdb/optimizer/cascade/task/CTask.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::CMiniDumper
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMiniDumper::CMiniDumper(CMemoryPool *mp)
	: m_mp(mp), m_initialized(false), m_finalized(false), m_oos(NULL)
{
	GPOS_ASSERT(NULL != mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::~CMiniDumper
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMiniDumper::~CMiniDumper()
{
	if (m_initialized)
	{
		CTask *task = CTask::Self();
		GPOS_ASSERT(NULL != task);
		task->ConvertErrCtxt()->Unregister(
#ifdef GPOS_DEBUG
			this
#endif	// GPOS_DEBUG
		);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::Init
//
//	@doc:
//		Initialize
//
//---------------------------------------------------------------------------
void CMiniDumper::Init(COstream *oos)
{
	GPOS_ASSERT(!m_initialized);
	GPOS_ASSERT(!m_finalized);
	CTask *task = CTask::Self();
	GPOS_ASSERT(NULL != task);
	m_oos = oos;
	task->ConvertErrCtxt()->Register(this);
	m_initialized = true;
	SerializeHeader();
}

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::Finalize
//
//	@doc:
//		Finalize
//
//---------------------------------------------------------------------------
void CMiniDumper::Finalize()
{
	GPOS_ASSERT(m_initialized);
	GPOS_ASSERT(!m_finalized);

	SerializeFooter();

	m_finalized = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::GetOStream
//
//	@doc:
//		Get stream to serialize to
//
//---------------------------------------------------------------------------
COstream& CMiniDumper::GetOStream()
{
	GPOS_ASSERT(m_initialized);

	return *m_oos;
}