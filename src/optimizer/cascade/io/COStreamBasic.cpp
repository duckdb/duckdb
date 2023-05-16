//---------------------------------------------------------------------------
//	@filename:
//		COstreamBasic.cpp
//
//	@doc:
//		Implementation of basic wide character output stream
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/io/COstreamBasic.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/io/ioutils.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		COstreamBasic::COstreamBasic
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COstreamBasic::COstreamBasic(WOSTREAM *pos) : COstream(), m_ostream(pos)
{
	GPOS_ASSERT(NULL != m_ostream && "Output stream cannot be NULL");
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamBasic::operator<<
//
//	@doc:
//		WCHAR write thru;
//
//---------------------------------------------------------------------------
IOstream & COstreamBasic::operator<<(const WCHAR *wsz)
{
	m_ostream = &(*m_ostream << wsz);
	return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamBasic::operator<<
//
//	@doc:
//		WCHAR write thru;
//
//---------------------------------------------------------------------------
IOstream & COstreamBasic::operator<<(const WCHAR wc)
{
	m_ostream = &(*m_ostream << wc);
	return *this;
}