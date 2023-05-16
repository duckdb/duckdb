//---------------------------------------------------------------------------
//	@filename:
//		COstreamString.cpp
//
//	@doc:
//		Implementation of basic wide character output stream
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/string/CWStringConst.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::COstreamString
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COstreamString::COstreamString(CWString *pws)
    : COstream(), m_string(pws)
{
	GPOS_ASSERT(m_string && "Backing string cannot be NULL");
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		WCHAR array write thru;
//
//---------------------------------------------------------------------------
IOstream & COstreamString::operator<<(const WCHAR *wc_array)
{
	m_string->AppendWideCharArray(wc_array);
	return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		CHAR array write thru;
//
//---------------------------------------------------------------------------
IOstream & COstreamString::operator<<(const CHAR *c)
{
	m_string->AppendCharArray(c);
	return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		WCHAR write thru;
//
//---------------------------------------------------------------------------
IOstream & COstreamString::operator<<(const WCHAR wc)
{
	WCHAR wc_array[2];
	wc_array[0] = wc;
	wc_array[1] = L'\0';
	m_string->AppendWideCharArray(wc_array);
	return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		CHAR write thru;
//
//---------------------------------------------------------------------------
IOstream & COstreamString::operator<<(const CHAR c)
{
	CHAR char_array[2];
	char_array[0] = c;
	char_array[1] = '\0';
	m_string->AppendCharArray(char_array);
	return *this;
}