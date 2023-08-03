//---------------------------------------------------------------------------
//	@filename:
//		CWString.cpp
//
//	@doc:
//		Implementation of the wide character string class.
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/string/CWString.h"
#include "duckdb/optimizer/cascade/base.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CWString::CWString
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CWString::CWString(ULONG length)
	: CWStringBase(length, true), m_w_str_buffer(NULL)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CWString::GetBuffer
//
//	@doc:
//		Returns the wide character buffer storing the string
//
//---------------------------------------------------------------------------
const WCHAR* CWString::GetBuffer() const
{
	return m_w_str_buffer;
}


//---------------------------------------------------------------------------
//	@function:
//		CWString::Append
//
//	@doc:
//		Appends a string to the current string
//
//---------------------------------------------------------------------------
void CWString::Append(const CWStringBase *str)
{
	if (0 < str->Length())
	{
		AppendBuffer(str->GetBuffer());
	}
}