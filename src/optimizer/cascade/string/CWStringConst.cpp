//---------------------------------------------------------------------------
//	@filename:
//		CWStringConst.cpp
//
//	@doc:
//		Implementation of the wide character constant string class
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/string/CWStringConst.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::CWStringConst
//
//	@doc:
//		Initializes a constant string with a given character buffer. The string
//		does not own the memory
//
//---------------------------------------------------------------------------
CWStringConst::CWStringConst(const WCHAR *w_str_buffer)
	: CWStringBase(GPOS_WSZ_LENGTH(w_str_buffer), false), m_w_str_buffer(w_str_buffer)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::CWStringConst
//
//	@doc:
//		Shallow copy constructor.
//
//---------------------------------------------------------------------------
CWStringConst::CWStringConst(const CWStringConst &str)
	: CWStringBase(str.Length(), false), m_w_str_buffer(str.GetBuffer())
{
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::~CWStringConst
//
//	@doc:
//		Destroys a constant string. This involves releasing the character buffer
//		provided the string owns it.
//
//---------------------------------------------------------------------------
CWStringConst::~CWStringConst()
{
	delete[] m_w_str_buffer;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::GetBuffer
//
//	@doc:
//		Returns the wide character buffer
//
//---------------------------------------------------------------------------
const WCHAR* CWStringConst::GetBuffer() const
{
	return m_w_str_buffer;
}