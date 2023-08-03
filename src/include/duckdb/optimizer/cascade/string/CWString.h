//---------------------------------------------------------------------------
//	@filename:
//		CWString.h
//
//	@doc:
//		Wide character string interface.
//---------------------------------------------------------------------------
#ifndef GPOS_CWString_H
#define GPOS_CWString_H

#include "duckdb/optimizer/cascade/string/CWStringBase.h"

#define GPOS_MAX_FMT_STR_LENGTH (10 * 1024 * 1024)	// 10MB

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CWString
//
//	@doc:
//		Wide character String interface.
//		Internally, the class uses a null-terminated WCHAR buffer to store the string characters.
//		The class API provides functions for accessing the wide-character buffer and its length,
//		as well as functions that modify the current string by appending another string to it,
//		or that construct a string according to a given format.
//		For constant strings consider using the CWStringConst class.
//
//---------------------------------------------------------------------------
class CWString : public CWStringBase
{
public:
	// null-terminated wide character buffer
	WCHAR* m_w_str_buffer;

	// appends the contents of a buffer to the current string
	virtual void AppendBuffer(const WCHAR* w_str_buffer) = 0;

public:
	// ctor
	CWString(ULONG length);

	// dtor
	virtual ~CWString()
	{
	}

	// returns the wide character buffer storing the string
	const WCHAR* GetBuffer() const;

	// appends a string
	void Append(const CWStringBase* str);

	// appends a formatted string
	virtual void AppendFormat(const WCHAR* format, ...) = 0;

	// appends a string and replaces character with string
	virtual void AppendEscape(const CWStringBase* str, WCHAR wc, const WCHAR* w_str_replace) = 0;

	// appends a null terminated character array
	virtual void AppendCharArray(const CHAR* sz) = 0;

	// appends a null terminated wide character array
	virtual void AppendWideCharArray(const WCHAR* w_str) = 0;

	// resets string
	virtual void Reset() = 0;
};
}  // namespace gpos
#endif	// !GPOS_CWString_H