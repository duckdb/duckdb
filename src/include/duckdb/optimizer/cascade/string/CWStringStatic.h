//---------------------------------------------------------------------------
//	@filename:
//		CWStringStatic.h
//
//	@doc:
//		Wide character String class with buffer.
//---------------------------------------------------------------------------
#ifndef GPOS_CWStringStatic_H
#define GPOS_CWStringStatic_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/string/CWString.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CWStringStatic
//
//	@doc:
//		Implementation of the string interface with buffer pre-allocation.
//		Internally, the class uses a null-terminated WCHAR buffer to store the string
//		characters.	The buffer is assigned at construction time; its capacity cannot be
//		modified, thus restricting the maximum size of the stored string. Attempting to
//		store a larger string than the available buffer capacity results in truncation.
//		CWStringStatic owner is responsible for allocating the buffer and releasing it
//		after the object is destroyed.
//
//---------------------------------------------------------------------------
class CWStringStatic : public CWString
{
private:
	// buffer capacity
	ULONG m_capacity;

	// private copy ctor
	CWStringStatic(const CWStringStatic &);

protected:
	// appends the contents of a buffer to the current string
	void AppendBuffer(const WCHAR *w_str_buffer);

public:
	// ctor
	CWStringStatic(WCHAR w_str_buffer[], ULONG capacity);

	// ctor with string initialization
	CWStringStatic(WCHAR w_str_buffer[], ULONG capacity,
				   const WCHAR w_str_init[]);

	// appends a string and replaces character with string
	void AppendEscape(const CWStringBase *str, WCHAR wc,
					  const WCHAR *w_str_replace);

	// appends a formatted string
	void AppendFormat(const WCHAR *format, ...);

	// appends a formatted string based on passed va list
	void AppendFormatVA(const WCHAR *format, VA_LIST va_args);

	// appends a null terminated character array
	virtual void AppendCharArray(const CHAR *sz);

	// appends a null terminated  wide character array
	virtual void AppendWideCharArray(const WCHAR *w_str);

	// dtor - owner is responsible for releasing the buffer
	virtual ~CWStringStatic()
	{
	}

	// resets string
	void Reset();
};
}  // namespace gpos

#endif
