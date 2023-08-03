//---------------------------------------------------------------------------
//	@filename:
//		CWStringDynamic.h
//
//	@doc:
//		Wide character string class with dynamic buffer allocation.
//---------------------------------------------------------------------------
#ifndef GPOS_CWStringDynamic_H
#define GPOS_CWStringDynamic_H

#include "duckdb/optimizer/cascade/string/CWString.h"

#define GPOS_WSTR_DYNAMIC_CAPACITY_INIT (1 << 7)
#define GPOS_WSTR_DYNAMIC_STATIC_BUFFER (1 << 10)

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CWStringDynamic
//
//	@doc:
//		Implementation of the string interface with dynamic buffer allocation.
//		This CWStringDynamic class dynamically allocates memory when constructing a new
//		string, or when modifying a string. The memory is released at destruction time
//		or when the string is reset.
//
//---------------------------------------------------------------------------
class CWStringDynamic : public CWString
{
public:
	// string capacity
	ULONG m_capacity;

	// increase string capacity
	void IncreaseCapacity(ULONG requested);

	// find capacity that fits requested string size
	static ULONG Capacity(ULONG requested);

	// appends the contents of a buffer to the current string
	void AppendBuffer(const WCHAR* w_str_buffer);

public:
	// ctor
	CWStringDynamic();

	// private copy ctor
	CWStringDynamic(const CWStringDynamic &) = delete;
	
	// ctor - copies passed string
	CWStringDynamic(const WCHAR* w_str_buffer);
	
	// dtor
	virtual ~CWStringDynamic();
	
	// appends a string and replaces character with string
	void AppendEscape(const CWStringBase* str, WCHAR wc, const WCHAR *w_str_replace);

	// appends a formatted string
	void AppendFormat(const WCHAR* format, ...);

	// appends a null terminated character array
	virtual void AppendCharArray(const CHAR* sz);

	// appends a null terminated wide character array
	virtual void AppendWideCharArray(const WCHAR* w_str);

	// resets string
	void Reset();
};
}  // namespace gpos
#endif