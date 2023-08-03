//---------------------------------------------------------------------------
//	@filename:
//		CWStringDynamic.cpp
//
//	@doc:
//		Implementation of the wide character string class
//		with dynamic buffer allocation.
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/string/CStringStatic.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::CWStringDynamic
//
//	@doc:
//		Constructs an empty string
//
//---------------------------------------------------------------------------
CWStringDynamic::CWStringDynamic()
	: CWString(0), m_capacity(0)
{
	Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::CWStringDynamic
//
//	@doc:
//		Constructs a new string and initializes it with the given buffer
//
//---------------------------------------------------------------------------
CWStringDynamic::CWStringDynamic(const WCHAR* w_str_buffer)
	: CWString(GPOS_WSZ_LENGTH(w_str_buffer)), m_capacity(0)
{
	Reset();
	AppendBuffer(w_str_buffer);
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::~CWStringDynamic
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CWStringDynamic::~CWStringDynamic()
{
	Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CWString::Reset
//
//	@doc:
//		Resets string
//
//---------------------------------------------------------------------------
void CWStringDynamic::Reset()
{
	if (NULL != m_w_str_buffer && &m_empty_wcstr != m_w_str_buffer)
	{
		delete[] m_w_str_buffer;
	}
	m_w_str_buffer = const_cast<WCHAR*>(&m_empty_wcstr);
	m_length = 0;
	m_capacity = 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendBuffer
//
//	@doc:
//		Appends the contents of a buffer to the current string
//
//---------------------------------------------------------------------------
void CWStringDynamic::AppendBuffer(const WCHAR* w_str)
{
	ULONG length = GPOS_WSZ_LENGTH(w_str);
	if (0 == length)
	{
		return;
	}
	// expand buffer if needed
	ULONG new_length = m_length + length;
	if (new_length + 1 > m_capacity)
	{
		IncreaseCapacity(new_length);
	}
	clib::WcStrNCpy(m_w_str_buffer + m_length, w_str, length + 1);
	m_length = new_length;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendWideCharArray
//
//	@doc:
//		Appends a a null terminated wide character array
//
//---------------------------------------------------------------------------
void CWStringDynamic::AppendWideCharArray(const WCHAR* w_str)
{
	AppendBuffer(w_str);
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendCharArray
//
//	@doc:
//		Appends a a null terminated character array
//
//---------------------------------------------------------------------------
void CWStringDynamic::AppendCharArray(const CHAR* sz)
{
	// expand buffer if needed
	const ULONG length = GPOS_SZ_LENGTH(sz);
	ULONG new_length = m_length + length;
	if (new_length + 1 > m_capacity)
	{
		IncreaseCapacity(new_length);
	}
	WCHAR *w_str_buffer = new WCHAR[length + 1];
	clib::Mbstowcs(w_str_buffer, sz, length);
	// append input string to current end of buffer
	(void) clib::Wmemcpy(m_w_str_buffer + m_length, w_str_buffer, length + 1);
	delete[] w_str_buffer;
	m_w_str_buffer[new_length] = WCHAR_EOS;
	m_length = new_length;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendFormat
//
//	@doc:
//		Appends a formatted string
//
//---------------------------------------------------------------------------
void CWStringDynamic::AppendFormat(const WCHAR* format, ...)
{
	using clib::Vswprintf;
	VA_LIST va_args;
	// determine length of format string after expansion
	INT res = -1;
	// attempt to fit the formatted string in a static array
	WCHAR w_str_buf_static[GPOS_WSTR_DYNAMIC_STATIC_BUFFER];
	// get arguments
	VA_START(va_args, format);
	// try expanding the formatted string in the buffer
	res = Vswprintf(w_str_buf_static, GPOS_ARRAY_SIZE(w_str_buf_static), format, va_args);
	// reset arguments
	VA_END(va_args);
	// estimated number of characters in expanded format string
	ULONG size = std::max(GPOS_WSZ_LENGTH(format), GPOS_ARRAY_SIZE(w_str_buf_static));
	// if the static buffer is too small, find the formatted string
	// length by trying to store it in a buffer of increasing size
	while (-1 == res)
	{
		// try with a bigger buffer this time
		size *= 2;
		WCHAR* a_w_str_buff = new WCHAR[size + 1];
		// get arguments
		VA_START(va_args, format);
		// try expanding the formatted string in the buffer
		res = Vswprintf(a_w_str_buff, size, format, va_args);
		// reset arguments
		VA_END(va_args);
	}
	// expand buffer if needed
	ULONG new_length = m_length + ULONG(res);
	if (new_length + 1 > m_capacity)
	{
		IncreaseCapacity(new_length);
	}
	// get arguments
	VA_START(va_args, format);
	// print va_args to string
	Vswprintf(m_w_str_buffer + m_length, res + 1, format, va_args);
	// reset arguments
	VA_END(va_args);
	m_length = new_length;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendEscape
//
//	@doc:
//		Appends a string and replaces character with string
//
//---------------------------------------------------------------------------
void CWStringDynamic::AppendEscape(const CWStringBase* str, WCHAR wc, const WCHAR *w_str_replace)
{
	if (str->IsEmpty())
	{
		return;
	}
	// count how many times the character to be escaped appears in the string
	ULONG occurrences = str->CountOccurrencesOf(wc);
	if (0 == occurrences)
	{
		Append(str);
		return;
	}
	ULONG length = str->Length();
	const WCHAR *w_str = str->GetBuffer();
	ULONG length_replace = GPOS_WSZ_LENGTH(w_str_replace);
	ULONG new_length = m_length + length + (length_replace - 1) * occurrences;
	if (new_length + 1 > m_capacity)
	{
		IncreaseCapacity(new_length);
	}
	// append new contents while replacing character with escaping string
	for (ULONG i = 0, j = m_length; i < length; i++)
	{
		if (wc == w_str[i] && !str->HasEscapedCharAt(i))
		{
			clib::WcStrNCpy(m_w_str_buffer + j, w_str_replace, length_replace);
			j += length_replace;
		}
		else
		{
			m_w_str_buffer[j++] = w_str[i];
		}
	}
	// terminate string
	m_w_str_buffer[new_length] = WCHAR_EOS;
	m_length = new_length;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::IncreaseCapacity
//
//	@doc:
//		Increase string capacity
//
//---------------------------------------------------------------------------
void CWStringDynamic::IncreaseCapacity(ULONG requested)
{
	ULONG capacity = Capacity(requested + 1);
	WCHAR* a_w_str_new_buff = new WCHAR[capacity];
	if (0 < m_length)
	{
		// current string is not empty: copy it to the resulting string
		a_w_str_new_buff = clib::WcStrNCpy(a_w_str_new_buff, m_w_str_buffer, m_length);
	}
	// release old buffer
	if (m_w_str_buffer != &m_empty_wcstr)
	{
		delete[] m_w_str_buffer;
	}
	m_w_str_buffer = a_w_str_new_buff;
	m_capacity = capacity;
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::Capacity
//
//	@doc:
//		Find capacity that fits requested string size
//
//---------------------------------------------------------------------------
ULONG
CWStringDynamic::Capacity(ULONG requested)
{
	ULONG capacity = GPOS_WSTR_DYNAMIC_CAPACITY_INIT;
	while (capacity <= requested + 1)
	{
		capacity = capacity << 1;
	}

	return capacity;
}