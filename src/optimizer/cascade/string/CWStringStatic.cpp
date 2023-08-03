//---------------------------------------------------------------------------
//	@filename:
//		CWStringStatic.cpp
//
//	@doc:
//		Implementation of the wide character string class
//		with static buffer allocation
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/string/CWStringStatic.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/string/CStringStatic.h"

using namespace gpos;

#define GPOS_STATIC_STR_BUFFER_LENGTH 5000

//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::CWStringStatic
//
//	@doc:
//		Ctor - initializes with empty string
//
//---------------------------------------------------------------------------
CWStringStatic::CWStringStatic(WCHAR w_str_buffer[], ULONG capacity)
	: CWString(0), m_capacity(capacity)
{
	m_w_str_buffer = w_str_buffer;
	m_w_str_buffer[0] = WCHAR_EOS;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::CWStringStatic
//
//	@doc:
//		Ctor - initializes with passed string
//
//---------------------------------------------------------------------------
CWStringStatic::CWStringStatic(WCHAR w_str_buffer[], ULONG capacity, const WCHAR w_str_init[])
	: CWString(0), m_capacity(capacity)
{
	m_w_str_buffer = w_str_buffer;
	AppendBuffer(w_str_init);
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendBuffer
//
//	@doc:
//		Appends the contents of a buffer to the current string
//
//---------------------------------------------------------------------------
void CWStringStatic::AppendBuffer(const WCHAR *w_str_buffer)
{
	ULONG length = GPOS_WSZ_LENGTH(w_str_buffer);
	if (0 == length || m_capacity == m_length)
	{
		return;
	}
	// check if new length exceeds capacity
	if (m_capacity <= length + m_length)
	{
		// truncate string
		length = m_capacity - m_length - 1;
	}
	clib::WcStrNCpy(m_w_str_buffer + m_length, w_str_buffer, length + 1);
	m_length += length;
	// terminate string
	m_w_str_buffer[m_length] = WCHAR_EOS;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendWideCharArray
//
//	@doc:
//		Appends a null terminated wide character array
//
//---------------------------------------------------------------------------
void CWStringStatic::AppendWideCharArray(const WCHAR *w_str)
{
	AppendBuffer(w_str);
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendCharArray
//
//	@doc:
//		Appends a null terminated character array
//
//---------------------------------------------------------------------------
void CWStringStatic::AppendCharArray(const CHAR *sz)
{
	if (0 == GPOS_SZ_LENGTH(sz) || m_capacity == m_length)
	{
		return;
	}
	ULONG length = GPOS_SZ_LENGTH(sz);
	if (length >= GPOS_STATIC_STR_BUFFER_LENGTH)
	{
		// if input string is larger than buffer length, use AppendFormat
		AppendFormat(GPOS_WSZ_LIT("%s"), sz);
		return;
	}
	// otherwise, append to wide string character array directly
	WCHAR w_str_buffer[GPOS_STATIC_STR_BUFFER_LENGTH];
	// convert input string to wide character buffer
	clib::Mbstowcs(w_str_buffer, sz, length);
	// check if new length exceeds capacity
	if (m_capacity <= length + m_length)
	{
		// truncate string
		length = m_capacity - m_length - 1;
	}
	// append input string to current end of buffer
	(void) clib::Wmemcpy(m_w_str_buffer + m_length, w_str_buffer, length + 1);
	m_length += length;
	m_w_str_buffer[m_length] = WCHAR_EOS;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendFormat
//
//	@doc:
//		Appends a formatted string
//
//---------------------------------------------------------------------------
void CWStringStatic::AppendFormat(const WCHAR *format, ...)
{
	VA_LIST va_args;
	// get arguments
	VA_START(va_args, format);
	AppendFormatVA(format, va_args);
	// reset arguments
	VA_END(va_args);
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendFormatVA
//
//	@doc:
//		Appends a formatted string based on passed va list
//
//---------------------------------------------------------------------------
void CWStringStatic::AppendFormatVA(const WCHAR *format, VA_LIST va_args)
{
	// available space in buffer
	ULONG ulAvailable = m_capacity - m_length;
	// format string
	(void) clib::Vswprintf(m_w_str_buffer + m_length, ulAvailable, format, va_args);
	m_w_str_buffer[m_capacity - 1] = WCHAR_EOS;
	m_length = GPOS_WSZ_LENGTH(m_w_str_buffer);
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::Reset
//
//	@doc:
//		Resets string
//
//---------------------------------------------------------------------------
void CWStringStatic::Reset()
{
	m_w_str_buffer[0] = WCHAR_EOS;
	m_length = 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendEscape
//
//	@doc:
//		Appends a string and replaces character with string
//
//---------------------------------------------------------------------------
void CWStringStatic::AppendEscape(const CWStringBase *str, WCHAR wc, const WCHAR *w_str_replace)
{
	if (str->IsEmpty())
	{
		return;
	}
	ULONG length = str->Length();
	ULONG length_replace = GPOS_WSZ_LENGTH(w_str_replace);
	ULONG ulLengthNew = m_length;
	const WCHAR *w_str = str->GetBuffer();
	for (ULONG i = 0; i < length && ulLengthNew < m_capacity - 1; i++)
	{
		if (wc == w_str[i] && !str->HasEscapedCharAt(i))
		{
			// check for overflow
			ULONG ulLengthCopy = std::min(length_replace, m_capacity - ulLengthNew - 1);
			clib::WcStrNCpy(m_w_str_buffer + ulLengthNew, w_str_replace, ulLengthCopy);
			ulLengthNew += ulLengthCopy;
		}
		else
		{
			m_w_str_buffer[ulLengthNew] = w_str[i];
			ulLengthNew++;
		}
	}
	// terminate string
	m_w_str_buffer[ulLengthNew] = WCHAR_EOS;
	m_length = ulLengthNew;
}