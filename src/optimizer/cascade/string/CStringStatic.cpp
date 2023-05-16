//---------------------------------------------------------------------------
//	@filename:
//		CStringStatic.cpp
//
//	@doc:
//		Implementation of the ASCII-character String class
//		with static buffer allocation
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/string/CStringStatic.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/string/CWStringStatic.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CStringStatic::CStringStatic
//
//	@doc:
//		Ctor - initializes with empty string
//
//---------------------------------------------------------------------------
CStringStatic::CStringStatic(CHAR buffer[], ULONG capacity)
	: m_buffer(buffer), m_length(0), m_capacity(capacity)
{
	GPOS_ASSERT(NULL != buffer);
	GPOS_ASSERT(0 < m_capacity);

	m_buffer[0] = CHAR_EOS;
}


//---------------------------------------------------------------------------
//	@function:
//		CStringStatic::CStringStatic
//
//	@doc:
//		Ctor with string initialization
//
//---------------------------------------------------------------------------
CStringStatic::CStringStatic(CHAR buffer[], ULONG capacity,
							 const CHAR init_str[])
	: m_buffer(buffer), m_length(0), m_capacity(capacity)
{
	GPOS_ASSERT(NULL != buffer);
	GPOS_ASSERT(0 < m_capacity);

	AppendBuffer(init_str);
}


//---------------------------------------------------------------------------
//	@function:
//		CStringStatic::Equals
//
//	@doc:
//		Checks whether the string is byte-wise equal to a given string literal
//
//---------------------------------------------------------------------------
BOOL
CStringStatic::Equals(const CHAR *buf) const
{
	GPOS_ASSERT(NULL != buf);

	ULONG length = clib::Strlen(buf);
	return (m_length == length && 0 == clib::Strncmp(m_buffer, buf, length));
}


//---------------------------------------------------------------------------
//	@function:
//		CStringStatic::Append
//
//	@doc:
//		Appends a string
//
//---------------------------------------------------------------------------
void
CStringStatic::Append(const CStringStatic *str)
{
	AppendBuffer(str->Buffer());
}


//---------------------------------------------------------------------------
//	@function:
//		CStringStatic::AppendBuffer
//
//	@doc:
//		Appends the contents of a buffer to the current string
//
//---------------------------------------------------------------------------
void
CStringStatic::AppendBuffer(const CHAR *buf)
{
	GPOS_ASSERT(NULL != buf);
	ULONG length = clib::Strlen(buf);
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

	GPOS_ASSERT(m_capacity > length + m_length);

	clib::Strncpy(m_buffer + m_length, buf, length + 1);
	m_length += length;

	// terminate string
	m_buffer[m_length] = CHAR_EOS;

	GPOS_ASSERT(IsValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CStringStatic::AppendFormat
//
//	@doc:
//		Appends a formatted string
//
//---------------------------------------------------------------------------
void
CStringStatic::AppendFormat(const CHAR *format, ...)
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
//		CStringStatic::AppendFormatVA
//
//	@doc:
//		Appends a formatted string based on passed va list
//
//---------------------------------------------------------------------------
void
CStringStatic::AppendFormatVA(const CHAR *format, VA_LIST va_args)
{
	GPOS_ASSERT(NULL != format);

	// available space in buffer
	ULONG ulAvailable = m_capacity - m_length;

	// format string
	(void) clib::Vsnprintf(m_buffer + m_length, ulAvailable, format, va_args);

	// terminate string
	m_buffer[m_capacity - 1] = CHAR_EOS;
	m_length = clib::Strlen(m_buffer);

	GPOS_ASSERT(m_capacity > m_length);

	GPOS_ASSERT(IsValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CStringStatic::AppendWsz
//
//	@doc:
//		Appends wide character string
//
//---------------------------------------------------------------------------
void
CStringStatic::AppendConvert(const WCHAR *wc_str)
{
	ULONG length_entry = GPOS_WSZ_LENGTH(wc_str);

	if (m_capacity - m_length < length_entry)
	{
		length_entry = m_capacity - m_length - 1;
	}

	for (ULONG i = 0; i < length_entry; i++)
	{
		CHAR str_convert[MB_LEN_MAX];

		/* convert wide character to multi-byte array */
		ULONG char_length = clib::Wctomb(str_convert, wc_str[i]);
		GPOS_ASSERT(0 < char_length);

		// check if wide character is ASCII-compatible
		if (1 == char_length)
		{
			// simple cast; works only for ASCII characters
			m_buffer[m_length] = CHAR(wc_str[i]);
		}
		else
		{
			// substitute wide character
			m_buffer[m_length] = GPOS_WCHAR_UNPRINTABLE;
		}
		m_length++;
	}

	m_buffer[m_length] = CHAR_EOS;
	GPOS_ASSERT(IsValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CStringStatic::Reset
//
//	@doc:
//		Resets string
//
//---------------------------------------------------------------------------
void
CStringStatic::Reset()
{
	m_buffer[0] = CHAR_EOS;
	m_length = 0;
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CStringStatic::IsValid
//
//	@doc:
//		Checks whether a string is properly null-terminated
//
//---------------------------------------------------------------------------
bool
CStringStatic::IsValid() const
{
	return (m_length == clib::Strlen(m_buffer));
}

#endif