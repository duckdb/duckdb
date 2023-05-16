//---------------------------------------------------------------------------
//	@filename:
//		CMessage.h
//
//	@doc:
//		Error message container; each instance corresponds to a message as
//		loaded from an external configuration file;
//		Both warnings and errors;
//---------------------------------------------------------------------------
#ifndef GPOS_CMessage_H
#define GPOS_CMessage_H

#include "duckdb/optimizer/cascade/assert.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtable.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/types.h"

#define GPOS_WSZ_WSZLEN(x) (L##x), (gpos::clib::Wcslen(L##x))

#define GPOS_ERRMSG_FORMAT(...) gpos::CMessage::FormatMessage(__VA_ARGS__)

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CMessage
//
//	@doc:
//		Corresponds to individual message as defined in config file
//
//---------------------------------------------------------------------------
class CMessage
{
private:
	// severity
	ULONG m_severity;

	// format string
	const WCHAR *m_fmt;

	// length of format string
	ULONG m_fmt_len;

	// number of parameters
	ULONG m_num_params;

	// comment string
	const WCHAR *m_comment;

	// length of commen string
	ULONG m_comment_len;

public:
	// exception carries error number/identification
	CException m_exception;

	// TODO: 6/29/2010: incorporate string class
	// as soon as available
	//
	// ctor
	CMessage(CException exc, ULONG severity, const WCHAR *fmt, ULONG fmt_len,
			 ULONG num_params, const WCHAR *comment, ULONG comment_len);

	// copy ctor
	CMessage(const CMessage &);

	// format contents into given buffer
	void Format(CWStringStatic *buf, VA_LIST) const;

	// severity accessor
	ULONG
	GetSeverity() const
	{
		return m_severity;
	}

	// link object
	SLink m_link;

	// access a message by index
	static CMessage *GetMessage(ULONG index);

	// format an error message
	static void FormatMessage(CWStringStatic *str, ULONG major, ULONG minor,
							  ...);

#ifdef GPOS_DEBUG
	// debug print function
	IOstream &OsPrint(IOstream &);
#endif	// GPOS_DEBUG

};	// class CMessage
}  // namespace gpos

#endif