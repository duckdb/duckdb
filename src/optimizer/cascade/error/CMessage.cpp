//---------------------------------------------------------------------------
//	@filename:
//		CMessage.cpp
//
//	@doc:
//		Implements message records
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/error/CMessage.h"
#include "duckdb/optimizer/cascade/error/CMessageRepository.h"
#include "duckdb/optimizer/cascade/utils.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMessage::CMessage
//
//	@doc:
//
//---------------------------------------------------------------------------
CMessage::CMessage(CException exc, ULONG severity, const WCHAR *wszFmt, ULONG ulLenFmt, ULONG ulParams, const WCHAR *wszComment, ULONG ulLenComment)
	: m_severity(severity), m_fmt(wszFmt), m_fmt_len(ulLenFmt), m_num_params(ulParams), m_comment(wszComment), m_comment_len(ulLenComment), m_exception(exc)
{
	// TODO: 6/29/2010; incorporate string class
}

//---------------------------------------------------------------------------
//	@function:
//		CMessage::CMessage
//
//	@doc:
//		copy ctor
//
//---------------------------------------------------------------------------
CMessage::CMessage(const CMessage &msg)
	: m_severity(msg.m_severity), m_fmt(msg.m_fmt), m_fmt_len(msg.m_fmt_len), m_num_params(msg.m_num_params), m_comment(msg.m_comment), m_comment_len(msg.m_comment_len), m_exception(msg.m_exception)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::EresFormat
//
//	@doc:
//		Format error message into given buffer
//
//---------------------------------------------------------------------------
void CMessage::Format(CWStringStatic *pwss, VA_LIST vl) const
{
	pwss->AppendFormatVA(m_fmt, vl);
}

//---------------------------------------------------------------------------
//	@function:
//		CMessage::FormatMessage
//
//	@doc:
//		Format the message corresponding to the given exception
//
//---------------------------------------------------------------------------
void CMessage::FormatMessage(CWStringStatic *str, ULONG major, ULONG minor, ...)
{
	// manufacture actual exception object
	CException exc(major, minor);

	// during bootstrap there's no context object otherwise, record
	// all details in the context object
	if (NULL != ITask::Self())
	{
		VA_LIST valist;
		VA_START(valist, minor);

		ELocale locale = ITask::Self()->Locale();
		CMessage *msg =
			CMessageRepository::GetMessageRepository()->LookupMessage(exc,
																	  locale);
		msg->Format(str, valist);

		VA_END(valist);
	}
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMessage::OsPrint
//
//	@doc:
//		debug print of message (without filling in parameters)
//
//---------------------------------------------------------------------------
IOstream &
CMessage::OsPrint(IOstream &os)
{
	os << "Message No: " << m_exception.Major() << "-" << m_exception.Minor()
	   << std::endl
	   << "Message:   \"" << m_fmt << "\" [" << m_fmt_len << "]" << std::endl
	   << "Parameters: " << m_num_params << std::endl
	   << "Comments:  \"" << m_comment << "\" [" << m_comment_len << "]"
	   << std::endl;

	return os;
}
#endif	// GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMessage::GetMessage
//
//	@doc:
//		Access a message by its index
//
//---------------------------------------------------------------------------
CMessage* CMessage::GetMessage(ULONG index)
{
	GPOS_ASSERT(index < CException::ExmiSentinel);

	// Basic system-side messages in English
	static CMessage msg[CException::ExmiSentinel] = {
		CMessage(CException(CException::ExmaInvalid, CException::ExmiInvalid),
				 CException::ExsevError, GPOS_WSZ_WSZLEN("Unknown error: %ls"),
				 1,	 // # params
				 GPOS_WSZ_WSZLEN("This message is used if no error message "
								 "can be found at handling time")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiAbort),
				 CException::ExsevError, GPOS_WSZ_WSZLEN("Statement aborted"),
				 0,	 // # params
				 GPOS_WSZ_WSZLEN("Message to indicate statement was"
								 "canceled (both internal/external)")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiAssert),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("%s:%d: Failed assertion: %ls"),
				 3,	 // params: filename (CHAR*), line, assertion condition
				 GPOS_WSZ_WSZLEN("Internal assertion has been violated")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiOOM),
				 CException::ExsevError, GPOS_WSZ_WSZLEN("Out of memory"),
				 0,	 // # params
				 GPOS_WSZ_WSZLEN(
					 "Memory pool or virtual memory on system exhausted")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiOutOfStack),
				 CException::ExsevError, GPOS_WSZ_WSZLEN("Out of stack space"),
				 0,	 // # params
				 GPOS_WSZ_WSZLEN(
					 "Maximally permitted stack allocation exceeded;"
					 "This is not a stack overflow detected by the OS but"
					 "caught internally, i.e., the process is still safe"
					 "to operate afterwards")),

		CMessage(
			CException(CException::ExmaSystem, CException::ExmiAbortTimeout),
			CException::ExsevError,
			GPOS_WSZ_WSZLEN("Last check for aborts before %d ms, at:\n%ls"),
			2,	// # params: interval (ULONG), stack trace (WCHAR *)
			GPOS_WSZ_WSZLEN(
				"Interval between successive abort checkpoints exceeds maximum")),

		CMessage(
			CException(CException::ExmaSystem, CException::ExmiIOError),
			CException::ExsevError,
			GPOS_WSZ_WSZLEN("Error during I/O operation, error code: %d"),
			1,	// # params
			GPOS_WSZ_WSZLEN(
				"I/O operation failed; use error code to identify the error type")),

		CMessage(
			CException(CException::ExmaSystem, CException::ExmiNetError),
			CException::ExsevError,
			GPOS_WSZ_WSZLEN(
				"Error during networking operation, error code: %d"),
			1,	// # params
			GPOS_WSZ_WSZLEN(
				"Networking operation failed; use error code to identify the error type")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiOverflow),
				 CException::ExsevError, GPOS_WSZ_WSZLEN("Arithmetic Overflow"),
				 0,	 // # params
				 GPOS_WSZ_WSZLEN("Arithmetic Overflow")),

		CMessage(
			CException(CException::ExmaSystem, CException::ExmiInvalidDeletion),
			CException::ExsevError,
			GPOS_WSZ_WSZLEN("Error during delete operation, error code: %d"),
			1,	// # params
			GPOS_WSZ_WSZLEN(
				"Delete operation failed; use error code to identify the error type")),

		CMessage(
			CException(CException::ExmaSystem,
					   CException::ExmiUnexpectedOOMDuringFaultSimulation),
			CException::ExsevError,
			GPOS_WSZ_WSZLEN("Unexpected out of memory during fault simulation"),
			0,	// # params
			GPOS_WSZ_WSZLEN(
				"Unexpected out of memory during fault simulation")),

		CMessage(
			CException(CException::ExmaSystem, CException::ExmiDummyWarning),
			CException::ExsevWarning,
			GPOS_WSZ_WSZLEN("This is a dummy warning"),
			1,	// # params
			GPOS_WSZ_WSZLEN("Used to test warning logging")),

		CMessage(CException(CException::ExmaSQL, CException::ExmiSQLDefault),
				 CException::ExsevError, GPOS_WSZ_WSZLEN("Internal error"), 0,
				 GPOS_WSZ_WSZLEN("Internal error")),

		CMessage(
			CException(CException::ExmaSQL,
					   CException::ExmiSQLNotNullViolation),
			CException::ExsevError,
			GPOS_WSZ_WSZLEN(
				"Not null constraint for column %ls of table %ls was violated"),
			2,	// column name, table name
			GPOS_WSZ_WSZLEN("Not null constraint for table was violated")),

		CMessage(
			CException(CException::ExmaSQL,
					   CException::ExmiSQLCheckConstraintViolation),
			CException::ExsevError,
			GPOS_WSZ_WSZLEN("Check constraint %ls for table %ls was violated"),
			2,	// constraint name, table name
			GPOS_WSZ_WSZLEN("Check constraint for table was violated")),

		CMessage(
			CException(CException::ExmaSQL, CException::ExmiSQLMaxOneRow),
			CException::ExsevError,
			GPOS_WSZ_WSZLEN(
				"Expected no more than one row to be returned by expression"),
			0,
			GPOS_WSZ_WSZLEN(
				"Expected no more than one row to be returned by expression")),

		CMessage(CException(CException::ExmaSQL, CException::ExmiSQLTest),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Test sql error message: %ls"),
				 1,	 // error message
				 GPOS_WSZ_WSZLEN("Test sql error message")),

		CMessage(
			CException(CException::ExmaUnhandled, CException::ExmiUnhandled),
			CException::ExsevError, GPOS_WSZ_WSZLEN("Unhandled exception"), 0,
			GPOS_WSZ_WSZLEN("Unhandled exception")),

		CMessage(
			CException(CException::ExmaSystem,
					   CException::ExmiIllegalByteSequence),
			CException::ExsevError,
			GPOS_WSZ_WSZLEN(
				"Invalid multibyte character for locale encountered in metadata name"),
			0,
			GPOS_WSZ_WSZLEN(
				"Invalid multibyte character for locale encountered in metadata name")),
	};

	return &msg[index];
}