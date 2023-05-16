//---------------------------------------------------------------------------
//	@filename:
//		CException.cpp
//
//	@doc:
//		Implements simplified exception handling.
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/error/CException.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/error/CErrorContext.h"
#include "duckdb/optimizer/cascade/task/CTask.h"

using namespace gpos;

const CHAR *CException::m_severity[] = {"INVALID", "PANIC",	 "FATAL", "ERROR", "WARNING", "NOTICE", "TRACE"};

// invalid exception
const CException CException::m_invalid_exception(CException::ExmaInvalid, CException::ExmiInvalid);

// standard SQL error codes
const CException::ErrCodeElem CException::m_errcode[] = {
	{ExmiSQLDefault, "XX000"},					 // internal error
	{ExmiSQLNotNullViolation, "23502"},			 // not null violation
	{ExmiSQLCheckConstraintViolation, "23514"},	 // check constraint violation
	{ExmiSQLMaxOneRow, "P0003"},				 // max one row
	{ExmiSQLTest, "XXXXX"}						 // test sql state
};


//---------------------------------------------------------------------------
//	@function:
//		CException::CException
//
//	@doc:
//		Constructor for exception record; given the situation in which
//		exceptions are raised, init all elements, do not allocate any memory
//		dynamically
//
//---------------------------------------------------------------------------
CException::CException(ULONG major, ULONG minor, const CHAR *filename, ULONG line)
	: m_major(major),
	  m_minor(minor),
	  m_filename(const_cast<CHAR *>(filename)),
	  m_line(line)
{
	m_severity_level = CException::ExsevSentinel;
	m_sql_state = GetSQLState(major, minor);
}

// ctor
CException::CException(ULONG major, ULONG minor, const CHAR *filename,
					   ULONG line, ULONG severity_level)
	: m_major(major),
	  m_minor(minor),
	  m_filename(const_cast<CHAR *>(filename)),
	  m_line(line),
	  m_severity_level(severity_level)
{
	m_sql_state = GetSQLState(major, minor);
}

//---------------------------------------------------------------------------
//	@function:
//		CException::CException
//
//	@doc:
//		Constructor for exception record; this version typically stored
//		in lookup structures etc.
//
//---------------------------------------------------------------------------
CException::CException(ULONG major, ULONG minor)
	: m_major(major), m_minor(minor), m_filename(NULL), m_line(0)
{
	m_severity_level = CException::ExsevSentinel;
	m_sql_state = GetSQLState(major, minor);
}


//---------------------------------------------------------------------------
//	@function:
//		CException::Raise
//
//	@doc:
//		Actual point where an exception is thrown; encapsulated in a function
//		(a) to facilitate debugging, i.e. function to set a breakpoint
//		(b) to allow for additional debugging tools such as stack dumps etc.
//			at a later point in time
//
//---------------------------------------------------------------------------
void CException::Raise(const CHAR *filename, ULONG line, ULONG major, ULONG minor, ...)
{
	// manufacture actual exception object
	CException exc(major, minor, filename, line);

	// during bootstrap there's no context object otherwise, record
	// all details in the context object
	if (NULL != ITask::Self())
	{
		CErrorContext *err_ctxt = CTask::Self()->ConvertErrCtxt();

		VA_LIST va_list;
		VA_START(va_list, minor);

		err_ctxt->Record(exc, va_list);

		VA_END(va_list);

		err_ctxt->Serialize();
	}

	Raise(exc);
}


void CException::Raise(const CHAR *filename, ULONG line, ULONG major, ULONG minor, ULONG severity_level...)
{
	// manufacture actual exception object
	CException exc(major, minor, filename, line, severity_level);

	// during bootstrap there's no context object otherwise, record
	// all details in the context object
	if (NULL != ITask::Self())
	{
		CErrorContext *err_ctxt = CTask::Self()->ConvertErrCtxt();

		VA_LIST va_list;
		VA_START(va_list, severity_level);

		err_ctxt->Record(exc, va_list);

		VA_END(va_list);

		err_ctxt->Serialize();
	}

	Raise(exc);
}


//---------------------------------------------------------------------------
//	@function:
//		CException::Reraise
//
//	@doc:
//		Throw/rethrow interface to reraise an already caught exc;
//		Wrapper that asserts there is a pending error;
//
//---------------------------------------------------------------------------
void CException::Reraise(CException exc, BOOL propagate)
{
	if (NULL != ITask::Self())
	{
		CErrorContext *err_ctxt = CTask::Self()->ConvertErrCtxt();
		GPOS_ASSERT(err_ctxt->IsPending());

		err_ctxt->SetRethrow();

		// serialize registered objects when current task propagates
		// an exception thrown by a child task
		if (propagate)
		{
			err_ctxt->GetStackDescriptor()->BackTrace();
			err_ctxt->Serialize();
		}
	}

	Raise(exc);
}


//---------------------------------------------------------------------------
//	@function:
//		CException::Raise
//
//	@doc:
//		Throw/rethrow interface
//
//---------------------------------------------------------------------------
void
CException::Raise(CException exc)
{
#ifdef GPOS_DEBUG
	if (NULL != ITask::Self())
	{
		IErrorContext *err_ctxt = ITask::Self()->GetErrCtxt();
		GPOS_ASSERT_IMP(err_ctxt->IsPending(),
						err_ctxt->GetException() == exc &&
							"Rethrow inconsistent with current error context");
	}
#endif	// GPOS_DEBUG

	throw exc;
}

//---------------------------------------------------------------------------
//	@function:
//		CException::GetSQLState
//
//	@doc:
//		Get sql state code for exception
//
//---------------------------------------------------------------------------
const CHAR *
CException::GetSQLState(ULONG major, ULONG minor)
{
	const CHAR *sql_state = m_errcode[0].m_sql_state;
	if (ExmaSQL == major)
	{
		ULONG sql_states = GPOS_ARRAY_SIZE(m_errcode);
		for (ULONG ul = 0; ul < sql_states; ul++)
		{
			ErrCodeElem errcode = m_errcode[ul];
			if (minor == errcode.m_exception_num)
			{
				sql_state = errcode.m_sql_state;
				break;
			}
		}
	}
	return sql_state;
}

// EOF
