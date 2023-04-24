//---------------------------------------------------------------------------
//	@filename:
//		CException.h
//
//	@doc:
//		Implements basic exception handling. All excpetion handling related
//		functionality is wrapped in macros to facilitate easy modifications
//		at a later point in time.
//---------------------------------------------------------------------------
#ifndef CException_H
#define CException_H

#include "duckdb/optimizer/cascade/types.h"

// SQL state code length
#define GPOS_SQLSTATE_LENGTH 5

// standard way to raise an exception
#define GPOS_RAISE(...) gpos::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)

// raises GPOS exception,
// these exceptions can later be translated to GPDB log severity levels
// so that they can be written in GPDB with appropriate severity level.
#define GPOS_THROW_EXCEPTION(...) gpos::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)

// helper to match a caught exception
#define GPOS_MATCH_EX(ex, major, minor) (major == ex.Major() && minor == ex.Minor())

// being of a try block w/o explicit handler
#define GPOS_TRY                         \
	do                                   \
	{                                    \
		CErrorHandler *err_hdl__ = NULL; \
		try                              \
		{
// begin of a try block
#define GPOS_TRY_HDL(perrhdl)               \
	do                                      \
	{                                       \
		CErrorHandler *err_hdl__ = perrhdl; \
		try                                 \
		{
// begin of a catch block
#define GPOS_CATCH_EX(exc)               \
	}                                    \
	catch (gpos::CException & exc)       \
	{                                    \
		{                                \
			if (NULL != err_hdl__)       \
				err_hdl__->Process(exc); \
		}


// end of a catch block
#define GPOS_CATCH_END \
	}                  \
	}                  \
	while (0)

// to be used inside a catch block
#define GPOS_RESET_EX ITask::Self()->GetErrCtxt()->Reset()
#define GPOS_RETHROW(exc) gpos::CException::Reraise(exc)

// short hands for frequently used exceptions
#define GPOS_ABORT GPOS_RAISE(CException::ExmaSystem, CException::ExmiAbort)
#define GPOS_OOM_CHECK(x)                                            \
	do                                                               \
	{                                                                \
		if (NULL == (void *) x)                                      \
		{                                                            \
			GPOS_RAISE(CException::ExmaSystem, CException::ExmiOOM); \
		}                                                            \
	} while (0)

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CException
//
//	@doc:
//		Basic exception class -- used for "throw by value/catch by reference"
//		Contains only a category (= major) and a error (= minor).
//
//---------------------------------------------------------------------------
class CException
{
public:
	// majors - reserve range 0-99
	enum ExMajor
	{
		ExmaInvalid = 0,
		ExmaSystem = 1,
		ExmaSQL = 2,
		ExmaUnhandled = 3,
		ExmaSentinel
	};

	// minors
	enum ExMinor
	{
		// system errors
		ExmiInvalid = 0,
		ExmiAbort,
		ExmiAssert,
		ExmiOOM,
		ExmiOutOfStack,
		ExmiAbortTimeout,
		ExmiIOError,
		ExmiNetError,
		ExmiOverflow,
		ExmiInvalidDeletion,
		// unexpected OOM during fault simulation
		ExmiUnexpectedOOMDuringFaultSimulation,
		// sql exceptions
		ExmiSQLDefault,
		ExmiSQLNotNullViolation,
		ExmiSQLCheckConstraintViolation,
		ExmiSQLMaxOneRow,
		ExmiSQLTest,
		// warnings
		ExmiDummyWarning,
		// unknown exception
		ExmiUnhandled,
		// illegal byte sequence
		ExmiIllegalByteSequence,
		ExmiSentinel
	};

	// structure for mapping exceptions to SQLerror codes
private:
	struct ErrCodeElem
	{
		// exception number
		ULONG m_exception_num;
		// SQL standard error code
		const CHAR *m_sql_state;
	};

	// error range
	ULONG m_major;

	// error number
	ULONG m_minor;

	// SQL state error code
	const CHAR *m_sql_state;

	// filename
	CHAR *m_filename;

	// line in file
	ULONG m_line;

	// severity level mapped to GPDB log severity level
	ULONG m_severity_level;

	// sql state error codes
	static const ErrCodeElem m_errcode[ExmiSQLTest - ExmiSQLDefault + 1];


	// internal raise API
	static void Raise(CException exc) __attribute__((__noreturn__));

	// get sql error code for given exception
	static const CHAR *GetSQLState(ULONG major, ULONG minor);

public:
	// severity levels
	enum ExSeverity
	{
		ExsevInvalid = 0,
		ExsevPanic,
		ExsevFatal,
		ExsevError,
		ExsevWarning,
		ExsevNotice,
		ExsevTrace,
		ExsevDebug1,
		ExsevSentinel
	};

	// severity levels
	static const CHAR *m_severity[ExsevSentinel];

	// ctor
	CException(ULONG major, ULONG minor);
	CException(ULONG major, ULONG minor, const CHAR *filename, ULONG line);
	CException(ULONG major, ULONG minor, const CHAR *filename, ULONG line, ULONG severity_level);

	// accessors
	ULONG
	Major() const
	{
		return m_major;
	}

	ULONG
	Minor() const
	{
		return m_minor;
	}

	const CHAR *
	Filename() const
	{
		return m_filename;
	}

	ULONG
	Line() const
	{
		return m_line;
	}

	ULONG
	SeverityLevel() const
	{
		return m_severity_level;
	}

	const CHAR *
	GetSQLState() const
	{
		return m_sql_state;
	}

	// simple equality
	BOOL
	operator==(const CException &exc) const
	{
		return m_major == exc.m_major && m_minor == exc.m_minor;
	}


	// simple inequality
	BOOL
	operator!=(const CException &exc) const
	{
		return !(*this == exc);
	}

	// equality function -- needed for hashtable
	static BOOL
	Equals(const CException &exc, const CException &excOther)
	{
		return exc == excOther;
	}

	// basic hash function
	static ULONG
	HashValue(const CException &exc)
	{
		return exc.m_major ^ exc.m_minor;
	}

	// wrapper around throw
	static void Raise(const CHAR *filename, ULONG line, ULONG major,
					  ULONG minor, ...) __attribute__((__noreturn__));

	// wrapper around throw with severity level
	static void Raise(const CHAR *filename, ULONG line, ULONG major,
					  ULONG minor, ULONG severity_level, ...)
		__attribute__((__noreturn__));

	// rethrow wrapper
	static void Reraise(CException exc, BOOL propagate = false)
		__attribute__((__noreturn__));

	// invalid exception
	static const CException m_invalid_exception;

};	// class CException
}  // namespace gpos

#endif
