//---------------------------------------------------------------------------
//	@filename:
//		CErrorContext.h
//
//	@doc:
//		Error context to record error message, stack, etc.
//---------------------------------------------------------------------------
#ifndef CErrorContext_H
#define CErrorContext_H

#include "duckdb/optimizer/cascade/common/CStackDescriptor.h"
#include "duckdb/optimizer/cascade/error/CException.h"
#include "duckdb/optimizer/cascade/error/CMiniDumper.h"
#include "duckdb/optimizer/cascade/error/CSerializable.h"
#include "duckdb/optimizer/cascade/error/IErrorContext.h"
#include "duckdb/optimizer/cascade/io/ioutils.h"
#include "duckdb/optimizer/cascade/string/CWStringStatic.h"

#define GPOS_ERROR_MESSAGE_BUFFER_SIZE (4 * 1024)

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CErrorContext
//
//	@doc:
//		Context object, owned by Task
//
//---------------------------------------------------------------------------
class CErrorContext : public IErrorContext
{
private:
	// copy of original exception
	CException m_exception;

	// exception severity
	ULONG m_severity;

	// flag to indicate if handled yet
	BOOL m_pending;

	// flag to indicate if handled yet
	BOOL m_rethrown;

	// flag to indicate that we are currently serializing this.
	BOOL m_serializing;

	// error message buffer
	WCHAR m_error_msg[GPOS_ERROR_MESSAGE_BUFFER_SIZE];

	// system error message buffer
	CHAR m_system_error_msg[GPOS_ERROR_MESSAGE_BUFFER_SIZE];

	// string with static buffer allocation
	CWStringStatic m_static_buffer;

	// stack descriptor to store error stack info
	CStackDescriptor m_stack_descriptor;

	// list of objects to serialize on exception
	CList<CSerializable> m_serializable_objects_list;

	// minidump handler
	CMiniDumper *m_mini_dumper_handle;

	// private copy ctor
	CErrorContext(const CErrorContext &);

public:
	// ctor
	explicit CErrorContext(CMiniDumper *mini_dumper_handle = NULL);

	// dtor
	virtual ~CErrorContext();

	// reset context, clear out handled error
	virtual void Reset();

	// record error context
	virtual void Record(CException &exc, VA_LIST);

	// accessors
	virtual CException
	GetException() const
	{
		return m_exception;
	}

	virtual const WCHAR *
	GetErrorMsg() const
	{
		return m_error_msg;
	}

	CStackDescriptor *
	GetStackDescriptor()
	{
		return &m_stack_descriptor;
	}

	CMiniDumper *
	GetMiniDumper()
	{
		return m_mini_dumper_handle;
	}

	// register minidump handler
	void
	Register(CMiniDumper *mini_dumper_handle)
	{
		GPOS_ASSERT(NULL == m_mini_dumper_handle);

		m_mini_dumper_handle = mini_dumper_handle;
	}

	// unregister minidump handler
	void
	Unregister(
#ifdef GPOS_DEBUG
		CMiniDumper *mini_dumper_handle
#endif	// GPOS_DEBUG
	)
	{
		GPOS_ASSERT(mini_dumper_handle == m_mini_dumper_handle);
		m_mini_dumper_handle = NULL;
	}

	// register object to serialize
	void
	Register(CSerializable *serializable_obj)
	{
		m_serializable_objects_list.Append(serializable_obj);
	}

	// unregister object to serialize
	void
	Unregister(CSerializable *serializable_obj)
	{
		m_serializable_objects_list.Remove(serializable_obj);
	}

	// serialize registered objects
	void Serialize();

	// copy necessary info for error propagation
	virtual void CopyPropErrCtxt(const IErrorContext *perrctxt);

	// severity accessor
	virtual ULONG
	GetSeverity() const
	{
		return m_severity;
	}

	// set severity
	virtual void
	SetSev(ULONG severity)
	{
		m_severity = severity;
	}

	// print error stack trace
	virtual void
	AppendStackTrace()
	{
		m_static_buffer.AppendFormat(GPOS_WSZ_LIT("\nStack trace:\n"));
		m_stack_descriptor.AppendTrace(&m_static_buffer);
	}

	// print errno message
	virtual void AppendErrnoMsg();

	virtual BOOL
	IsPending() const
	{
		return m_pending;
	}

	virtual BOOL
	IsRethrown() const
	{
		return m_rethrown;
	}

	virtual void
	SetRethrow()
	{
		m_rethrown = true;
	}

};	// class CErrorContext
}  // namespace gpos

#endif
