//---------------------------------------------------------------------------
//	@filename:
//		CTaskContext.h
//
//	@doc:
//		Context object for task; holds configurable state of worker, e.g.
//		GUCs, trace options, output streams etc.; by default inherited from
//		parent task
//---------------------------------------------------------------------------
#ifndef GPOS_CTaskContext_H
#define GPOS_CTaskContext_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CBitSet.h"

namespace gpos
{
class CTaskContext
{
	// trace flag iterator needs access to trace vector
	friend class CTraceFlagIter;

private:
	// trace vector
	CBitSet *m_bitset;

	// output log abstraction
	ILogger *m_log_out;

	// error log abstraction
	ILogger *m_log_err;

	// locale of messages
	ELocale m_locale;

public:
	// basic ctor; used only for the main worker
	CTaskContext(CMemoryPool *mp);

	// copy ctor
	// used to inherit parent task's context
	CTaskContext(CMemoryPool *mp, const CTaskContext &task_ctxt);

	// dtor
	~CTaskContext();

	// accessors
	inline ILogger *
	GetOutputLogger() const
	{
		return m_log_out;
	}

	inline ILogger *
	GetErrorLogger() const
	{
		return m_log_err;
	}

	void
	SetLogOut(ILogger *log_out)
	{
		GPOS_ASSERT(NULL != log_out);
		m_log_out = log_out;
	}

	void
	SetLogErr(ILogger *log_err)
	{
		GPOS_ASSERT(NULL != log_err);
		m_log_err = log_err;
	}

	// set trace flag
	BOOL SetTrace(ULONG trace, BOOL val);

	// test if tracing on
	inline BOOL
	IsTraceSet(ULONG trace)
	{
		return m_bitset->Get(trace);
	}

	// locale
	ELocale
	Locale() const
	{
		return m_locale;
	}

	void
	SetLocale(ELocale locale)
	{
		m_locale = locale;
	}

	CBitSet *
	copy_trace_flags(CMemoryPool *mp) const
	{
		return GPOS_NEW(mp) CBitSet(mp, *m_bitset);
	}

};	// class CTaskContext
}  // namespace gpos

#endif