//---------------------------------------------------------------------------
//	@filename:
//		CRefCount.h
//
//	@doc:
//		Base class for reference counting in the optimizer;
//		Ref-counting is single-threaded only;
//		Enforces allocation on the heap, i.e. no deletion unless the
//		count has really dropped to zero
//---------------------------------------------------------------------------
#ifndef GPOS_CRefCount_H
#define GPOS_CRefCount_H

#include "duckdb/optimizer/cascade/assert.h"
#include "duckdb/optimizer/cascade/common/CHeapObject.h"
#include "duckdb/optimizer/cascade/error/CException.h"
#include "duckdb/optimizer/cascade/task/ITask.h"
#include "duckdb/optimizer/cascade/types.h"
#include "duckdb/optimizer/cascade/utils.h"

// pattern used to mark deallocated memory, this must match
// GPOS_MEM_FREED_PATTERN_CHAR in CMemoryPool.h
#ifdef GPOS_32BIT
#define GPOS_WIPED_MEM_PATTERN 0xCdCdCdCd
#else
#define GPOS_WIPED_MEM_PATTERN 0xCdCdCdCdCdCdCdCd
#endif

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CRefCount
//
//	@doc:
//		Basic reference counting
//
//---------------------------------------------------------------------------
class CRefCount : public CHeapObject
{
private:
	// reference counter -- first in class to be in sync with Check()
	ULONG_PTR m_refs;

#ifdef GPOS_DEBUG
	// sanity check to detect deleted memory
	void
	Check()
	{
		// assert that first member of class has not been wiped
		GPOS_ASSERT(m_refs != GPOS_WIPED_MEM_PATTERN);
	}
#endif	// GPOS_DEBUG

	// private copy ctor
	CRefCount(const CRefCount &);

public:
	// ctor
	CRefCount() : m_refs(1)
	{
	}

	// dtor
	virtual ~CRefCount()
	{
		// enforce strict ref-counting unless we're in a pending exception,
		// e.g., a ctor has thrown
		GPOS_ASSERT(NULL == ITask::Self() ||
					ITask::Self()->HasPendingExceptions() || 0 == m_refs);
	}

	// return ref-count
	ULONG_PTR
	RefCount() const
	{
		return m_refs;
	}

	// return true if calling object's destructor is allowed
	virtual BOOL
	Deletable() const
	{
		return true;
	}

	// count up
	void
	AddRef()
	{
#ifdef GPOS_DEBUG
		Check();
#endif	// GPOS_DEBUG
		m_refs++;
	}

	// count down
	void
	Release()
	{
#ifdef GPOS_DEBUG
		Check();
#endif	// GPOS_DEBUG
		m_refs--;

		if (0 == m_refs)
		{
			if (!Deletable())
			{
				// restore ref-count
				AddRef();

				// deletion is not allowed
				GPOS_RAISE(CException::ExmaSystem,
						   CException::ExmiInvalidDeletion);
			}

			GPOS_DELETE(this);
		}
	}

	// safe version of Release -- handles NULL pointers
	static void
	SafeRelease(CRefCount *rc)
	{
		if (NULL != rc)
		{
			rc->Release();
		}
	}

#ifdef GPOS_DEBUG
	// debug print for interactive debugging sessions only
	void DbgPrint() const;
#endif	// GPOS_DEBUG

	// print function
	virtual IOstream &
	OsPrint(IOstream &os) const
	{
		return os;
	}


};	// class CRefCount
}  // namespace gpos

#endif
