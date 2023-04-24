//---------------------------------------------------------------------------
//	@filename:
//		CMemoryPool.h
//
//	@doc:
//		Memory pool wrapper that cleans up the pool automatically
//
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef CAutoMemoryPool_H
#define CAutoMemoryPool_H

#include "gpos/assert.h"
#include "gpos/common/CStackObject.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/types.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CAutoMemoryPool
//
//	@doc:
//		Automatic memory pool interface;
//		tears down memory pool when going out of scope;
//
//		For cleanliness, do not provide an automatic cast to CMemoryPool
//
//---------------------------------------------------------------------------
class CAutoMemoryPool : public CStackObject
{
public:
	enum ELeakCheck
	{
		ElcNone,  // no leak checking -- to be deprecated

		ElcExc,	   // check for leaks unless an exception is pending (default)
		ElcStrict  // always check for leaks
	};

private:
	// private copy ctor
	CAutoMemoryPool(const CAutoMemoryPool &);

	// memory pool to protect
	CMemoryPool *m_mp;

	// type of leak check to perform
	ELeakCheck m_leak_check_type;

public:
	// ctor
	CAutoMemoryPool(ELeakCheck leak_check_type = ElcExc);

	// dtor
	~CAutoMemoryPool();

	// accessor
	CMemoryPool *
	Pmp() const
	{
		return m_mp;
	}

	// detach from pool
	CMemoryPool *Detach();

};	// CAutoMemoryPool
}  // namespace gpos
#endif
