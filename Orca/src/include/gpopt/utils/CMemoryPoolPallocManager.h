//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2019 VMware, Inc. or its affiliates.
//
//	@filename:
//		CMemoryPoolPallocManager.h
//
//	@doc:
//		MemoryPoolManager implementation that creates
//		CMemoryPoolPalloc memory pools
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CMemoryPoolPallocManager_H
#define GPDXL_CMemoryPoolPallocManager_H

#include "gpos/base.h"
#include "gpos/memory/CMemoryPoolManager.h"

namespace gpos
{
// memory pool manager that uses GPDB memory contexts
class CMemoryPoolPallocManager : public CMemoryPoolManager
{
private:
public:
	CMemoryPoolPallocManager(const CMemoryPoolPallocManager &) = delete;

	// ctor
	CMemoryPoolPallocManager(CMemoryPool *internal,
							 EMemoryPoolType memory_pool_type);

	// allocate new memorypool
	CMemoryPool *NewMemoryPool() override;

	// free allocation
	void DeleteImpl(void *ptr, CMemoryPool::EAllocationType eat) override;

	// get user requested size of allocation
	ULONG UserSizeOfAlloc(const void *ptr) override;


	static GPOS_RESULT Init();
};
}  // namespace gpos

#endif	// !GPDXL_CMemoryPoolPallocManager_H

// EOF
