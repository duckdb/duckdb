//---------------------------------------------------------------------------
//	@filename:
//		CMemoryPool.h
//
//	@doc:
//		Abstraction of memory pool management. Memory pool types are derived
//		from this class as drop-in replacements. This acts as an abstract class,
//		concrete memory pools such as CMemoryPoolTracker and CMemoryPoolPalloc are
//		derived from this.
//		Some things to note:
//		1. When allocating memory, we have the mp pointer that we are allocating into.
//			However, when deleting memory, we no longer have that pointer. How we free
//			this memory is dependent on the memory pool used, which we get from the
//			memory pool manager.
//		2. When deleting an array, we need to iterate through each element of the array.
//			To calculate this, we calculate the length by calling UserSizeOfAlloc(). This
//			is only done for allocations of type EatArray and thus we do not store the
//			allocation length for non-array allocations.
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPool_H
#define GPOS_CMemoryPool_H

#include "duckdb/optimizer/cascade/assert.h"
#include "duckdb/optimizer/cascade/common/CLink.h"
#include "duckdb/optimizer/cascade/common/CStackDescriptor.h"
#include "duckdb/optimizer/cascade/error/CException.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPoolStatistics.h"
#include "duckdb/optimizer/cascade/types.h"

// 8-byte alignment
#define GPOS_MEM_ARCH (8)

#define GPOS_MEM_ALIGNED_SIZE(x)  (GPOS_MEM_ARCH * (((x) / GPOS_MEM_ARCH) + ((x) % GPOS_MEM_ARCH ? 1 : 0)))

// padded size for a given data structure
#define GPOS_MEM_ALIGNED_STRUCT_SIZE(x) GPOS_MEM_ALIGNED_SIZE(GPOS_SIZEOF(x))

// sanity check: char and ulong always fits into the basic unit of alignment
GPOS_CPL_ASSERT(GPOS_MEM_ALIGNED_STRUCT_SIZE(gpos::CHAR) == GPOS_MEM_ARCH);
GPOS_CPL_ASSERT(GPOS_MEM_ALIGNED_STRUCT_SIZE(gpos::ULONG) == GPOS_MEM_ARCH);

// static pattern to init memory
#define GPOS_MEM_INIT_PATTERN_CHAR (0xCC)
// pattern used to mark deallocated memory, this must match
// GPOS_WIPED_MEM_PATTERN defined in CRefCount.h
#define GPOS_MEM_FREED_PATTERN_CHAR (0xCD)

// max allocation per request: 1GB
#define GPOS_MEM_ALLOC_MAX (0x40000000)

#define GPOS_MEM_OFFSET_POS(p, ullOffset) ((void *) ((BYTE *) (p) + ullOffset))

namespace gpos
{
// prototypes
class IMemoryVisitor;
//---------------------------------------------------------------------------
//	@class:
//		CMemoryPool
//
//	@doc:
//		Interface class for memory pool
//---------------------------------------------------------------------------
class CMemoryPool
{
	// manager accesses internal functionality
	friend class CMemoryPoolManager;

private:
	// hash key is only set by pool manager
	ULONG_PTR m_hash_key;

#ifdef GPOS_DEBUG
	// stack where pool is created
	CStackDescriptor m_stack_desc;
#endif	// GPOS_DEBUG

	// link structure to manage pools
	SLink m_link;

protected:
	// invalid memory pool key
	static const ULONG_PTR m_invalid;

public:
	enum EAllocationType
	{
		EatUnknown = 0x00,
		EatSingleton = 0x7f,
		EatArray = 0x7e
	};

	// dtor
	virtual ~CMemoryPool()
	{
	}

	// prepare the memory pool to be deleted
	virtual void TearDown() = 0;

	// hash key accessor
	virtual ULONG_PTR
	GetHashKey() const
	{
		return m_hash_key;
	}

	// implementation of placement new with memory pool
	virtual void *NewImpl(const ULONG bytes, const CHAR *file, const ULONG line,
						  CMemoryPool::EAllocationType eat) = 0;

	// implementation of array-new with memory pool
	template <typename T>
	T *
	NewArrayImpl(SIZE_T num_elements, const CHAR *filename, ULONG line)
	{
		T *array = static_cast<T *>(
			NewImpl(sizeof(T) * num_elements, filename, line, EatArray));
		for (SIZE_T idx = 0; idx < num_elements; ++idx)
		{
			try
			{
				new (array + idx) T();
			}
			catch (...)
			{
				// If any element's constructor throws, deconstruct
				// previous objects and reclaim memory before rethrowing.
				for (SIZE_T destroy_idx = idx - 1; destroy_idx < idx;
					 --destroy_idx)
				{
					array[destroy_idx].~T();
				}
				DeleteImpl(array, EatArray);
				throw;
			}
		}
		return array;
	}

	// return total allocated size
	virtual ULLONG
	TotalAllocatedSize() const
	{
		GPOS_ASSERT(!"not supported");
		return 0;
	}

	// requested size of allocation
	static ULONG UserSizeOfAlloc(const void *ptr);

	// free allocation
	static void DeleteImpl(void *ptr, EAllocationType eat);

#ifdef GPOS_DEBUG

	// check if the memory pool keeps track of live objects
	virtual BOOL
	SupportsLiveObjectWalk() const
	{
		return false;
	}

	// walk the live objects, calling pVisitor.visit() for each one
	virtual void
	WalkLiveObjects(IMemoryVisitor *)
	{
		GPOS_ASSERT(!"not supported");
	}

	// dump memory pool to given stream
	virtual IOstream &OsPrint(IOstream &os);

	// check if a memory pool is empty
	virtual void AssertEmpty(IOstream &os);

#endif	// GPOS_DEBUG

};	// class CMemoryPool
	// Overloading placement variant of singleton new operator. Used to allocate
	// arbitrary objects from an CMemoryPool. This does not affect the ordinary
	// built-in 'new', and is used only when placement-new is invoked with the
	// specific type signature defined below.

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::operator <<
//
//	@doc:
//		Print function for memory pools
//
//---------------------------------------------------------------------------
inline IOstream &
operator<<(IOstream &os, CMemoryPool &mp)
{
	return mp.OsPrint(os);
}
#endif	// GPOS_DEBUG

namespace delete_detail
{
// All-static helper class. Base version deletes unqualified pointers / arrays.
template <typename T>
class CDeleter
{
public:
	static void
	Delete(T *object)
	{
		if (NULL == object)
		{
			return;
		}

		// Invoke destructor, then free memory.
		object->~T();
		CMemoryPool::DeleteImpl(object, CMemoryPool::EatSingleton);
	}

	static void
	DeleteArray(T *object_array)
	{
		if (NULL == object_array)
		{
			return;
		}

		// Invoke destructor on each array element in reverse
		// order from construction.
		const SIZE_T num_elements =
			CMemoryPool::UserSizeOfAlloc(object_array) / sizeof(T);
		for (SIZE_T idx = num_elements - 1; idx < num_elements; --idx)
		{
			object_array[idx].~T();
		}

		// Free memory.
		CMemoryPool::DeleteImpl(object_array, CMemoryPool::EatArray);
	}
};

// Specialization for const-qualified types.
template <typename T>
class CDeleter<const T>
{
public:
	static void
	Delete(const T *object)
	{
		CDeleter<T>::Delete(const_cast<T *>(object));
	}

	static void
	DeleteArray(const T *object_array)
	{
		CDeleter<T>::DeleteArray(const_cast<T *>(object_array));
	}
};
}  // namespace delete_detail
}  // namespace gpos

// Overloading placement variant of singleton new operator. Used to allocate
// arbitrary objects from an CMemoryPool. This does not affect the ordinary
// built-in 'new', and is used only when placement-new is invoked with the
// specific type signature defined below.
inline void *
operator new(gpos::SIZE_T size, gpos::CMemoryPool *mp,
			 const gpos::CHAR *filename, gpos::ULONG line)
{
	return mp->NewImpl(size, filename, line, gpos::CMemoryPool::EatSingleton);
}

// Corresponding placement variant of delete operator. Note that, for delete
// statements in general, the compiler can not determine which overloaded
// version of new was used to allocate memory originally, and the global
// non-placement version is used. This placement version of 'delete' is used
// *only* when a constructor throws an exception, and the version of 'new' is
// known to be the one declared above.
inline void
operator delete(void *ptr, gpos::CMemoryPool *, const gpos::CHAR *, gpos::ULONG)
{
	// Reclaim memory after constructor throws exception.
	gpos::CMemoryPool::DeleteImpl(ptr, gpos::CMemoryPool::EatSingleton);
}

// Placement new-style macro to do 'new' with a memory pool. Anything allocated
// with this *must* be deleted by GPOS_DELETE, *not* the ordinary delete
// operator.
#define GPOS_NEW(mp) new (mp, __FILE__, __LINE__)

// Replacement for array-new. Conceptually equivalent to
// 'new(mp) datatype[count]'. Any arrays allocated with this *must* be deleted
// by GPOS_DELETE_ARRAY, *not* the ordinary delete[] operator.
//
// NOTE: Unlike singleton new, we do not overload the built-in new operator for
// arrays, because when we do so the C++ compiler adds its own book-keeping
// information to the allocation in a non-portable way such that we can not
// recover GPOS' own book-keeping information reliably.
#define GPOS_NEW_ARRAY(mp, datatype, count) \
	mp->NewArrayImpl<datatype>(count, __FILE__, __LINE__)

// Delete a singleton object allocated by GPOS_NEW().
template <typename T>
void
GPOS_DELETE(T *object)
{
	::gpos::delete_detail::CDeleter<T>::Delete(object);
}

// Delete an array allocated by GPOS_NEW_ARRAY().
template <typename T>
void
GPOS_DELETE_ARRAY(T *object_array)
{
	::gpos::delete_detail::CDeleter<T>::DeleteArray(object_array);
}
#endif
