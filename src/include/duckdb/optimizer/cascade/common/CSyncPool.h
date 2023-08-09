//---------------------------------------------------------------------------
//	@filename:
//		CSyncPool.h
//
//	@doc:
//		Template-based synchronized object pool class with minimum synchronization
//		overhead; it provides thread-safe object retrieval and release through
//		atomic primitives (lock-free);
//
//		Object pool is dynamically created during construction and released at
//		destruction; users retrieve objects without incurring the construction
//		cost (memory allocation, constructor invocation)
//
//		In order for the objects to be used in lock-free lists, the class uses
//		the clock algorithm to recycle objects.
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncPool_H
#define GPOS_CSyncPool_H

#include "duckdb/optimizer/cascade/types.h"
#include "duckdb/optimizer/cascade/utils.h"

#define BYTES_PER_ULONG (GPOS_SIZEOF(ULONG))
#define BITS_PER_ULONG (BYTES_PER_ULONG * 8)

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CSyncPool<class T>
//
//	@doc:
//		Object pool class (not thread-safe, despite the name)
//
//---------------------------------------------------------------------------
template <class T> class CSyncPool
{
public:
	// array of preallocated objects
	T* m_objects;

	// bitmap indicating object reservation
	ULONG* m_objs_reserved;

	// bitmap indicating object recycle
	ULONG* m_objs_recycled;

	// number of allocated objects
	ULONG m_numobjs;

	// number of elements (ULONG) in bitmap
	ULONG m_bitmap_size;

	// offset of last lookup - clock index
	ULONG_PTR m_last_lookup_idx;

	// offset of id inside the object
	ULONG m_id_offset;

public:
	// ctor
	CSyncPool(ULONG size)
		: m_objects(NULL), m_objs_reserved(NULL), m_objs_recycled(NULL), m_numobjs(size), m_bitmap_size(size / BITS_PER_ULONG + 1), m_last_lookup_idx(0), m_id_offset(gpos::ulong_max)
	{
	}

	// no copy ctor
	CSyncPool(const CSyncPool &) = delete;

	// dtor
	~CSyncPool()
	{
		if (gpos::ulong_max != m_id_offset)
		{
			delete[] m_objects;
			delete[] m_objs_reserved;
			delete[] m_objs_recycled;
		}
	}

public:
	// atomically set bit if it is unset
	bool SetBit(ULONG* dest, ULONG bit_val)
	{
		ULONG old_val = *dest;
		// keep trying while the bit is unset
		while (0 == (bit_val & old_val))
		{
			ULONG new_val = bit_val | old_val;
			// attempt to set the bit
			if (*dest == old_val)
			{
				*dest = new_val;
				return true;
			}
			old_val = *dest;
		}
		return false;
	}

	// atomically unset bit if it is set
	bool UnsetBit(ULONG* dest, ULONG bit_val)
	{
		ULONG old_val = *dest;
		// keep trying while the bit is set
		while (bit_val == (bit_val & old_val))
		{
			ULONG new_val = bit_val ^ old_val;
			// attempt to set the bit
			if (*dest == old_val)
			{
				*dest = new_val;
				return true;
			}
			old_val = *dest;
		}
		return false;
	}

	// init function to facilitate arrays
	void Init(ULONG id_offset)
	{
		m_objects = new T[m_numobjs];
		m_objs_reserved = new ULONG[m_bitmap_size];
		m_objs_recycled = new ULONG[m_bitmap_size];
		m_id_offset = id_offset;
		// initialize object ids
		for (ULONG i = 0; i < m_numobjs; i++)
		{
			ULONG *id = (ULONG *) (((BYTE *) &m_objects[i]) + m_id_offset);
			*id = i;
		}
		// initialize bitmaps
		for (ULONG i = 0; i < m_bitmap_size; i++)
		{
			m_objs_reserved[i] = 0;
			m_objs_recycled[i] = 0;
		}
	}

	// find unreserved object and reserve it
	T* PtRetrieve()
	{
		// iterate over all objects twice (two full clock rotations);
		// objects marked as recycled cannot be reserved on the first round;
		for (ULONG i = 0; i < 2 * m_numobjs; i++)
		{
			// move clock index
			ULONG_PTR index = (m_last_lookup_idx++) % m_numobjs;
			ULONG elem_offset = (ULONG) index / BITS_PER_ULONG;
			ULONG bit_offset = (ULONG) index % BITS_PER_ULONG;
			ULONG bit_val = 1 << bit_offset;
			// attempt to reserve object
			if (SetBit(&m_objs_reserved[elem_offset], bit_val))
			{
				// set id in corresponding object
				T* elem = &m_objects[index];
				return elem;
			}
			// object is reserved, check if it has been marked for recycling
			if (bit_val == (bit_val & m_objs_recycled[elem_offset]))
			{
				// attempt to unset the recycle bit
				if (UnsetBit(&m_objs_recycled[elem_offset], bit_val))
				{
					// unset the reserve bit - must succeed
					UnsetBit(&m_objs_reserved[elem_offset], bit_val);
				}
			}
		}
		// no object is currently available, create a new one
		T* elem = new T();
		*(ULONG*) (((BYTE*)elem) + m_id_offset) = gpos::ulong_max;
		return elem;
	}

	// recycle reserved object
	void Recycle(T* elem)
	{
		ULONG offset = *(ULONG*) (((BYTE*)elem) + m_id_offset);
		if (gpos::ulong_max == offset)
		{
			// object does not belong to the array, delete it
			delete elem;
			return;
		}
		ULONG elem_offset = offset / BITS_PER_ULONG;
		ULONG bit_offset = offset % BITS_PER_ULONG;
		ULONG bit_val = 1 << bit_offset;
		SetBit(&m_objs_recycled[elem_offset], bit_val);
	}
};	// class CSyncPool
}  // namespace gpos
#endif