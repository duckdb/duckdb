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

#include "duckdb/optimizer/cascade/common/CAutoP.h"
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
template <class T>
class CSyncPool
{
private:
	// memory pool
	CMemoryPool *m_mp;

	// array of preallocated objects
	T *m_objects;

	// bitmap indicating object reservation
	ULONG *m_objs_reserved;

	// bitmap indicating object recycle
	ULONG *m_objs_recycled;

	// number of allocated objects
	ULONG m_numobjs;

	// number of elements (ULONG) in bitmap
	ULONG m_bitmap_size;

	// offset of last lookup - clock index
	ULONG_PTR m_last_lookup_idx;

	// offset of id inside the object
	ULONG m_id_offset;

	// atomically set bit if it is unset
	BOOL
	SetBit(ULONG *dest, ULONG bit_val)
	{
		GPOS_ASSERT(NULL != dest);

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
	BOOL
	UnsetBit(ULONG *dest, ULONG bit_val)
	{
		GPOS_ASSERT(NULL != dest);

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

	// no copy ctor
	CSyncPool(const CSyncPool &);

public:
	// ctor
	CSyncPool(CMemoryPool *mp, ULONG size)
		: m_mp(mp),
		  m_objects(NULL),
		  m_objs_reserved(NULL),
		  m_objs_recycled(NULL),
		  m_numobjs(size),
		  m_bitmap_size(size / BITS_PER_ULONG + 1),
		  m_last_lookup_idx(0),
		  m_id_offset(gpos::ulong_max)
	{
	}

	// dtor
	~CSyncPool()
	{
		if (gpos::ulong_max != m_id_offset)
		{
			GPOS_ASSERT(NULL != m_objects);
			GPOS_ASSERT(NULL != m_objs_reserved);
			GPOS_ASSERT(NULL != m_objs_recycled);

#ifdef GPOS_DEBUG
			if (!ITask::Self()->HasPendingExceptions())
			{
				for (ULONG i = 0; i < m_numobjs; i++)
				{
					ULONG elem_offset = i / BITS_PER_ULONG;
					ULONG bit_offset = i % BITS_PER_ULONG;
					ULONG bit_val = 1 << bit_offset;

					BOOL reserved =
						(bit_val == (m_objs_reserved[elem_offset] & bit_val));
					BOOL recycled =
						(bit_val == (m_objs_recycled[elem_offset] & bit_val));

					GPOS_ASSERT((!reserved || recycled) &&
								"Object is still in use");
				}
			}
#endif	// GPOS_DEBUG

			GPOS_DELETE_ARRAY(m_objects);
			GPOS_DELETE_ARRAY(m_objs_reserved);
			GPOS_DELETE_ARRAY(m_objs_recycled);
		}
	}

	// init function to facilitate arrays
	void
	Init(ULONG id_offset)
	{
		GPOS_ASSERT(ALIGNED_32(id_offset));

		m_objects = GPOS_NEW_ARRAY(m_mp, T, m_numobjs);
		m_objs_reserved = GPOS_NEW_ARRAY(m_mp, ULONG, m_bitmap_size);
		m_objs_recycled = GPOS_NEW_ARRAY(m_mp, ULONG, m_bitmap_size);

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
	T *
	PtRetrieve()
	{
		GPOS_ASSERT(gpos::ulong_max != m_id_offset &&
					"Id offset not initialized.");

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
				T *elem = &m_objects[index];

#ifdef GPOS_DEBUG
				ULONG *id = (ULONG *) (((BYTE *) elem) + m_id_offset);
				GPOS_ASSERT(index == *id);
#endif	// GPOS_DEBUG

				return elem;
			}

			// object is reserved, check if it has been marked for recycling
			if (bit_val == (bit_val & m_objs_recycled[elem_offset]))
			{
				// attempt to unset the recycle bit
				if (UnsetBit(&m_objs_recycled[elem_offset], bit_val))
				{
#ifdef GPOS_DEBUG
					BOOL recycled =
#endif	// GPOS_DEBUG

						// unset the reserve bit - must succeed
						UnsetBit(&m_objs_reserved[elem_offset], bit_val);

					GPOS_ASSERT(recycled &&
								"Object was reserved before being recycled");
				}
			}
		}

		// no object is currently available, create a new one
		T *elem = GPOS_NEW(m_mp) T();
		*(ULONG *) (((BYTE *) elem) + m_id_offset) = gpos::ulong_max;

		return elem;
	}

	// recycle reserved object
	void
	Recycle(T *elem)
	{
		GPOS_ASSERT(gpos::ulong_max != m_id_offset &&
					"Id offset not initialized.");

		ULONG offset = *(ULONG *) (((BYTE *) elem) + m_id_offset);
		if (gpos::ulong_max == offset)
		{
			// object does not belong to the array, delete it
			GPOS_DELETE(elem);
			return;
		}

		GPOS_ASSERT(offset < m_numobjs);

		ULONG elem_offset = offset / BITS_PER_ULONG;
		ULONG bit_offset = offset % BITS_PER_ULONG;
		ULONG bit_val = 1 << bit_offset;

#ifdef GPOS_DEBUG
		ULONG reserved = m_objs_reserved[elem_offset];
		GPOS_ASSERT((bit_val == (bit_val & reserved)) &&
					"Object is not reserved");

		BOOL mark_recycled =
#endif	// GPOS_DEBUG
			SetBit(&m_objs_recycled[elem_offset], bit_val);

		GPOS_ASSERT(mark_recycled &&
					"Object has already been marked for recycling");
	}

};	// class CSyncPool
}  // namespace gpos

#endif