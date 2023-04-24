
//---------------------------------------------------------------------------
//	@filename:
//		CList.h
//
//	@doc:
//		Template-based list class;
//		In order to be useful for system programming the class must be
//		allocation-less, i.e. manage elements without additional allocation,
//		to work in exception or OOM situations
//---------------------------------------------------------------------------
#ifndef GPOS_CList_H
#define GPOS_CList_H

#include "duckdb/optimizer/cascade/common/CLink.h"
#include "duckdb/optimizer/cascade/task/ITask.h"
#include "duckdb/optimizer/cascade/types.h"
#include "duckdb/optimizer/cascade/utils.h"

namespace gpos
{
// forward declaration
template <class T>
class CSyncList;

//---------------------------------------------------------------------------
//	@class:
//		CList<class T>
//
//	@doc:
//		Allocation-less list class; requires T to have SLink embedded and
//		N being the offset of this member;
//
//		Unfortunately not all compilers support use of an offset macro as
//		template parameter; hence, we have to provide offset as parameter to
//		constructor
//
//---------------------------------------------------------------------------
template <class T>
class CList
{
	// friends
	friend class CSyncList<T>;

private:
	// offest of link element
	ULONG m_offset;

	// size
	ULONG m_size;

	// head element
	T *m_head;

	// tail element
	T *m_tail;

	// no copy ctor
	CList(const CList &);

	// extract link from element
	SLink &
	Link(const void *pv) const
	{
		GPOS_ASSERT(MAX_ALIGNED(pv));
		GPOS_ASSERT(MAX_ALIGNED(m_offset));

		GPOS_ASSERT(gpos::ulong_max != m_offset &&
					"Link offset not initialized.");
		SLink &link = *(SLink *) (((BYTE *) pv) + m_offset);
		return link;
	}

public:
	// ctor
	CList() : m_offset(gpos::ulong_max), m_size(0), m_head(NULL), m_tail(NULL)
	{
	}

	// init function to facilitate arrays
	void
	Init(ULONG offset)
	{
		GPOS_ASSERT(MAX_ALIGNED(offset));
		m_offset = offset;
	}

	// Insert by prepending/appending to current list
	void
	Prepend(T *elem)
	{
		GPOS_ASSERT(NULL != elem);

		// inserting first element?
		if (NULL == m_head)
		{
			GPOS_ASSERT(NULL == m_tail);
			GPOS_ASSERT(0 == m_size);
			SLink &link = Link(elem);

			link.m_next = NULL;
			link.m_prev = NULL;

			m_head = elem;
			m_tail = elem;

			m_size++;
		}
		else
		{
			Prepend(elem, m_head);
		}

		GPOS_ASSERT(0 != m_size);
	}

	void
	Append(T *elem)
	{
		GPOS_ASSERT(NULL != elem);

		if (NULL == m_tail)
		{
			Prepend(elem);
		}
		else
		{
			Append(elem, m_tail);
		}

		GPOS_ASSERT(0 != m_size);
	}

	// Insert by prepending/appending relative to given position
	void
	Prepend(T *elem, T *next_elem)
	{
		GPOS_ASSERT(NULL != elem);
		GPOS_ASSERT(NULL != next_elem);
		T *prev = static_cast<T *>(Link(next_elem).m_prev);

		SLink &link = Link(elem);
		link.m_next = next_elem;
		link.m_prev = prev;

		Link(next_elem).m_prev = elem;
		if (NULL != prev)
		{
			// inserted not at head, ie valid prev element
			Link(prev).m_next = elem;
		}
		else
		{
			// prepended to head
			m_head = elem;
		}

		m_size++;
	}

	void
	Append(T *elem, T *prev_elem)
	{
		GPOS_ASSERT(NULL != elem);
		GPOS_ASSERT(NULL != prev_elem);
		T *next = static_cast<T *>(Link(prev_elem).m_next);

		SLink &sl = Link(elem);
		sl.m_prev = prev_elem;
		sl.m_next = next;

		Link(prev_elem).m_next = elem;
		if (NULL != next)
		{
			// inserted not at tail, ie valid next element
			Link(next).m_prev = elem;
		}
		else
		{
			// appended to tail
			m_tail = elem;
		}

		m_size++;
	}

	// remove element by navigating and manipulating its SLink member
	void
	Remove(T *elem)
	{
		GPOS_ASSERT(NULL != elem);
		GPOS_ASSERT(NULL != m_head);
		GPOS_ASSERT(NULL != m_tail);
		GPOS_ASSERT(0 != m_size);

		SLink &link = Link(elem);

		if (link.m_next)
		{
			Link(link.m_next).m_prev = link.m_prev;
		}
		else
		{
			// removing tail element
			GPOS_ASSERT(m_tail == elem);
			m_tail = static_cast<T *>(link.m_prev);
		}

		if (link.m_prev)
		{
			Link(link.m_prev).m_next = link.m_next;
		}
		else
		{
			// removing head element
			GPOS_ASSERT(m_head == elem);
			m_head = static_cast<T *>(link.m_next);
		}

		// unlink element
		link.m_prev = NULL;
		link.m_next = NULL;

		m_size--;
	}

	// remove and return first element
	T *
	RemoveHead()
	{
		T *head = m_head;
		Remove(m_head);
		return head;
	}

	// remove and return last element
	T *
	RemoveTail()
	{
		T *tail = m_tail;
		Remove(m_tail);
		return tail;
	}

	// accessor to head of list
	T *
	First() const
	{
		return m_head;
	}

	// accessor to tail of list
	T *
	Last() const
	{
		return m_tail;
	}

	// traversal functions Next and PtPrev assume
	// that the pointer passed is a valid member of the list
	T *
	Next(const T *pt) const
	{
		GPOS_ASSERT(NULL != pt);
		GPOS_ASSERT(NULL != m_head);
		GPOS_ASSERT(NULL != m_tail);

		SLink &sl = Link(pt);
		return static_cast<T *>(sl.m_next);
	}

	// and backward...
	T *
	Prev(const T *pt) const
	{
		GPOS_ASSERT(NULL != pt);
		GPOS_ASSERT(NULL != m_head);
		GPOS_ASSERT(NULL != m_tail);

		SLink &sl = Link(pt);
		return static_cast<T *>(sl.m_prev);
	}

	// get size
	ULONG
	Size() const
	{
		return m_size;
	}

	// check if empty
	BOOL
	IsEmpty() const
	{
		return NULL == First();
	}

#ifdef GPOS_DEBUG
	// lookup a given element in the list
	GPOS_RESULT
	Find(T *elem) const
	{
		GPOS_ASSERT(NULL != elem);

		// iterate until found
		T *t = First();
		while (t)
		{
			if (elem == t)
			{
				return GPOS_OK;
			}
			t = Next(t);
		}
		return GPOS_NOT_FOUND;
	}

	// debug print of element addresses
	IOstream &
	OsPrint(IOstream &os) const
	{
		ULONG c = 0;

		T *t = First();

		do
		{
			os << "[" << c++ << "]" << (void *) t << (NULL == t ? "" : " -> ");
		} while (t && (t = Next(t)));
		os << std::endl;

		return os;
	}
#endif	// GPOS_DEBUG

};	// class CList<T>
}  // namespace gpos

#endif
