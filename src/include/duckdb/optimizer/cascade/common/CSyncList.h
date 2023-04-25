//---------------------------------------------------------------------------
//	@filename:
//		CSyncList.h
//
//	@doc:
//		Template-based synchronized list class - no longer thread-safe
//
//		It requires that the elements are only inserted once, as their address
//		is used for identification;
//
//		In order to be useful for system programming the class must be
//		allocation-less, i.e. manage elements without additional allocation,
//		to work in exception or OOM situations;
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncList_H
#define GPOS_CSyncList_H

#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/types.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CSyncList<class T>
//
//	@doc:
//		Allocation-less list class; requires T to have a pointer to T embedded
//		and N being the offset of this member;
//
//---------------------------------------------------------------------------
template <class T>
class CSyncList
{
private:
	// underlying list
	CList<T> m_list;

	// no copy ctor
	CSyncList(const CSyncList &);

public:
	// ctor
	CSyncList()
	{
	}

	// dtor
	~CSyncList()
	{
	}

	// init function to facilitate arrays
	void
	Init(ULONG offset)
	{
		m_list.Init(offset);
	}

	// insert element at the head of the list;
	void
	Push(T *elem)
	{
		GPOS_ASSERT(NULL != elem);
		GPOS_ASSERT(m_list.First() != elem);

		SLink &link = m_list.Link(elem);

#ifdef GPOS_DEBUG
		void *next_head = link.m_next;
#endif	// GPOS_DEBUG

		GPOS_ASSERT(NULL == link.m_next);

		T *head = m_list.First();

		GPOS_ASSERT(elem != head && "Element is already inserted");
		GPOS_ASSERT(next_head == link.m_next &&
					"Element is concurrently accessed");

		// set current head as next element
		link.m_next = head;
#ifdef GPOS_DEBUG
		next_head = link.m_next;
#endif	// GPOS_DEBUG

		// set element as head
		GPOS_ASSERT(m_list.m_head == head);
		m_list.m_head = elem;
	}

	// remove element from the head of the list;
	T *
	Pop()
	{
		T *old_head = NULL;

		// get current head
		old_head = m_list.First();
		if (NULL != old_head)
		{
			// second element becomes the new head
			SLink &link = m_list.Link(old_head);
			T *new_head = static_cast<T *>(link.m_next);

			GPOS_ASSERT(m_list.m_head == old_head);
			m_list.m_head = new_head;
			// reset link
			link.m_next = NULL;
		}

		return old_head;
	}

	// get first element
	T *
	PtFirst()
	{
		m_list.m_tail = m_list.m_head;
		return m_list.First();
	}

	// get next element
	T *
	Next(T *elem)
	{
		m_list.m_tail = m_list.m_head;
		return m_list.Next(elem);
	}

	// check if list is empty
	BOOL
	IsEmpty() const
	{
		return NULL == m_list.First();
	}

#ifdef GPOS_DEBUG

	// lookup a given element in the stack
	// this works only when no elements are removed
	GPOS_RESULT
	Find(T *elem)
	{
		m_list.m_tail = m_list.m_head;
		return m_list.Find(elem);
	}

	// debug print of element addresses
	// this works only when no elements are removed
	IOstream &
	OsPrint(IOstream &os) const
	{
		return m_list.OsPrint(os);
	}

#endif	// GPOS_DEBUG

};	// class CSyncList
}  // namespace gpos

#endif
