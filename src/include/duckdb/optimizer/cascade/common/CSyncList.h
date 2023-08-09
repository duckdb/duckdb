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
public:
	// underlying list
	CList<T> m_list;

public:
	// ctor
	CSyncList()
	{
	}

	// no copy ctor
	CSyncList(const CSyncList &) = delete;

	// dtor
	~CSyncList()
	{
	}

	// init function to facilitate arrays
	void Init(ULONG offset)
	{
		m_list.Init(offset);
	}

	// insert element at the head of the list;
	void Push(T* elem)
	{
		SLink &link = m_list.Link(elem);
		T* head = m_list.First();
		// set current head as next element
		link.m_next = head;
		m_list.m_head = elem;
	}

	// remove element from the head of the list;
	T* Pop()
	{
		T* old_head;
		// get current head
		old_head = m_list.First();
		if (nullptr != old_head)
		{
			// second element becomes the new head
			SLink &link = m_list.Link(old_head);
			T* new_head = static_cast<T*>(link.m_next);
			m_list.m_head = new_head;
			// reset link
			link.m_next = NULL;
		}
		return old_head;
	}

	// get first element
	T* PtFirst()
	{
		m_list.m_tail = m_list.m_head;
		return m_list.First();
	}

	// get next element
	T* Next(T* elem)
	{
		m_list.m_tail = m_list.m_head;
		return m_list.Next(elem);
	}

	// check if list is empty
	bool IsEmpty() const
	{
		return NULL == m_list.First();
	}
};	// class CSyncList
}  // namespace gpos
#endif