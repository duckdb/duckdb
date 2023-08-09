//---------------------------------------------------------------------------
//	@filename:
//		CStack.h
//
//	@doc:
//		Create stack of objects
//
//	@owner:
//		Siva
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CStack_H
#define GPOPT_CStack_H

#include "duckdb/optimizer/cascade/base.h"
#include <algorithm>

using namespace std;

namespace gpos
{
template <class T> class CStack
{
private:
	// backing dynamic array store
	vector<shared_ptr<T>> m_dynamic_ptr_array;

	// top of stack index
	ULONG m_size;

	// copy c'tor - not defined
	CStack(CStack &);

public:
	// ctor
	CStack<T>(ULONG min_size = 4)
		: m_size(0)
	{
	}

	// destructor
	virtual ~CStack()
	{
		m_dynamic_ptr_array.clear();
	}

	// push element onto stack
	void Push(shared_ptr<T> obj)
	{
		// if the stack was Popped before, reuse that space by replacing the element
		if (m_size < m_dynamic_ptr_array.size())
		{
			replace(m_dynamic_ptr_array.begin(), m_dynamic_ptr_array.end(), m_size, obj);
		}
		else
		{
			m_dynamic_ptr_array.emplace_back(obj);
		}
		m_size++;
	}

	// pop top element
	shared_ptr<T> Pop()
	{
		shared_ptr<T> obj = m_dynamic_ptr_array[m_size - 1];
		m_size--;
		return obj;
	}

	// peek at top element
	const shared_ptr<T> Peek() const
	{
		const shared_ptr<T> obj = m_dynamic_ptr_array[m_size - 1];
		return obj;
	}

	// is stack empty?
	BOOL IsEmpty() const
	{
		return (m_size == 0);
	}
};
}  // namespace gpos
#endif