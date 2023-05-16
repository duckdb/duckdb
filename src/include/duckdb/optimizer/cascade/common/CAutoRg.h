//---------------------------------------------------------------------------
//	@filename:
//		CAutoRg.h
//
//	@doc:
//		Basic auto range implementation; do not anticipate ownership based
//		on assignment to other auto ranges etc. Require explicit return/assignment
//		to re-init the object;
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoRg_H
#define GPOS_CAutoRg_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CStackObject.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CAutoRg
//
//	@doc:
//		Wrapper around arrays analogous to CAutoP
//
//---------------------------------------------------------------------------
template <class T>
class CAutoRg : public CStackObject
{
private:
	// actual element to point to
	T *m_object_array;

	// hidden copy ctor
	CAutoRg<T>(const CAutoRg &);

public:
	// ctor
	explicit CAutoRg<T>() : m_object_array(NULL)
	{
	}

	// ctor
	explicit CAutoRg<T>(T *object_array) : m_object_array(object_array)
	{
	}


	// dtor
	virtual ~CAutoRg();

	// simple assignment
	inline CAutoRg<T> const &
	operator=(T *object_array)
	{
		m_object_array = object_array;
		return *this;
	}

	// indexed access
	inline T &
	operator[](ULONG ulPos)
	{
		return m_object_array[ulPos];
	}

	// return basic pointer
	T *
	Rgt()
	{
		return m_object_array;
	}

	// unhook pointer from auto object
	inline T *
	RgtReset()
	{
		T *object_array = m_object_array;
		m_object_array = NULL;
		return object_array;
	}

};	// class CAutoRg


//---------------------------------------------------------------------------
//	@function:
//		CAutoRg::~CAutoRg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
template <class T>
CAutoRg<T>::~CAutoRg()
{
	GPOS_DELETE_ARRAY(m_object_array);
}
}  // namespace gpos

#endif
