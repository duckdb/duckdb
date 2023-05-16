//---------------------------------------------------------------------------
//	@filename:
//		CAutoRef.h
//
//	@doc:
//		Basic auto pointer for ref-counted objects
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoRef_H
#define GPOS_CAutoRef_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CAutoP.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CAutoRef
//
//	@doc:
//		Wrapps pointer of type T which is a subtype of CRefCount
//
//---------------------------------------------------------------------------
template <class T>
class CAutoRef : public CAutoP<T>
{
private:
	// hidden copy ctor
	CAutoRef<T>(const CAutoRef &);

public:
	// ctor
	explicit CAutoRef<T>() : CAutoP<T>()
	{
	}

	// ctor
	explicit CAutoRef<T>(T *object) : CAutoP<T>(object)
	{
	}

	virtual ~CAutoRef();

	// simple assignment
	CAutoRef<T> const &
	operator=(T *object)
	{
		CAutoP<T>::m_object = object;
		return *this;
	}

};	// class CAutoRef

//---------------------------------------------------------------------------
//	@function:
//		CAutoRef::~CAutoRef
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
template <class T>
CAutoRef<T>::~CAutoRef()
{
	if (NULL != CAutoP<T>::m_object)
	{
		reinterpret_cast<CRefCount *>(CAutoP<T>::m_object)->Release();
	}

	// null out pointer before ~CAutoP() gets called
	CAutoP<T>::m_object = NULL;
}
}  // namespace gpos

#endif