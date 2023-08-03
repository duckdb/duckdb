//---------------------------------------------------------------------------
//	@filename:
//		CTaskLocalStorageObjectObject.h
//
//	@doc:
//		Base class for objects stored in TLS
//---------------------------------------------------------------------------
#ifndef GPOS_CTaskLocalStorageObject_H
#define GPOS_CTaskLocalStorageObject_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CLink.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CTaskLocalStorageObject
//
//	@doc:
//		Abstract TLS object base class; provides hooks for hash table, key, and
//		hash function;
//
//---------------------------------------------------------------------------
class CTaskLocalStorageObject
{
public:
	// ctor
	CTaskLocalStorageObject(Etlsidx etlsidx)
		: m_etlsidx(etlsidx)
	{
	}

	// no copy ctor
	CTaskLocalStorageObject(const CTaskLocalStorageObject &) = delete;

	// dtor
	virtual ~CTaskLocalStorageObject()
	{
	}

	// accessor
	const Etlsidx & idx() const
	{
		return m_etlsidx;
	}

	// link
	SLink m_link;

	// key
	const Etlsidx m_etlsidx;
};	// class CTaskLocalStorageObject
}  // namespace gpos
#endif