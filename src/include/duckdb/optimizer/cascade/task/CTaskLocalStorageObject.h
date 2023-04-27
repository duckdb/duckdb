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
#include "duckdb/optimizer/cascade/task/CTaskLocalStorage.h"

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
private:
	// private copy ctor
	CTaskLocalStorageObject(const CTaskLocalStorageObject &);

public:
	// ctor
	CTaskLocalStorageObject(CTaskLocalStorage::Etlsidx etlsidx)
		: m_etlsidx(etlsidx)
	{
		GPOS_ASSERT(CTaskLocalStorage::EtlsidxSentinel > etlsidx &&
					"TLS index out of range");
	}

	// dtor
	virtual ~CTaskLocalStorageObject()
	{
	}

	// accessor
	const CTaskLocalStorage::Etlsidx &
	idx() const
	{
		return m_etlsidx;
	}

	// link
	SLink m_link;

	// key
	const CTaskLocalStorage::Etlsidx m_etlsidx;

#ifdef GPOS_DEBUG
	// debug print
	virtual IOstream &OsPrint(IOstream &os) const = 0;
#endif	// GPOS_DEBUG

};	// class CTaskLocalStorageObject
}  // namespace gpos

#endif
