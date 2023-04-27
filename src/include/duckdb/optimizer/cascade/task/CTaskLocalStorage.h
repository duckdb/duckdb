//---------------------------------------------------------------------------
//	@filename:
//		CTaskLocalStorage.h
//
//	@doc:
//		Task-local storage facility; implements TLS to store an instance
//		of a subclass of CTaskLocalStorageObject by an enum index;
//		no restrictions as to where the actual data is allocated,
//		e.g. Task's memory pool, global memory etc.
//---------------------------------------------------------------------------
#ifndef GPOS_CTaskLocalStorage_H
#define GPOS_CTaskLocalStorage_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtable.h"

namespace gpos
{
// fwd declaration
class CTaskLocalStorageObject;

//---------------------------------------------------------------------------
//	@class:
//		CTaskLocalStorage
//
//	@doc:
//		TLS implementation; single instance of this class per task; initialized
//		and destroyed during task setup/tear down
//
//---------------------------------------------------------------------------
class CTaskLocalStorage
{
public:
	enum Etlsidx
	{
		EtlsidxTest,	 // unittest slot
		EtlsidxOptCtxt,	 // optimizer context
		EtlsidxInvalid,	 // used only for hashtable iteration

		EtlsidxSentinel
	};

	// ctor
	CTaskLocalStorage()
	{
	}

	// dtor
	~CTaskLocalStorage();

	// reset
	void Reset(CMemoryPool *mp);

	// accessors
	void Store(CTaskLocalStorageObject *);
	CTaskLocalStorageObject *Get(const Etlsidx);

	// delete object
	void Remove(CTaskLocalStorageObject *);

	// equality function -- used for hashtable
	static BOOL
	Equals(const CTaskLocalStorage::Etlsidx &idx,
		   const CTaskLocalStorage::Etlsidx &idx_other)
	{
		return idx == idx_other;
	}

	// hash function
	static ULONG
	HashIdx(const CTaskLocalStorage::Etlsidx &idx)
	{
		// keys are unique
		return static_cast<ULONG>(idx);
	}

	// invalid Etlsidx
	static const Etlsidx m_invalid_idx;

private:
	// hash table
	CSyncHashtable<CTaskLocalStorageObject, Etlsidx> m_hash_table;

	// private copy ctor
	CTaskLocalStorage(const CTaskLocalStorage &);

};	// class CTaskLocalStorage
}  // namespace gpos

#endif
