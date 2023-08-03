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
#include "duckdb/optimizer/cascade/task/CTaskLocalStorageObject.h"
#include <unordered_map>
#include "duckdb/common/unique_ptr.hpp"

using namespace duckdb;
using namespace std;

namespace gpos
{
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
	// hash table
	unordered_map<Etlsidx, duckdb::unique_ptr<CTaskLocalStorageObject>, EtlsidxHash> m_hash_table;

public:
	// invalid Etlsidx
	static const Etlsidx m_invalid_idx;

public:
	// ctor
	CTaskLocalStorage()
	{
	}

	// no copy ctor
	CTaskLocalStorage(const CTaskLocalStorage &) = delete;

	// dtor
	~CTaskLocalStorage();

	// reset
	void Reset();

	// accessors
	void Store(duckdb::unique_ptr<CTaskLocalStorageObject> obj);
	
	CTaskLocalStorageObject* Get(const Etlsidx idx);

	// delete object
	void Remove(CTaskLocalStorageObject* obj);

	// equality function -- used for hashtable
	static bool Equals(const Etlsidx &idx, const Etlsidx &idx_other)
	{
		return idx == idx_other;
	}

	// hash function
	static ULONG HashIdx(const Etlsidx &idx)
	{
		// keys are unique
		return static_cast<ULONG>(idx);
	}
};	// class CTaskLocalStorage
}  // namespace gpos
#endif