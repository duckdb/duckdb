//---------------------------------------------------------------------------
//	@filename:
//		CTaskLocalStorage.cpp
//
//	@doc:
//		Task-local storage implementation
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/task/CTaskLocalStorage.h"
#include "duckdb/optimizer/cascade/task/CTaskLocalStorageObject.h"
#include "duckdb/common/unique_ptr.hpp"
#include <memory>

using namespace gpos;
using namespace std;
using namespace duckdb;

// invalid idx
const Etlsidx CTaskLocalStorage::m_invalid_idx = EtlsidxInvalid;

//---------------------------------------------------------------------------
//	@function:
//		CTaskLocalStorage::~CTaskLocalStorage
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CTaskLocalStorage::~CTaskLocalStorage()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CTaskLocalStorage::Reset
//
//	@doc:
//		(re-)init TLS
//
//---------------------------------------------------------------------------
void CTaskLocalStorage::Reset()
{
	// destroy old
	m_hash_table.clear();
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskLocalStorage::Store
//
//	@doc:
//		Store object in TLS
//
//---------------------------------------------------------------------------
void CTaskLocalStorage::Store(duckdb::unique_ptr<CTaskLocalStorageObject> obj)
{
	m_hash_table.insert(make_pair(obj->m_etlsidx, std::move(obj)));
}

//---------------------------------------------------------------------------
//	@function:
//		CTaskLocalStorage::Get
//
//	@doc:
//		Lookup object in TLS
//
//---------------------------------------------------------------------------
CTaskLocalStorageObject* CTaskLocalStorage::Get(Etlsidx idx)
{
	return m_hash_table.find(idx)->second.get();
}

//---------------------------------------------------------------------------
//	@function:
//		CTaskLocalStorage::Remove
//
//	@doc:
//		Delete given object from TLS
//
//---------------------------------------------------------------------------
void CTaskLocalStorage::Remove(CTaskLocalStorageObject* obj)
{
	// lookup object
	auto itr = m_hash_table.find(obj->m_etlsidx);
	m_hash_table.erase(itr);
}