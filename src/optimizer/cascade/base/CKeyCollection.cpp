//---------------------------------------------------------------------------
//	@filename:
//		CKeyCollection.cpp
//
//	@doc:
//		Implementation of key collections
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::CKeyCollection
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CKeyCollection::CKeyCollection()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::CKeyCollection
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CKeyCollection::CKeyCollection(duckdb::vector<ColumnBinding> pcrs)
{
	m_pdrgpcrs.push_back(pcrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::~CKeyCollection
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CKeyCollection::~CKeyCollection()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::FKey
//
//	@doc:
//		Check if set constitutes key
//
//---------------------------------------------------------------------------
bool CKeyCollection::FKey(const duckdb::vector<ColumnBinding> pcrs, bool fExactMatch) const
{
	const ULONG ulSets = m_pdrgpcrs.size();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		if (fExactMatch)
		{
			// accept only exact matches
			if (CUtils::Equals(pcrs, m_pdrgpcrs[ul]))
			{
				return true;
			}
		}
		else
		{
			return CUtils::ContainsAll(pcrs, m_pdrgpcrs[ul]);
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrTrim
//
//	@doc:
//		Return first subsumed key as column array
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> CKeyCollection::PdrgpcrTrim(const duckdb::vector<ColumnBinding> colref_array) const
{
	duckdb::vector<ColumnBinding> pdrgpcrTrim;
	duckdb::vector<ColumnBinding> pcrs;
	pcrs = colref_array;
	ULONG ulSets = m_pdrgpcrs.size();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		duckdb::vector<ColumnBinding> pcrsKey = m_pdrgpcrs[ul];
		if (CUtils::ContainsAll(pcrs, pcrsKey))
		{
			pdrgpcrTrim = pcrsKey;
			break;
		}
	}
	return pdrgpcrTrim;
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrKey
//
//	@doc:
//		Extract a key
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> CKeyCollection::PdrgpcrKey() const
{
	duckdb::vector<ColumnBinding> v;
	if (0 == m_pdrgpcrs.size())
	{
		return v;
	}
	v = m_pdrgpcrs[0];
	return v;
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrHashableKey
//
//	@doc:
//		Extract a hashable key
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> CKeyCollection::PdrgpcrHashableKey() const
{
	duckdb::vector<ColumnBinding> pdrgpcrKey = m_pdrgpcrs[0];
	return pdrgpcrKey;
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrKey
//
//	@doc:
//		Extract the key at a position
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> CKeyCollection::PdrgpcrKey(ULONG ulIndex) const
{
	duckdb::vector<ColumnBinding> colref_array;
	if (0 == m_pdrgpcrs.size())
	{
		return colref_array;
	}
	colref_array = m_pdrgpcrs[ulIndex];
	return colref_array;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PcrsKey
//
//	@doc:
//		Extract key at given position
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> CKeyCollection::PcrsKey(ULONG ulIndex) const
{
	duckdb::vector<ColumnBinding> v;
	if (0 == m_pdrgpcrs.size())
	{
		return v;
	}
	duckdb::vector<ColumnBinding> pcrsKey = m_pdrgpcrs[ulIndex];
	v.insert(v.end(), pcrsKey.begin(), pcrsKey.end());
	return v;
}