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
CKeyCollection::CKeyCollection(CMemoryPool *mp)
    : m_mp(mp), m_pdrgpcrs(NULL)
{
	GPOS_ASSERT(NULL != mp);

	m_pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::CKeyCollection
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CKeyCollection::CKeyCollection(CMemoryPool *mp, CColRefSet *pcrs)
	: m_mp(mp), m_pdrgpcrs(NULL)
{
	GPOS_ASSERT(NULL != pcrs && 0 < pcrs->Size());
	m_pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);
	Add(pcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::CKeyCollection
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CKeyCollection::CKeyCollection(CMemoryPool *mp, CColRefArray *colref_array)
	: m_mp(mp), m_pdrgpcrs(NULL)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != colref_array && 0 < colref_array->Size());

	m_pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(colref_array);
	Add(pcrs);

	// we own the array
	colref_array->Release();
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
	m_pdrgpcrs->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::Add
//
//	@doc:
//		Add key set to collection; takes ownership
//
//---------------------------------------------------------------------------
void
CKeyCollection::Add(CColRefSet *pcrs)
{
	GPOS_ASSERT(!FKey(pcrs) && "no duplicates allowed");

	m_pdrgpcrs->Append(pcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::FKey
//
//	@doc:
//		Check if set constitutes key
//
//---------------------------------------------------------------------------
BOOL CKeyCollection::FKey(const CColRefSet *pcrs, BOOL fExactMatch  // true: match keys exactly,
									   //  false: match keys by inclusion
) const
{
	const ULONG ulSets = m_pdrgpcrs->Size();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		if (fExactMatch)
		{
			// accept only exact matches
			if (pcrs->Equals((*m_pdrgpcrs)[ul]))
			{
				return true;
			}
		}
		else
		{
			// if given column set includes a key, then it is also a key
			if (pcrs->ContainsAll((*m_pdrgpcrs)[ul]))
			{
				return true;
			}
		}
	}

	return false;
}



//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::FKey
//
//	@doc:
//		Check if array constitutes key
//
//---------------------------------------------------------------------------
BOOL CKeyCollection::FKey(CMemoryPool *mp, const CColRefArray *colref_array) const
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(colref_array);

	BOOL fKey = FKey(pcrs);
	pcrs->Release();

	return fKey;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrTrim
//
//	@doc:
//		Return first subsumed key as column array
//
//---------------------------------------------------------------------------
CColRefArray* CKeyCollection::PdrgpcrTrim(CMemoryPool *mp, const CColRefArray *colref_array) const
{
	CColRefArray *pdrgpcrTrim = NULL;
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(colref_array);

	const ULONG ulSets = m_pdrgpcrs->Size();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		CColRefSet *pcrsKey = (*m_pdrgpcrs)[ul];
		if (pcrs->ContainsAll(pcrsKey))
		{
			pdrgpcrTrim = pcrsKey->Pdrgpcr(mp);
			break;
		}
	}
	pcrs->Release();

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
CColRefArray* CKeyCollection::PdrgpcrKey(CMemoryPool *mp) const
{
	if (0 == m_pdrgpcrs->Size())
	{
		return NULL;
	}

	GPOS_ASSERT(NULL != (*m_pdrgpcrs)[0]);

	CColRefArray *colref_array = (*m_pdrgpcrs)[0]->Pdrgpcr(mp);
	return colref_array;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrHashableKey
//
//	@doc:
//		Extract a hashable key
//
//---------------------------------------------------------------------------
CColRefArray* CKeyCollection::PdrgpcrHashableKey(CMemoryPool *mp) const
{
	const ULONG ulSets = m_pdrgpcrs->Size();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		CColRefArray *pdrgpcrKey = (*m_pdrgpcrs)[ul]->Pdrgpcr(mp);
		if (CUtils::IsHashable(pdrgpcrKey))
		{
			return pdrgpcrKey;
		}
		pdrgpcrKey->Release();
	}

	// no hashable key is found
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrKey
//
//	@doc:
//		Extract the key at a position
//
//---------------------------------------------------------------------------
CColRefArray* CKeyCollection::PdrgpcrKey(CMemoryPool *mp, ULONG ulIndex) const
{
	if (0 == m_pdrgpcrs->Size())
	{
		return NULL;
	}

	GPOS_ASSERT(NULL != (*m_pdrgpcrs)[ulIndex]);

	CColRefArray *colref_array = (*m_pdrgpcrs)[ulIndex]->Pdrgpcr(mp);
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
CColRefSet* CKeyCollection::PcrsKey(CMemoryPool *mp, ULONG ulIndex) const
{
	if (0 == m_pdrgpcrs->Size())
	{
		return NULL;
	}

	GPOS_ASSERT(NULL != (*m_pdrgpcrs)[ulIndex]);

	CColRefSet *pcrsKey = (*m_pdrgpcrs)[ulIndex];
	return GPOS_NEW(mp) CColRefSet(mp, *pcrsKey);
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream& CKeyCollection::OsPrint(IOstream &os) const
{
	os << " Keys: (";
	const ULONG ulSets = m_pdrgpcrs->Size();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		if (0 < ul)
		{
			os << ", ";
		}
		GPOS_ASSERT(NULL != (*m_pdrgpcrs)[ul]);
		os << "[" << (*(*m_pdrgpcrs)[ul]) << "]";
	}
	return os << ")";
}