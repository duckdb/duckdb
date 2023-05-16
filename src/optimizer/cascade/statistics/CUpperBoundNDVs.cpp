//---------------------------------------------------------------------------
//      @filename:
//              CUpperBoundNDVs.cpp
//
//      @doc:
//              Implementation of upper bound on the number of distinct values for a
//              given set of columns
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CUpperBoundNDVs.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"

using namespace gpnaucrates;
using namespace gpopt;

//---------------------------------------------------------------------------
//      @function:
//              CUpperBoundNDVs::CopyUpperBoundNDVWithRemap
//
//      @doc:
//              Copy upper bound ndvs with remapped column id; function will
//              return null if there is no mapping found for any of the columns
//
//---------------------------------------------------------------------------
CUpperBoundNDVs* CUpperBoundNDVs::CopyUpperBoundNDVWithRemap(CMemoryPool *mp, UlongToColRefMap *colid_to_colref_map) const
{
	BOOL mapping_not_found = false;

	CColRefSet *column_refset_copy = GPOS_NEW(mp) CColRefSet(mp);
	CColRefSetIter column_refset_iter(*m_column_refset);
	while (column_refset_iter.Advance() && !mapping_not_found)
	{
		ULONG colid = column_refset_iter.Pcr()->Id();
		CColRef *column_ref = colid_to_colref_map->Find(&colid);
		if (NULL != column_ref)
		{
			column_refset_copy->Include(column_ref);
		}
		else
		{
			mapping_not_found = true;
		}
	}

	if (0 < column_refset_copy->Size() && !mapping_not_found)
	{
		return GPOS_NEW(mp)
			CUpperBoundNDVs(column_refset_copy, UpperBoundNDVs());
	}

	column_refset_copy->Release();

	return NULL;
}


//---------------------------------------------------------------------------
//      @function:
//              CUpperBoundNDVs::CopyUpperBoundNDVs
//
//      @doc:
//              Copy upper bound ndvs
//
//---------------------------------------------------------------------------
CUpperBoundNDVs *
CUpperBoundNDVs::CopyUpperBoundNDVs(CMemoryPool *mp,
									CDouble upper_bound_ndv) const
{
	m_column_refset->AddRef();
	CUpperBoundNDVs *ndv_copy =
		GPOS_NEW(mp) CUpperBoundNDVs(m_column_refset, upper_bound_ndv);

	return ndv_copy;
}

//---------------------------------------------------------------------------
//      @function:
//              CUpperBoundNDVs::CopyUpperBoundNDVs
//
//      @doc:
//              Copy upper bound ndvs
//
//---------------------------------------------------------------------------
CUpperBoundNDVs *
CUpperBoundNDVs::CopyUpperBoundNDVs(CMemoryPool *mp) const
{
	return CopyUpperBoundNDVs(mp, m_upper_bound_ndv);
}


//---------------------------------------------------------------------------
//      @function:
//              CUpperBoundNDVs::OsPrint
//
//      @doc:
//              Print function
//
//---------------------------------------------------------------------------
IOstream &
CUpperBoundNDVs::OsPrint(IOstream &os) const
{
	os << "{" << std::endl;
	m_column_refset->OsPrint(os);
	os << " Upper Bound of NDVs" << UpperBoundNDVs() << std::endl;
	os << "}" << std::endl;

	return os;
}