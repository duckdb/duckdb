//---------------------------------------------------------------------------
//	@filename:
//		CColConstraintsHashMapper.cpp
//
//	@doc:
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CColConstraintsHashMapper.h"

#include "duckdb/optimizer/cascade/common/CAutoRef.h"

using namespace gpopt;

CConstraintArray* CColConstraintsHashMapper::PdrgPcnstrLookup(CColRef *colref)
{
	CConstraintArray *pdrgpcnstrCol = m_phmColConstr->Find(colref);
	pdrgpcnstrCol->AddRef();
	return pdrgpcnstrCol;
}

// mapping between columns and single column constraints in array of constraints
static ColRefToConstraintArrayMap* PhmcolconstrSingleColConstr(CMemoryPool *mp, CConstraintArray *drgPcnstr)
{
	CAutoRef<CConstraintArray> arpdrgpcnstr(drgPcnstr);
	ColRefToConstraintArrayMap *phmcolconstr = GPOS_NEW(mp) ColRefToConstraintArrayMap(mp);

	const ULONG length = arpdrgpcnstr->Size();

	for (ULONG ul = 0; ul < length; ul++)
	{
		CConstraint *pcnstrChild = (*arpdrgpcnstr)[ul];
		CColRefSet *pcrs = pcnstrChild->PcrsUsed();

		if (1 == pcrs->Size())
		{
			CColRef *colref = pcrs->PcrFirst();
			CConstraintArray *pcnstrMapped = phmcolconstr->Find(colref);
			if (NULL == pcnstrMapped)
			{
				pcnstrMapped = GPOS_NEW(mp) CConstraintArray(mp);
				phmcolconstr->Insert(colref, pcnstrMapped);
			}
			pcnstrChild->AddRef();
			pcnstrMapped->Append(pcnstrChild);
		}
	}

	return phmcolconstr;
}

CColConstraintsHashMapper::CColConstraintsHashMapper(CMemoryPool *mp, CConstraintArray *pdrgpcnstr)
	: m_phmColConstr(PhmcolconstrSingleColConstr(mp, pdrgpcnstr))
{
}

CColConstraintsHashMapper::~CColConstraintsHashMapper()
{
	m_phmColConstr->Release();
}