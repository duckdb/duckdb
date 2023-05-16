//---------------------------------------------------------------------------
//	@filename:
//		CColConstraintsArrayMapper.h
//
//	@doc:
//		
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CColConstraintsArrayMapper.h"

#include "duckdb/optimizer/cascade/base/CConstraint.h"

using namespace gpopt;

CConstraintArray* CColConstraintsArrayMapper::PdrgPcnstrLookup(CColRef *colref)
{
	const BOOL fExclusive = true;
	return CConstraint::PdrgpcnstrOnColumn(m_mp, m_pdrgpcnstr, colref, fExclusive);
}

CColConstraintsArrayMapper::CColConstraintsArrayMapper(gpos::CMemoryPool *mp, CConstraintArray *pdrgpcnstr)
	: m_mp(mp), m_pdrgpcnstr(pdrgpcnstr)
{
}

CColConstraintsArrayMapper::~CColConstraintsArrayMapper()
{
	m_pdrgpcnstr->Release();
}