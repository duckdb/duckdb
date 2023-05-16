//---------------------------------------------------------------------------
//	@filename:
//		CColConstraintsHashMapper.cpp
//
//	@doc:
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CColConstraintsHashMapper_H
#define GPOPT_CColConstraintsHashMapper_H

#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"

#include "duckdb/optimizer/cascade/base/CConstraint.h"
#include "duckdb/optimizer/cascade/base/IColConstraintsMapper.h"

namespace gpopt
{
class CColConstraintsHashMapper : public IColConstraintsMapper
{
public:
	CColConstraintsHashMapper(CMemoryPool *mp, CConstraintArray *pdrgPcnstr);

	virtual CConstraintArray *PdrgPcnstrLookup(CColRef *colref);
	virtual ~CColConstraintsHashMapper();

private:
	ColRefToConstraintArrayMap *m_phmColConstr;
};
}  // namespace gpopt

#endif