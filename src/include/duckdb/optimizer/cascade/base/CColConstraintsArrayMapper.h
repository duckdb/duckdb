//---------------------------------------------------------------------------
//	@filename:
//		CColConstraintsArrayMapper.h
//
//	@doc:
//		
//---------------------------------------------------------------------------
#ifndef GPOPT_CColConstraintsArrayMapper_H
#define GPOPT_CColConstraintsArrayMapper_H

#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"

#include "duckdb/optimizer/cascade/base/CConstraint.h"
#include "duckdb/optimizer/cascade/base/IColConstraintsMapper.h"

namespace gpopt
{
class CColConstraintsArrayMapper : public IColConstraintsMapper
{
public:
	CColConstraintsArrayMapper(gpos::CMemoryPool *mp,
							   CConstraintArray *pdrgpcnstr);
	virtual CConstraintArray *PdrgPcnstrLookup(CColRef *colref);

	virtual ~CColConstraintsArrayMapper();

private:
	gpos::CMemoryPool *m_mp;
	CConstraintArray *m_pdrgpcnstr;
};
}  // namespace gpopt

#endif	//GPOPT_CColConstraintsArrayMapper_H
