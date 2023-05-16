//---------------------------------------------------------------------------
//	@filename:
//		IColConstraintsMapper.h
//
//	@doc:
//		
//---------------------------------------------------------------------------
#ifndef GPOPT_IColConstraintsMapper_H
#define GPOPT_IColConstraintsMapper_H

#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/base/CConstraint.h"

namespace gpopt
{
class IColConstraintsMapper : public CRefCount
{
public:
	virtual CConstraintArray *PdrgPcnstrLookup(CColRef *colref) = 0;

	virtual ~IColConstraintsMapper() = 0;
};
}  // namespace gpopt

#endif