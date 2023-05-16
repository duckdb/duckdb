//---------------------------------------------------------------------------
//	@filename:
//		CHashedDistributions.h
//
//	@doc:
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CHashedDistributions_H
#define GPOPT_CHashedDistributions_H

#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"
#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpec.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecHashed.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"

namespace gpopt
{
// Build hashed distributions used in physical union all during
// distribution derivation. The class is an array of hashed
// distribution on input column of each child, and an output hashed
// distribution on UnionAll output columns

class CHashedDistributions : public CDistributionSpecArray
{
public:
	CHashedDistributions(CMemoryPool *mp, CColRefArray *pdrgpcrOutput, CColRef2dArray *pdrgpdrgpcrInput);
};
}  // namespace gpopt

#endif	//GPOPT_CHashedDistributions_H
