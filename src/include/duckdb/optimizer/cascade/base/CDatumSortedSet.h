//---------------------------------------------------------------------------
//	@filename:
//		CConstraintInterval.cpp
//
//	@doc:
//		Implementation of interval constraints
//---------------------------------------------------------------------------
#ifndef GPOPT_CDatumSortedSet_H
#define GPOPT_CDatumSortedSet_H

#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"

#include "duckdb/optimizer/cascade/base/IComparator.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/base/IDatum.h"

namespace gpopt
{
// A sorted and uniq'd array of pointers to datums
// It facilitates the construction of CConstraintInterval
class CDatumSortedSet : public IDatumArray
{
private:
	BOOL m_fIncludesNull;

public:
	CDatumSortedSet(CMemoryPool *mp, CExpression *pexprArray,
					const IComparator *pcomp);

	BOOL FIncludesNull() const;
};
}  // namespace gpopt

#endif