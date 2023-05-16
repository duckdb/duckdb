//---------------------------------------------------------------------------
//	@filename:
//		IMDCheckConstraint.h
//
//	@doc:
//		Interface class for check constraint in a metadata cache relation
//---------------------------------------------------------------------------
#ifndef GPMD_IMDCheckConstraint_H
#define GPMD_IMDCheckConstraint_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/md/IMDCacheObject.h"

namespace gpopt
{
class CExpression;
class CMDAccessor;
}  // namespace gpopt

namespace gpmd
{
using namespace gpos;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@class:
//		IMDCheckConstraint
//
//	@doc:
//		Interface class for check constraint in a metadata cache relation
//
//---------------------------------------------------------------------------
class IMDCheckConstraint : public IMDCacheObject
{
public:
	// object type
	virtual Emdtype
	MDType() const
	{
		return EmdtCheckConstraint;
	}

	// mdid of the relation
	virtual IMDId *GetRelMdId() const = 0;

	// the scalar expression of the check constraint
	virtual CExpression *GetCheckConstraintExpr(
		CMemoryPool *mp, CMDAccessor *md_accessor,
		CColRefArray *colref_array) const = 0;
};
}  // namespace gpmd

#endif