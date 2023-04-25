//---------------------------------------------------------------------------
//	@filename:
//		IMDPartConstraint.h
//
//	@doc:
//		Interface class for partition constraints in the MD cache
//---------------------------------------------------------------------------
#ifndef GPMD_IMDPartConstraint_H
#define GPMD_IMDPartConstraint_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/md/IMDInterface.h"

// fwd decl
namespace gpopt
{
class CExpression;
class CMDAccessor;
}  // namespace gpopt

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDPartConstraint
//
//	@doc:
//		Interface class for partition constraints in the MD cache
//
//---------------------------------------------------------------------------
class IMDPartConstraint : public IMDInterface
{
public:
	// extract the scalar expression of the constraint with the given
	// column mappings
	virtual CExpression *GetPartConstraintExpr(
		CMemoryPool *mp, CMDAccessor *md_accessor,
		CColRefArray *colref_array) const = 0;

	// included default partitions
	virtual ULongPtrArray *GetDefaultPartsArray() const = 0;

	// is constraint unbounded
	virtual BOOL IsConstraintUnbounded() const = 0;

	// serialize constraint in DXL format
	virtual void Serialize(CXMLSerializer *xml_serializer) const = 0;
};
}  // namespace gpmd

#endif
