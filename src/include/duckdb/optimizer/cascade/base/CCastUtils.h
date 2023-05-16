//---------------------------------------------------------------------------
//	@filename:
//		CCastUtils.h
//
//	@doc:
//		Optimizer cast utility functions
//---------------------------------------------------------------------------
#ifndef GPOPT_CCastUtils_H
#define GPOPT_CCastUtils_H

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"

namespace gpmd
{
class IMDId;
}

namespace gpopt
{
using namespace gpos;

// general cast utility functions
class CCastUtils
{
public:
	// is the given expression a binary coercible cast of a scalar identifier for the given column
	static BOOL FBinaryCoercibleCastedScId(CExpression *pexpr, CColRef *colref);

	// is the given expression a binary coercible cast of a scalar identifier
	static BOOL FBinaryCoercibleCastedScId(CExpression *pexpr);

	static BOOL FBinaryCoercibleCastedConst(CExpression *pexpr);

	// extract the column reference if the given expression a scalar identifier
	// or a cast of a scalar identifier or a function that casts a scalar identifier.
	// Else return NULL.
	static const CColRef *PcrExtractFromScIdOrCastScId(CExpression *pexpr);

	// cast the input column reference to the destination mdid
	static CExpression *PexprCast(CMemoryPool *mp, CMDAccessor *md_accessor,
								  const CColRef *colref, IMDId *mdid_dest);

	// check whether the given expression is a binary coercible cast of something
	static BOOL FBinaryCoercibleCast(CExpression *pexpr);

	// check whether the given expression is a cast of something
	static BOOL FScalarCast(CExpression *pexpr);

	// return the given expression without any binary coercible casts
	// that exist on the top
	static CExpression *PexprWithoutBinaryCoercibleCasts(CExpression *pexpr);

	// add explicit casting to equality operations between compatible types
	static CExpressionArray *PdrgpexprCastEquality(CMemoryPool *mp,
												   CExpression *pexpr);

	// helper to add explicit casting to left child of given equality predicate
	static CExpression *PexprAddCast(CMemoryPool *mp, CExpression *pexprPred);

	// add explicit casting on the input expression to the destination type
	static CExpression *PexprCast(CMemoryPool *mp, CMDAccessor *md_accessor,
								  CExpression *pexpr, IMDId *mdid_dest);
};	// class CCastUtils

}  // namespace gpopt

#endif