//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSplitLimit.h
//
//	@doc:
//		Split a global limit into pair of local and global limit
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSplitLimit_H
#define GPOPT_CXformSplitLimit_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSplitLimit
//
//	@doc:
//		Split a global limit into pair of local and global limit
//
//---------------------------------------------------------------------------
class CXformSplitLimit : public CXformExploration
{
private:
	// private copy ctor
	CXformSplitLimit(const CXformSplitLimit &);

	// helper function for creating a limit expression
	CExpression *PexprLimit(
		CMemoryPool *mp,				// memory pool
		CExpression *pexprRelational,	// relational child
		CExpression *pexprScalarStart,	// limit offset
		CExpression *pexprScalarRows,	// limit count
		COrderSpec *pos,				// ordering specification
		BOOL fGlobal,					// is it a local or global limit
		BOOL fHasCount,					// does limit specify a number of rows
		BOOL fTopLimitUnderDML) const;

public:
	// ctor
	explicit CXformSplitLimit(CMemoryPool *mp);

	// dtor
	virtual ~CXformSplitLimit()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSplitLimit;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSplitLimit";
	}

	// Compatibility function for splitting limit
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return (CXform::ExfSplitLimit != exfid);
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformSplitLimit

}  // namespace gpopt

#endif	// !GPOPT_CXformSplitLimit_H

// EOF
