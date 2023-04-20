//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSimplifySubquery.h
//
//	@doc:
//		Simplify existential/quantified subqueries by transforming
//		into count(*) subqueries
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSimplifySubquery_H
#define GPOPT_CXformSimplifySubquery_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSimplifySubquery
//
//	@doc:
//		Simplify existential/quantified subqueries by transforming
//		into count(*) subqueries
//
//---------------------------------------------------------------------------
class CXformSimplifySubquery : public CXformExploration
{
private:
	// definition of simplification function
	typedef BOOL(FnSimplify)(CMemoryPool *mp, CExpression *, CExpression **);

	// definition of matching function
	typedef BOOL(FnMatch)(COperator *);

	//---------------------------------------------------------------------------
	//	@struct:
	//		SSimplifySubqueryMapping
	//
	//	@doc:
	//		Mapping of a simplify function to matching function
	//
	//---------------------------------------------------------------------------
	struct SSimplifySubqueryMapping
	{
		// simplification function
		FnSimplify *m_pfnsimplify;

		// matching function
		FnMatch *m_pfnmatch;

	};	// struct SSimplifySubqueryMapping

	// array of mappings
	static const SSimplifySubqueryMapping m_rgssm[];

	// transform existential subqueries to count(*) subqueries
	static BOOL FSimplifyExistential(CMemoryPool *mp, CExpression *pexprScalar,
									 CExpression **ppexprNewScalar);

	// transform quantified subqueries to count(*) subqueries
	static BOOL FSimplifyQuantified(CMemoryPool *mp, CExpression *pexprScalar,
									CExpression **ppexprNewScalar);

	// main driver, transform existential/quantified subqueries to count(*) subqueries
	static BOOL FSimplify(CMemoryPool *mp, CExpression *pexprScalar,
						  CExpression **ppexprNewScalar,
						  FnSimplify *pfnsimplify, FnMatch *pfnmatch);

	// private copy ctor
	CXformSimplifySubquery(const CXformSimplifySubquery &);

public:
	// ctor
	explicit CXformSimplifySubquery(CExpression *pexprPattern);

	// dtor
	virtual ~CXformSimplifySubquery()
	{
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;


};	// class CXformSimplifySubquery

}  // namespace gpopt

#endif	// !GPOPT_CXformSimplifySubquery_H

// EOF
