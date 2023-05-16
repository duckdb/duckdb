//---------------------------------------------------------------------------
//	@filename:
//		CScalarSubqueryAny.h
//
//	@doc:
//		Class for scalar subquery ANY operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarSubqueryAny_H
#define GPOPT_CScalarSubqueryAny_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CScalarSubqueryQuantified.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarSubqueryAny
//
//	@doc:
//		Scalar subquery ANY.
//		A scalar subquery ANY expression has two children: relational and scalar.
//
//---------------------------------------------------------------------------
class CScalarSubqueryAny : public CScalarSubqueryQuantified
{
private:
	// private copy ctor
	CScalarSubqueryAny(const CScalarSubqueryAny &);

public:
	// ctor
	CScalarSubqueryAny(CMemoryPool *mp, IMDId *scalar_op_mdid,
					   const CWStringConst *pstrScalarOp,
					   const CColRef *colref);

	// dtor
	virtual ~CScalarSubqueryAny()
	{
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarSubqueryAny;
	}

	// return a string for scalar subquery
	virtual const CHAR *
	SzId() const
	{
		return "CScalarSubqueryAny";
	}

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	// conversion function
	static CScalarSubqueryAny *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarSubqueryAny == pop->Eopid());

		return reinterpret_cast<CScalarSubqueryAny *>(pop);
	}

};	// class CScalarSubqueryAny
}  // namespace gpopt

#endif