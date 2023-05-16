//---------------------------------------------------------------------------
//	@filename:
//		CScalarSubqueryNotExists.h
//
//	@doc:
//		Scalar subquery NOT EXISTS operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarSubqueryNotExists_H
#define GPOPT_CScalarSubqueryNotExists_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CScalarSubqueryExistential.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarSubqueryNotExists
//
//	@doc:
//		Scalar subquery NOT EXISTS.
//
//---------------------------------------------------------------------------
class CScalarSubqueryNotExists : public CScalarSubqueryExistential
{
private:
	// private copy ctor
	CScalarSubqueryNotExists(const CScalarSubqueryNotExists &);

public:
	// ctor
	CScalarSubqueryNotExists(CMemoryPool *mp) : CScalarSubqueryExistential(mp)
	{
	}

	// dtor
	virtual ~CScalarSubqueryNotExists()
	{
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarSubqueryNotExists;
	}

	// return a string for scalar subquery
	virtual const CHAR *
	SzId() const
	{
		return "CScalarSubqueryNotExists";
	}

	// conversion function
	static CScalarSubqueryNotExists *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarSubqueryNotExists == pop->Eopid());

		return reinterpret_cast<CScalarSubqueryNotExists *>(pop);
	}

};	// class CScalarSubqueryNotExists
}  // namespace gpopt

#endif