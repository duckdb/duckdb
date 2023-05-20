//---------------------------------------------------------------------------
//	@filename:
//		CScalarNullTest.h
//
//	@doc:
//		Scalar null test
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarNullTest_H
#define GPOPT_CScalarNullTest_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarNullTest
//
//	@doc:
//		Scalar null test operator
//
//---------------------------------------------------------------------------
class CScalarNullTest : public CScalar
{
private:
	// private copy ctor
	CScalarNullTest(const CScalarNullTest &);

public:
	// ctor
	explicit CScalarNullTest(CMemoryPool *mp) : CScalar(mp)
	{
	}

	// dtor
	virtual ~CScalarNullTest()
	{
	}

	// ident accessors
	virtual EOperatorId Eopid() const
	{
		return EopScalarNullTest;
	}

	// return a string for operator name
	virtual const CHAR* SzId() const
	{
		return "CScalarNullTest";
	}

	// match function
	BOOL Matches(COperator *) const;

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const
	{
		return false;
	}

	// return a copy of the operator with remapped columns
	virtual COperator* PopCopyWithRemappedColumns(CMemoryPool* mp, UlongToColRefMap* colref_mapping, BOOL must_exist)
	{
		return PopCopyDefault();
	}

	// the type of the scalar expression
	virtual IMDId *MdidType() const;

	// boolean expression evaluation
	virtual EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const;

	// conversion function
	static CScalarNullTest* PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarNullTest == pop->Eopid());
		return dynamic_cast<CScalarNullTest *>(pop);
	}
};	// class CScalarNullTest

}  // namespace gpopt

#endif