//---------------------------------------------------------------------------
//	@filename:
//		CScalar.h
//
//	@doc:
//		Base class for all scalar operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalar_H
#define GPOPT_CScalar_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/base/CPartInfo.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"
#include "duckdb/optimizer/cascade/operators/COperator.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

// forward declaration
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CScalar
//
//	@doc:
//		base class for all scalar operators
//
//---------------------------------------------------------------------------
class CScalar : public COperator
{
public:
	// possible results of Boolean evaluation of a scalar expression
	enum EBoolEvalResult
	{
		EberTrue = 1,  // TRUE
		EberFalse,	   // FALSE
		EberNull,	   // NULL
		EberNotTrue,   // FALSE or NULL
		EberAny,	   // Any result is possible

		EerSentinel
	};

private:
	// private copy ctor
	CScalar(const CScalar &);

	// helper for combining partition consumer arrays of scalar children
	static CPartInfo *PpartinfoDeriveCombineScalar(CMemoryPool *mp,
												   CExpressionHandle &exprhdl);

protected:
	// perform conjunction of child boolean evaluation results
	static EBoolEvalResult EberConjunction(ULongPtrArray *pdrgpulChildren);

	// perform disjunction of child boolean evaluation results
	static EBoolEvalResult EberDisjunction(ULongPtrArray *pdrgpulChildren);

	// return Null if any child is Null
	static EBoolEvalResult EberNullOnAnyNullChild(
		ULongPtrArray *pdrgpulChildren);

	// return Null if all children are Null
	static EBoolEvalResult EberNullOnAllNullChildren(
		ULongPtrArray *pdrgpulChildren);

public:
	// ctor
	explicit CScalar(CMemoryPool *mp) : COperator(mp)
	{
	}

	// dtor
	virtual ~CScalar()
	{
	}

	// type of operator
	virtual BOOL
	FScalar() const
	{
		GPOS_ASSERT(!FPhysical() && !FLogical() && !FPattern());
		return true;
	}

	// create derived properties container
	virtual CDrvdProp *PdpCreate(CMemoryPool *mp) const;

	// create required properties container
	virtual CReqdProp *PrpCreate(CMemoryPool *mp) const;

	// return locally defined columns
	virtual CColRefSet *
	PcrsDefined(CMemoryPool *mp,
				CExpressionHandle &	 // exprhdl
	)
	{
		// return an empty set of column refs
		return GPOS_NEW(mp) CColRefSet(mp);
	}

	// return columns containing set-returning function
	virtual CColRefSet *
	PcrsSetReturningFunction(CMemoryPool *mp,
							 CExpressionHandle &  // exprhdl
	)
	{
		// return an empty set of column refs
		return GPOS_NEW(mp) CColRefSet(mp);
	}

	// return locally used columns
	virtual CColRefSet *
	PcrsUsed(CMemoryPool *mp,
			 CExpressionHandle &  // exprhdl
	)
	{
		// return an empty set of column refs
		return GPOS_NEW(mp) CColRefSet(mp);
	}

	// derive partition consumer info
	virtual CPartInfo *
	PpartinfoDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PpartinfoDeriveCombineScalar(mp, exprhdl);
	}

	// derive function properties
	virtual CFunctionProp *
	DeriveFunctionProperties(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PfpDeriveFromChildren(mp, exprhdl,
									 IMDFunction::EfsImmutable,	 // efsDefault
									 IMDFunction::EfdaNoSQL,	 // efdaDefault
									 false,	 // fHasVolatileFunctionScan
									 false	 // fScan
		);
	}

	// derive subquery existence
	virtual BOOL FHasSubquery(CExpressionHandle &exprhdl);

	// derive non-scalar function existence
	virtual BOOL FHasNonScalarFunction(CExpressionHandle &exprhdl);

	virtual BOOL FHasScalarArrayCmp(CExpressionHandle &exprhdl);

	// boolean expression evaluation
	virtual EBoolEvalResult
	Eber(ULongPtrArray *  // pdrgpulChildren
	) const
	{
		// by default, evaluation result can be false, true or NULL
		return EberAny;
	}

	// perform boolean evaluation of the given expression tree
	static EBoolEvalResult EberEvaluate(CMemoryPool *mp,
										CExpression *pexprScalar);

	// conversion function
	static CScalar *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(pop->FScalar());

		return reinterpret_cast<CScalar *>(pop);
	}

	// the type of the scalar expression
	virtual IMDId *MdidType() const = 0;

	// the type modifier of the scalar expression
	virtual INT
	TypeModifier() const
	{
		return default_type_modifier;
	}

};	// class CScalar

}  // namespace gpopt

#endif
