//---------------------------------------------------------------------------
//	@filename:
//		CLogicalUnary.h
//
//	@doc:
//		Base class of logical unary operators
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalUnary_H
#define GPOS_CLogicalUnary_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"
#include "duckdb/optimizer/cascade/base/IDatum.h"

namespace gpopt
{
using namespace gpnaucrates;

// fwd declaration
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalUnary
//
//	@doc:
//		Base class of logical unary operators
//
//---------------------------------------------------------------------------
class CLogicalUnary : public CLogical
{
private:
	// private copy ctor
	CLogicalUnary(const CLogicalUnary &);

protected:
	// derive statistics for projection operators
	IStatistics *PstatsDeriveProject(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 UlongToIDatumMap *phmuldatum = NULL) const;

public:
	// ctor
	explicit CLogicalUnary(CMemoryPool *mp) : CLogical(mp)
	{
	}

	// dtor
	virtual ~CLogicalUnary()
	{
	}

	// match function
	virtual BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	virtual BOOL
	FInputOrderSensitive() const
	{
		return true;
	}

	// return a copy of the operator with remapped columns
	virtual COperator *
	PopCopyWithRemappedColumns(CMemoryPool *,		//mp,
							   UlongToColRefMap *,	//colref_mapping,
							   BOOL					//must_exist
	)
	{
		return PopCopyDefault();
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive not nullable output columns
	virtual CColRefSet *
	DeriveNotNullColumns(CMemoryPool *,	 // mp
						 CExpressionHandle &exprhdl) const
	{
		// TODO,  03/18/2012, derive nullability of columns computed by scalar child
		return PcrsDeriveNotNullPassThruOuter(exprhdl);
	}

	// derive partition consumer info
	virtual CPartInfo *
	DerivePartitionInfo(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PpartinfoDeriveCombine(mp, exprhdl);
	}

	// derive function properties
	virtual CFunctionProp *
	DeriveFunctionProperties(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PfpDeriveFromScalar(mp, exprhdl, 1 /*ulScalarIndex*/);
	}

	//-------------------------------------------------------------------------------------
	// Derived Stats
	//-------------------------------------------------------------------------------------

	// promise level for stat derivation
	virtual EStatPromise Esp(CExpressionHandle &exprhdl) const;

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *
	PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsInput,
			 ULONG child_index) const
	{
		return PcrsReqdChildStats(mp, exprhdl, pcrsInput,
								  exprhdl.DeriveUsedColumns(1), child_index);
	}

};	// class CLogicalUnary

}  // namespace gpopt

#endif