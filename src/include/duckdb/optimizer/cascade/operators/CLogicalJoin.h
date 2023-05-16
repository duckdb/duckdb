//---------------------------------------------------------------------------
//	@filename:
//		CLogicalJoin.h
//
//	@doc:
//		Base class of all logical join operators
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalJoin_H
#define GPOS_CLogicalJoin_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalJoin
//
//	@doc:
//		join operator
//
//---------------------------------------------------------------------------
class CLogicalJoin : public CLogical
{
private:
	// private copy ctor
	CLogicalJoin(const CLogicalJoin &);

protected:
	// ctor
	explicit CLogicalJoin(CMemoryPool *mp);

	// dtor
	virtual ~CLogicalJoin()
	{
	}

public:
	// match function
	virtual BOOL Matches(COperator *pop) const;


	// sensitivity to order of inputs
	BOOL
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

	// derive output columns
	virtual CColRefSet *
	DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl)
	{
		return PcrsDeriveOutputCombineLogical(mp, exprhdl);
	}

	// derive partition consumer info
	virtual CPartInfo *
	DerivePartitionInfo(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PpartinfoDeriveCombine(mp, exprhdl);
	}


	// derive keys
	CKeyCollection *
	DeriveKeyCollection(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PkcCombineKeys(mp, exprhdl);
	}

	// derive function properties
	virtual CFunctionProp *
	DeriveFunctionProperties(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PfpDeriveFromScalar(mp, exprhdl, exprhdl.Arity() - 1);
	}

	//-------------------------------------------------------------------------------------
	// Derived Stats
	//-------------------------------------------------------------------------------------

	// promise level for stat derivation
	virtual EStatPromise
	Esp(CExpressionHandle &exprhdl) const
	{
		// no stat derivation on Join trees with subqueries
		if (exprhdl.DeriveHasSubquery(exprhdl.Arity() - 1))
		{
			return EspLow;
		}

		if (NULL != exprhdl.Pgexpr() &&
			exprhdl.Pgexpr()->ExfidOrigin() == CXform::ExfExpandNAryJoin)
		{
			return EspMedium;
		}

		return EspHigh;
	}

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *
	PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsInput,
			 ULONG child_index) const
	{
		const ULONG arity = exprhdl.Arity();

		return PcrsReqdChildStats(mp, exprhdl, pcrsInput,
								  exprhdl.DeriveUsedColumns(arity - 1),
								  child_index);
	}

	// return true if operator can select a subset of input tuples based on some predicate
	virtual BOOL
	FSelectionOp() const
	{
		return true;
	}

};	// class CLogicalJoin

}  // namespace gpopt

#endif