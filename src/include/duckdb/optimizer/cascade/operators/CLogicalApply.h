//---------------------------------------------------------------------------
//	@filename:
//		CLogicalApply.h
//
//	@doc:
//		Logical Apply operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalApply_H
#define GPOPT_CLogicalApply_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalApply
//
//	@doc:
//		Logical Apply operator; parent of different Apply operators used
//		in subquery transformations
//
//---------------------------------------------------------------------------
class CLogicalApply : public CLogical
{
private:
	// private copy ctor
	CLogicalApply(const CLogicalApply &);

protected:
	// columns used from Apply's inner child
	CColRefArray *m_pdrgpcrInner;

	// origin subquery id
	EOperatorId m_eopidOriginSubq;

	// ctor
	explicit CLogicalApply(CMemoryPool *mp);

	// ctor
	CLogicalApply(CMemoryPool *mp, CColRefArray *pdrgpcrInner,
				  EOperatorId eopidOriginSubq);

	// dtor
	virtual ~CLogicalApply();

public:
	// match function
	virtual BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	virtual BOOL
	FInputOrderSensitive() const
	{
		return true;
	}

	// inner column references accessor
	CColRefArray *
	PdrgPcrInner() const
	{
		return m_pdrgpcrInner;
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
		return PfpDeriveFromScalar(mp, exprhdl, 2 /*ulScalarIndex*/);
	}

	//-------------------------------------------------------------------------------------
	// Derived Stats
	//-------------------------------------------------------------------------------------

	// derive statistics
	virtual IStatistics *
	PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
				 IStatisticsArray *	 // stats_ctxt
	) const
	{
		// we should use stats from the corresponding Join tree if decorrelation succeeds
		return PstatsDeriveDummy(mp, exprhdl, CStatistics::DefaultRelationRows);
	}

	// promise level for stat derivation
	virtual EStatPromise
	Esp(CExpressionHandle &	 // exprhdl
	) const
	{
		// whenever we can decorrelate an Apply tree, we should use the corresponding Join tree
		return EspLow;
	}

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 CColRefSet *pcrsInput,
								 ULONG child_index) const;

	// return true if operator is a correlated apply
	virtual BOOL
	FCorrelated() const
	{
		return false;
	}

	// return true if operator is a left semi apply
	virtual BOOL
	FLeftSemiApply() const
	{
		return false;
	}

	// return true if operator is a left anti semi apply
	virtual BOOL
	FLeftAntiSemiApply() const
	{
		return false;
	}

	// return true if operator can select a subset of input tuples based on some predicate
	virtual BOOL
	FSelectionOp() const
	{
		return true;
	}

	// origin subquery id
	EOperatorId
	EopidOriginSubq() const
	{
		return m_eopidOriginSubq;
	}

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

	// conversion function
	static CLogicalApply *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(CUtils::FApply(pop));

		return dynamic_cast<CLogicalApply *>(pop);
	}

};	// class CLogicalApply

}  // namespace gpopt

#endif