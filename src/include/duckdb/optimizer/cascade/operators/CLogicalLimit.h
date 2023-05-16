//---------------------------------------------------------------------------
//	@filename:
//		CLogicalLimit.h
//
//	@doc:
//		Limit operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalLimit_H
#define GPOPT_CLogicalLimit_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalLimit
//
//	@doc:
//		Limit operator;
//		Scalar children compute (1) offset of start row, (2) number of rows
//
//---------------------------------------------------------------------------
class CLogicalLimit : public CLogical
{
private:
	// required sort order
	COrderSpec *m_pos;

	// global limit
	BOOL m_fGlobal;

	// does limit specify a number of rows?
	BOOL m_fHasCount;

	// the limit must be kept, even if it has no offset, nor count
	BOOL m_top_limit_under_dml;

	// private copy ctor
	CLogicalLimit(const CLogicalLimit &);

public:
	// ctors
	explicit CLogicalLimit(CMemoryPool *mp);
	CLogicalLimit(CMemoryPool *mp, COrderSpec *pos, BOOL fGlobal,
				  BOOL fHasCount, BOOL fTopLimitUnderDML);

	// dtor
	virtual ~CLogicalLimit();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalLimit;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CLogicalLimit";
	}

	// order spec
	COrderSpec *
	Pos() const
	{
		return m_pos;
	}

	// global limit
	BOOL
	FGlobal() const
	{
		return m_fGlobal;
	}

	// does limit specify a number of rows
	BOOL
	FHasCount() const
	{
		return m_fHasCount;
	}

	// must the limit be always kept
	BOOL
	IsTopLimitUnderDMLorCTAS() const
	{
		return m_top_limit_under_dml;
	}

	// match function
	virtual BOOL Matches(COperator *) const;

	// sensitivity to order of inputs
	virtual BOOL
	FInputOrderSensitive() const
	{
		return true;
	}

	// hash function
	virtual ULONG HashValue() const;

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *mp,
											CExpressionHandle &exprhdl);

	// derive outer references
	virtual CColRefSet *DeriveOuterReferences(CMemoryPool *mp,
											  CExpressionHandle &exprhdl);

	// dervive keys
	virtual CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	// derive max card
	virtual CMaxCard DeriveMaxCard(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const;

	// derive partition consumer info
	virtual CPartInfo *
	DerivePartitionInfo(CMemoryPool *,	// mp
						CExpressionHandle &exprhdl) const
	{
		return PpartinfoPassThruOuter(exprhdl);
	}

	// derive constraint property
	virtual CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *,	 //mp,
							 CExpressionHandle &exprhdl) const
	{
		return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
	}

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 CColRefSet *pcrsInput,
								 ULONG child_index) const;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *) const;

	// stat promise
	virtual EStatPromise
	Esp(CExpressionHandle &) const
	{
		return CLogical::EspHigh;
	}

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalLimit *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalLimit == pop->Eopid());

		return dynamic_cast<CLogicalLimit *>(pop);
	}

};	// class CLogicalLimit

}  // namespace gpopt

#endif