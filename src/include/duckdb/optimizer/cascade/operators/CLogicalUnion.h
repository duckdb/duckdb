//---------------------------------------------------------------------------
//	@filename:
//		CLogicalUnion.h
//
//	@doc:
//		Logical Union operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalUnion_H
#define GPOPT_CLogicalUnion_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogicalSetOp.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalUnion
//
//	@doc:
//		union operators
//
//---------------------------------------------------------------------------
class CLogicalUnion : public CLogicalSetOp
{
private:
	// private copy ctor
	CLogicalUnion(const CLogicalUnion &);

public:
	// ctor
	explicit CLogicalUnion(CMemoryPool *mp);

	CLogicalUnion(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
				  CColRef2dArray *pdrgpdrgpcrInput);

	// dtor
	virtual ~CLogicalUnion();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalUnion;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalUnion";
	}

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const
	{
		return true;
	}

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive max card
	virtual CMaxCard DeriveMaxCard(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const;

	// derive constraint property
	virtual CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PpcDeriveConstraintIntersectUnion(mp, exprhdl,
												 false /*fIntersect*/);
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	//-------------------------------------------------------------------------------------
	// Derived Stats
	//-------------------------------------------------------------------------------------

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
	static CLogicalUnion *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalUnion == pop->Eopid());

		return reinterpret_cast<CLogicalUnion *>(pop);
	}

};	// class CLogicalUnion

}  // namespace gpopt

#endif