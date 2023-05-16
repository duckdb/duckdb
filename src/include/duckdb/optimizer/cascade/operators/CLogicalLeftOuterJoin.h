//---------------------------------------------------------------------------
//	@filename:
//		CLogicalLeftOuterJoin.h
//
//	@doc:
//		Left outer join operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalLeftOuterJoin_H
#define GPOS_CLogicalLeftOuterJoin_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CLogicalJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalLeftOuterJoin
//
//	@doc:
//		Left outer join operator
//
//---------------------------------------------------------------------------
class CLogicalLeftOuterJoin : public CLogicalJoin
{
private:
	// private copy ctor
	CLogicalLeftOuterJoin(const CLogicalLeftOuterJoin &);

public:
	// ctor
	explicit CLogicalLeftOuterJoin(CMemoryPool *mp);

	// dtor
	virtual ~CLogicalLeftOuterJoin()
	{
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalLeftOuterJoin;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalLeftOuterJoin";
	}

	// return true if we can pull projections up past this operator from its given child
	virtual BOOL
	FCanPullProjectionsUp(ULONG child_index) const
	{
		return (0 == child_index);
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive not nullable output columns
	virtual CColRefSet *
	DeriveNotNullColumns(CMemoryPool *,	 // mp
						 CExpressionHandle &exprhdl) const
	{
		// left outer join passes through not null columns from outer child only
		return PcrsDeriveNotNullPassThruOuter(exprhdl);
	}

	// derive max card
	virtual CMaxCard DeriveMaxCard(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const;

	// derive constraint property
	virtual CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *,	 //mp,
							 CExpressionHandle &exprhdl) const
	{
		return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalLeftOuterJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalLeftOuterJoin == pop->Eopid());

		return dynamic_cast<CLogicalLeftOuterJoin *>(pop);
	}

};	// class CLogicalLeftOuterJoin

}  // namespace gpopt

#endif