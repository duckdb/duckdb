//---------------------------------------------------------------------------
//	@filename:
//		CLogicalSetOp.h
//
//	@doc:
//		Base for set operations
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalSetOp_H
#define GPOS_CLogicalSetOp_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalSetOp
//
//	@doc:
//		Base for all set operations
//
//---------------------------------------------------------------------------
class CLogicalSetOp : public CLogical
{
protected:
	// output column array
	CColRefArray *m_pdrgpcrOutput;

	// input column array
	CColRef2dArray *m_pdrgpdrgpcrInput;

	// set representation of output columns
	CColRefSet *m_pcrsOutput;

	// set representation of input columns
	CColRefSetArray *m_pdrgpcrsInput;

	// private copy ctor
	CLogicalSetOp(const CLogicalSetOp &);

	// build set representation of input/output columns for faster set operations
	void BuildColumnSets(CMemoryPool *mp);

	// output equivalence classes
	CColRefSetArray *PdrgpcrsOutputEquivClasses(CMemoryPool *mp,
												CExpressionHandle &exprhdl,
												BOOL fIntersect) const;

	// equivalence classes from one input child, mapped to output columns
	CColRefSetArray *PdrgpcrsInputMapped(CMemoryPool *mp,
										 CExpressionHandle &exprhdl,
										 ULONG ulChild) const;

	// constraints for a given output column from all children
	CConstraintArray *PdrgpcnstrColumn(CMemoryPool *mp,
									   CExpressionHandle &exprhdl,
									   ULONG ulColIndex, ULONG ulStart) const;

	// get constraint for a given output column from a given children
	CConstraint *PcnstrColumn(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  ULONG ulColIndex, ULONG ulChild) const;

	// derive constraint property for intersect and union operators
	CPropConstraint *PpcDeriveConstraintIntersectUnion(
		CMemoryPool *mp, CExpressionHandle &exprhdl, BOOL fIntersect) const;

public:
	// ctor
	explicit CLogicalSetOp(CMemoryPool *mp);

	CLogicalSetOp(CMemoryPool *mp, CColRefArray *pdrgOutput,
				  CColRefArray *pdrgpcrLeft, CColRefArray *pdrgpcrRight);

	CLogicalSetOp(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
				  CColRef2dArray *pdrgpdrgpcrInput);

	// dtor
	virtual ~CLogicalSetOp();

	// ident accessors
	virtual EOperatorId Eopid() const = 0;

	// return a string for operator name
	virtual const CHAR *SzId() const = 0;

	// accessor of output column array
	CColRefArray *
	PdrgpcrOutput() const
	{
		GPOS_ASSERT(NULL != m_pdrgpcrOutput);
		return m_pdrgpcrOutput;
	}

	// accessor of input column array
	CColRef2dArray *
	PdrgpdrgpcrInput() const
	{
		GPOS_ASSERT(NULL != m_pdrgpdrgpcrInput);
		return m_pdrgpdrgpcrInput;
	}

	// return true if we can pull projections up past this operator from its given child
	virtual BOOL FCanPullProjectionsUp(ULONG  //child_index
	) const
	{
		return false;
	}

	// match function
	BOOL Matches(COperator *pop) const;

	virtual IOstream &OsPrint(IOstream &os) const;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *, CExpressionHandle &);

	// derive key collections
	virtual CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	// derive partition consumer info
	virtual CPartInfo *DerivePartitionInfo(CMemoryPool *mp,
										   CExpressionHandle &exprhdl) const;

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *PcrsStat(CMemoryPool *,		   // mp
								 CExpressionHandle &,  // exprhdl
								 CColRefSet *pcrsInput,
								 ULONG	// child_index
	) const;

	// conversion function
	static CLogicalSetOp *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(CUtils::FLogicalSetOp(pop));

		return dynamic_cast<CLogicalSetOp *>(pop);
	}

};	// class CLogicalSetOp

}  // namespace gpopt

#endif