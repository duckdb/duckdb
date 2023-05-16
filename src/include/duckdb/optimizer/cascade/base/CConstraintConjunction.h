//---------------------------------------------------------------------------
//	@filename:
//		CConstraintConjunction.h
//
//	@doc:
//		Representation of a conjunction constraint. A conjunction is a number
//		of ANDed constraints
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstraintConjunction_H
#define GPOPT_CConstraintConjunction_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CConstraint.h"
#include "duckdb/optimizer/cascade/base/CRange.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CConstraintConjunction
//
//	@doc:
//		Representation of a conjunction constraint
//
//---------------------------------------------------------------------------
class CConstraintConjunction : public CConstraint
{
private:
	// array of constraints
	CConstraintArray *m_pdrgpcnstr;

	// mapping colref -> array of child constraints
	ColRefToConstraintArrayMap *m_phmcolconstr;

	// hidden copy ctor
	CConstraintConjunction(const CConstraintConjunction &);

public:
	// ctor
	CConstraintConjunction(CMemoryPool *mp, CConstraintArray *pdrgpcnstr);

	// dtor
	virtual ~CConstraintConjunction();

	// constraint type accessor
	virtual EConstraintType
	Ect() const
	{
		return CConstraint::EctConjunction;
	}

	// all constraints in conjunction
	CConstraintArray *
	Pdrgpcnstr() const
	{
		return m_pdrgpcnstr;
	}

	// is this constraint a contradiction
	virtual BOOL FContradiction() const;

	// scalar expression
	virtual CExpression *PexprScalar(CMemoryPool *mp);

	// check if there is a constraint on the given column
	virtual BOOL FConstraint(const CColRef *colref) const;

	// return a copy of the constraint with remapped columns
	virtual CConstraint *PcnstrCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	// return constraint on a given column
	virtual CConstraint *Pcnstr(CMemoryPool *mp, const CColRef *colref);

	// return constraint on a given column set
	virtual CConstraint *Pcnstr(CMemoryPool *mp, CColRefSet *pcrs);

	// return a clone of the constraint for a different column
	virtual CConstraint *PcnstrRemapForColumn(CMemoryPool *mp,
											  CColRef *colref) const;

	// print
	virtual IOstream &
	OsPrint(IOstream &os) const
	{
		return PrintConjunctionDisjunction(os, m_pdrgpcnstr);
	}

};	// class CConstraintConjunction
}  // namespace gpopt

#endif