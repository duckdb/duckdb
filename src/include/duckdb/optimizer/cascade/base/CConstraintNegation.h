//---------------------------------------------------------------------------
//	@filename:
//		CConstraintNegation.h
//
//	@doc:
//		Representation of a negation constraint
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstraintNegation_H
#define GPOPT_CConstraintNegation_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CConstraint.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CConstraintNegation
//
//	@doc:
//		Representation of a negation constraint
//
//---------------------------------------------------------------------------
class CConstraintNegation : public CConstraint
{
private:
	// child constraint
	CConstraint *m_pcnstr;

	// hidden copy ctor
	CConstraintNegation(const CConstraintNegation &);

public:
	// ctor
	CConstraintNegation(CMemoryPool *mp, CConstraint *pcnstr);

	// dtor
	virtual ~CConstraintNegation();

	// constraint type accessor
	virtual EConstraintType
	Ect() const
	{
		return CConstraint::EctNegation;
	}

	// child constraint
	CConstraint *
	PcnstrChild() const
	{
		return m_pcnstr;
	}

	// is this constraint a contradiction
	virtual BOOL
	FContradiction() const
	{
		return m_pcnstr->IsConstraintUnbounded();
	}

	// is this constraint unbounded
	virtual BOOL
	IsConstraintUnbounded() const
	{
		return m_pcnstr->FContradiction();
	}

	// scalar expression
	virtual CExpression *PexprScalar(CMemoryPool *mp);

	// check if there is a constraint on the given column
	virtual BOOL
	FConstraint(const CColRef *colref) const
	{
		return m_pcnstr->FConstraint(colref);
	}

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
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CConstraintNegation
}  // namespace gpopt

#endif