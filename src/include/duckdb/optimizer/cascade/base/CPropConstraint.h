//---------------------------------------------------------------------------
//	@filename:
//		CPropConstraint.h
//
//	@doc:
//		Representation of constraint property
//---------------------------------------------------------------------------
#ifndef GPOPT_CPropConstraint_H
#define GPOPT_CPropConstraint_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/column_binding.hpp"
#include <unordered_map>

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

//---------------------------------------------------------------------------
//	@class:
//		CPropConstraint
//
//	@doc:
//		Representation of constraint property
//
//---------------------------------------------------------------------------
class CPropConstraint
{
private:
	// array of equivalence classes
	duckdb::vector<duckdb::vector<ColumnBinding>> m_pdrgpcrs;

	// mapping from column to equivalence class
	std::unordered_map<ULONG, duckdb::vector<ColumnBinding>> m_phmcrcrs;

	// constraint
	Expression* m_pcnstr;

	// hidden copy ctor
	CPropConstraint(const CPropConstraint &);

	// initialize mapping from columns to equivalence classes
	void InitHashMap();

public:
	// ctor
	CPropConstraint(duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrs, Expression* pcnstr);

	// dtor
	virtual ~CPropConstraint();

	// equivalence classes
	duckdb::vector<duckdb::vector<ColumnBinding>> PdrgpcrsEquivClasses() const
	{
		return m_pdrgpcrs;
	}

	// mapping
	duckdb::vector<ColumnBinding>  PcrsEquivClass(ColumnBinding colref) const
	{
		if (m_phmcrcrs.size() == 0)
		{
			duckdb::vector<ColumnBinding> v;
			return v;
		}
		auto iter = m_phmcrcrs.find(colref.HashValue());
		return iter->second;
	}

	// constraint
	Expression* Pcnstr() const
	{
		return m_pcnstr;
	}

	// is this a contradiction
	BOOL FContradiction() const;

	// scalar expression on given column mapped from all constraints
	// on its equivalent columns
	// Expression* PexprScalarMappedFromEquivCols(ColumnBinding colref, CPropConstraint* constraintsForOuterRefs) const;
};	// class CPropConstraint
}  // namespace gpopt

#endif