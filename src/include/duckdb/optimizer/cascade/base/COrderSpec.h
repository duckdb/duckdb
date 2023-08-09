//---------------------------------------------------------------------------
//	@filename:
//		COrderSpec.h
//
//	@doc:
//		Description of sort order;
//		Can be used as required or derived property;
//---------------------------------------------------------------------------
#ifndef GPOPT_COrderSpec_H
#define GPOPT_COrderSpec_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace gpopt
{
	
using namespace duckdb;
using namespace gpos;

class Operator;

// type definition of corresponding dynamic pointer array
class COrderSpec;

//---------------------------------------------------------------------------
//	@class:
//		COrderSpec
//
//	@doc:
//		Array of Order Expressions
//
//---------------------------------------------------------------------------
class COrderSpec
{
public:
	// components of order spec
	duckdb::vector<BoundOrderByNode> m_pdrgpoe;

public:
	// ctor
	explicit COrderSpec();

	// copy ctor
	COrderSpec(const COrderSpec &) = delete;

	// dtor
	virtual ~COrderSpec();

public:
	// extract columns from order spec into the given column set
	void ExtractCols(duckdb::vector<ColumnBinding> pcrs) const;

	// number of sort expressions
	ULONG UlSortColumns() const
	{
		return m_pdrgpoe.size();
	}

	// accessor of sort column of the n-th component
	ColumnBinding Pcr(ULONG ul) const
	{
		return ((BoundColumnRefExpression*)m_pdrgpoe[ul].expression.get())->binding;
	}

	// check if order spec has no columns
	bool IsEmpty() const
	{
		return UlSortColumns() == 0;
	}

	// append new component
	void Append(OrderType type, OrderByNullType null_order, Expression* expr);
	
	// void Append(ColumnBinding colref, ENullTreatment ent);

	// extract colref set of order columns
	virtual duckdb::vector<ColumnBinding> PcrsUsed() const;

	// check if order specs match
	bool Matches(COrderSpec* pos) const;

	// check if order specs satisfies req'd spec
	bool FSatisfies(COrderSpec* pos) const;

	// append enforcers to dynamic array for the given plan properties
	virtual void AppendEnforcers(CExpressionHandle &exprhdl, CReqdPropPlan* prpp, duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr, duckdb::unique_ptr<Operator> pexpr);

	// hash function
	virtual ULONG HashValue() const;

	// return a copy of the order spec with remapped columns
	// virtual COrderSpec* PosCopyWithRemappedColumns(longToExpressionMap* colref_mapping, bool must_exist);

	// return a copy of the order spec after excluding the given columns
	virtual COrderSpec* PosExcludeColumns(duckdb::vector<ColumnBinding> pcrs);

	// matching function over order spec arrays
	static bool Equals(duckdb::vector<COrderSpec*> pdrgposFirst, duckdb::vector<COrderSpec*> pdrgposSecond);

	// combine hash values of a maximum number of entries
	ULONG HashValue(duckdb::vector<COrderSpec*> pdrgpos, ULONG ulMaxSize);

	// extract colref set of order columns used by elements of order spec array
	static duckdb::vector<ColumnBinding> GetColRefSet(duckdb::vector<COrderSpec*> pdrgpos);

	// filter out array of order specs from order expressions using the passed columns
	static duckdb::vector<COrderSpec*> PdrgposExclude(duckdb::vector<COrderSpec*> pdrgpos, duckdb::vector<ColumnBinding> pcrsToExclude);
};	// class COrderSpec
}  // namespace gpopt
#endif