//---------------------------------------------------------------------------
//	@filename:
//		CKeyCollection.h
//
//	@doc:
//		Encodes key sets for a relation
//---------------------------------------------------------------------------
#ifndef GPOPT_CKeyCollection_H
#define GPOPT_CKeyCollection_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

//---------------------------------------------------------------------------
//	@class:
//		CKeyCollection
//
//	@doc:
//		Captures sets of keys for a relation
//
//---------------------------------------------------------------------------
class CKeyCollection
{
public:
	// array of key sets
	duckdb::vector<duckdb::vector<ColumnBinding>> m_pdrgpcrs;

	// private copy ctor
	CKeyCollection(const CKeyCollection &);

public:
	// ctors
	explicit CKeyCollection();
	
	CKeyCollection(duckdb::vector<ColumnBinding> pcrs);

	// dtor
	virtual ~CKeyCollection();

	// add individual set -- takes ownership
	void Add(duckdb::vector<ColumnBinding> pcrs);

	// check if set forms a key
	bool FKey(const duckdb::vector<ColumnBinding> pcrs, bool fExactMatch = true) const;

	// trim off non-key columns
	duckdb::vector<ColumnBinding> PdrgpcrTrim(const duckdb::vector<ColumnBinding> colref_array) const;

	// extract a key
	duckdb::vector<ColumnBinding> PdrgpcrKey() const;

	// extract a hashable key
	duckdb::vector<ColumnBinding> PdrgpcrHashableKey() const;

	// extract key at given position
	duckdb::vector<ColumnBinding> PdrgpcrKey(ULONG ul) const;

	// extract key at given position
	duckdb::vector<ColumnBinding> PcrsKey(ULONG ul) const;

	// number of keys
	ULONG Keys() const
	{
		return m_pdrgpcrs.size();
	}
};	// class CKeyCollection
}  // namespace gpopt
#endif