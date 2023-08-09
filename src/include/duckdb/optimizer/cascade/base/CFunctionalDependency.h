//---------------------------------------------------------------------------
//	@filename:
//		CFunctionalDependency.h
//
//	@doc:
//		Functional dependency representation
//---------------------------------------------------------------------------
#ifndef GPOPT_CFunctionalDependency_H
#define GPOPT_CFunctionalDependency_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/common/vector.hpp"

namespace gpopt
{
// fwd declarations
class CFunctionalDependency;

using namespace gpos;
using namespace duckdb;

//---------------------------------------------------------------------------
//	@class:
//		CFunctionalDependency
//
//	@doc:
//		Functional dependency representation
//
//---------------------------------------------------------------------------
class CFunctionalDependency
{
private:
	// the left hand side of the FD
	duckdb::vector<ColumnBinding> m_pcrsKey;

	// the right hand side of the FD
	duckdb::vector<ColumnBinding> m_pcrsDetermined;

	// private copy ctor
	CFunctionalDependency(const CFunctionalDependency &);

public:
	// ctor
	CFunctionalDependency(duckdb::vector<ColumnBinding> pcrsKey, duckdb::vector<ColumnBinding> pcrsDetermined);

	// dtor
	virtual ~CFunctionalDependency();

	// key set accessor
	duckdb::vector<ColumnBinding> PcrsKey() const
	{
		return m_pcrsKey;
	}

	// determined set accessor
	duckdb::vector<ColumnBinding> PcrsDetermined() const
	{
		return m_pcrsDetermined;
	}

	// determine if all FD columns are included in the given column set
	BOOL FIncluded(duckdb::vector<ColumnBinding> pcrs) const;

	// hash function
	virtual ULONG HashValue() const;

	// equality function
	BOOL Equals(const shared_ptr<CFunctionalDependency> pfd) const;

	// do the given arguments form a functional dependency
	BOOL FFunctionallyDependent(duckdb::vector<ColumnBinding> pcrsKey, ColumnBinding colref);
	
	// hash function
	static ULONG HashValue(const duckdb::vector<shared_ptr<CFunctionalDependency>> pdrgpfd);

	// equality function
	static BOOL Equals(const duckdb::vector<shared_ptr<CFunctionalDependency>> pdrgpfdFst, const duckdb::vector<shared_ptr<CFunctionalDependency>> pdrgpfdSnd);

	// create a set of all keys in the passed FD's array
	static duckdb::vector<ColumnBinding> PcrsKeys(const duckdb::vector<shared_ptr<CFunctionalDependency>> pdrgpfd);

	// create an array of all keys in the passed FD's array
	static duckdb::vector<ColumnBinding> PdrgpcrKeys(const duckdb::vector<shared_ptr<CFunctionalDependency>> pdrgpfd);
};	// class CFunctionalDependency

}  // namespace gpopt

#endif