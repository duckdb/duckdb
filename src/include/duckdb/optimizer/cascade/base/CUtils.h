//---------------------------------------------------------------------------
//	@filename:
//		CUtils.h
//
//	@doc:
//		Function for convenience
//---------------------------------------------------------------------------
#ifndef GPOPT_CUTILS_H
#define GPOPT_CUTILS_H

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/optimizer/cascade/operators/Operator.h"

namespace gpopt
{
using namespace duckdb;
using namespace gpos;

class CUtils
{
public:
    //---------------------------------------------------------------------------
    //	@function:
    //		CUtils::IsDisjoint
    //
    //	@doc:
    //		Determine if disjoint
    //
    //---------------------------------------------------------------------------
    static bool IsDisjoint(duckdb::vector<ColumnBinding> pcrs1, duckdb::vector<ColumnBinding> pcrs2);
    
    // add an equivalence class (col ref set) to the array. If the new equiv
	// class contains columns from existing equiv classes, then these are merged
    static duckdb::vector<duckdb::vector<ColumnBinding>> AddEquivClassToArray(duckdb::vector<ColumnBinding> pcrsNew, duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrs);

    // merge 2 arrays of equivalence classes
	static duckdb::vector<duckdb::vector<ColumnBinding>> PdrgpcrsMergeEquivClasses(duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrsFst, duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrsSnd);

    static bool Equals(duckdb::vector<ColumnBinding> first, duckdb::vector<ColumnBinding> second);

    static bool ContainsAll(duckdb::vector<ColumnBinding> first, duckdb::vector<ColumnBinding> second);

    // check if a given operator is an enforcer
	static bool FEnforcer(Operator* pop);

    // return the number of occurrences of the given expression in the given
	// array of expressions
	static ULONG UlOccurrences(Operator* pexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr);

    static bool FMatchChildrenOrdered(Operator* pexprLeft, Operator* pexprRight);

    static bool FMatchChildrenUnordered(Operator* pexprLeft, Operator* pexprRight);

    static bool Equals(Operator* pexprLeft, Operator* pexprRight);

    static bool Equals(duckdb::vector<Operator*> pdrgpexprLeft, duckdb::vector<Operator*> pdrgpexprRight);

    // check existence of subqueries or Apply operators in deep expression tree
    static bool FHasSubquery(Operator* pexpr, bool fCheckRoot);

    // comparison function for pointers
	static int PtrCmp(const void *p1, const void *p2)
	{
		ULONG_PTR ulp1 = *(ULONG_PTR*) p1;
		ULONG_PTR ulp2 = *(ULONG_PTR*) p2;
		if (ulp1 < ulp2)
		{
			return -1;
		}
		else if (ulp1 > ulp2)
		{
			return 1;
		}
		return 0;
	}

    template<class T>
    static int SharedPtrCmp(shared_ptr<T> p1, shared_ptr<T> p2)
    {
        if (p1.get() < p2.get())
		{
			return -1;
		}
		else if (p1.get() > p2.get())
		{
			return 1;
		}
		return 0;
    }
};
}
#endif