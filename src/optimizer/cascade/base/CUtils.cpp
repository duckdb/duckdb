#include "duckdb/optimizer/cascade/base/CUtils.h"
#include <algorithm>

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

bool CUtils::IsDisjoint(duckdb::vector<ColumnBinding> pcrs1, duckdb::vector<ColumnBinding> pcrs2)
{
    duckdb::vector<ColumnBinding> target;
    std::set_intersection(pcrs1.begin(), pcrs1.end(), pcrs2.begin(), pcrs2.end(), target.begin());
    if(target.size() == 0)
    {
        return true;
    }   
    return false;
}

// add an equivalence class to the array. If the new equiv class contains
// columns from separate equiv classes, then these are merged. Returns a new
// array of equivalence classes
duckdb::vector<duckdb::vector<ColumnBinding>> CUtils::AddEquivClassToArray(duckdb::vector<ColumnBinding> pcrsNew, duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrs)
{
	duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrsNew;
	duckdb::vector<ColumnBinding> pcrsCopy = pcrsNew;
	const ULONG length = pdrgpcrs.size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		duckdb::vector<ColumnBinding> pcrs = pdrgpcrs[ul];
		if (IsDisjoint(pcrsCopy, pcrs))
		{
			pdrgpcrsNew.push_back(pcrs);
		}
		else
		{
			pcrsCopy.insert(pcrs.begin(), pcrs.end(), pcrsCopy.end());
		}
	}
	pdrgpcrsNew.push_back(pcrsCopy);
	return pdrgpcrsNew;
}

// merge 2 arrays of equivalence classes
duckdb::vector<duckdb::vector<ColumnBinding>> CUtils::PdrgpcrsMergeEquivClasses(duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrsFst, duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrsSnd)
{
	duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrsMerged = pdrgpcrsFst;
	ULONG length = pdrgpcrsSnd.size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		duckdb::vector<ColumnBinding> pcrs = pdrgpcrsSnd[ul];
		duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrs = AddEquivClassToArray(pcrs, pdrgpcrsMerged);
		pdrgpcrsMerged = pdrgpcrs;
	}
	return pdrgpcrsMerged;
}

bool CUtils::Equals(duckdb::vector<ColumnBinding> first, duckdb::vector<ColumnBinding> second)
{
	if(first.size() != second.size())
		return false;
	for(auto &c1 : first)
	{
		bool equal = false;
		for(auto &c2 : second)
		{
			if(c1 == c2)
			{
				equal = true;
				break;
			}
		}
		if(!equal)
		{
			return false;
		}
	}
	return true;
}

bool CUtils::ContainsAll(duckdb::vector<ColumnBinding> parent, duckdb::vector<ColumnBinding> child)
{
	if(parent.size() < child.size())
	{
		return false;
	}
	for(auto &c1 : child)
	{
		bool equal = false;
		for(auto &c2 : parent)
		{
			if(c1 == c2)
			{
				equal = true;
				break;
			}
		}
		if(!equal)
		{
			return false;
		}
	}
	return true;
}

// check if a given operator is an FEnforcer
bool CUtils::FEnforcer(Operator* pop)
{
	return PhysicalOperatorType::ORDER_BY == pop->physical_type || LogicalOperatorType::LOGICAL_ORDER_BY == pop->logical_type;
}

// return the number of occurrences of the given expression in the given array of expressions
ULONG CUtils::UlOccurrences(Operator* pexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr)
{
	ULONG count = 0;
	ULONG size = pdrgpexpr.size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		if (Equals(pexpr, pdrgpexpr[ul].get()))
		{
			count++;
		}
	}
	return count;
}

// check if two expressions have the same children in any order
bool CUtils::FMatchChildrenUnordered(Operator* pexprLeft, Operator* pexprRight)
{
	if(pexprLeft->Arity() != pexprRight->Arity())
	{
		return false;
	}
	bool fEqual = true;
	const ULONG arity = pexprLeft->Arity();
	for (ULONG ul = 0; fEqual && ul < arity; ul++)
	{
		fEqual = false;
		for(ULONG cnt = 0; cnt < pexprRight->Arity(); cnt++)
		{
			if(Equals(pexprLeft->children[ul].get(), pexprRight->children[cnt].get()))
			{
				fEqual = true;
				break;
			}
		}
		// fEqual = (UlOccurrences(pexprLeft->children[ul].get(), pexprLeft->children) == UlOccurrences(pexprLeft->children[ul].get(), pexprRight->children));
	}
	return fEqual;
}

// check if two expressions have the same children in the same order
bool CUtils::FMatchChildrenOrdered(Operator *pexprLeft, Operator *pexprRight)
{
	bool fEqual = true;
	const ULONG arity = pexprLeft->Arity();
	for (ULONG ul = 0; fEqual && ul < arity; ul++)
	{
		// child must be at the same position in the other expression
		fEqual = CUtils::Equals(pexprLeft->children[ul].get(), pexprRight->children[ul].get());
	}
	return fEqual;
}

// deep equality of expression trees
bool CUtils::Equals(Operator* pexprLeft, Operator* pexprRight)
{
	// NULL expressions are equal
	if (NULL == pexprLeft || NULL == pexprRight)
	{
		return NULL == pexprLeft && NULL == pexprRight;
	}
	// start with pointers comparison
	if (pexprLeft == pexprRight)
	{
		return true;
	}
	// compare number of children and root operators
	if (pexprLeft->Arity() != pexprRight->Arity() || !pexprLeft->Matches(pexprRight))
	{
		return false;
	}
	if (0 < pexprLeft->Arity() && pexprLeft->FInputOrderSensitive())
	{
		return FMatchChildrenOrdered(pexprLeft, pexprRight);
	}
	return FMatchChildrenUnordered(pexprLeft, pexprRight);
}

// deep equality of expression arrays
bool CUtils::Equals(duckdb::vector<Operator*> pdrgpexprLeft, duckdb::vector<Operator*> pdrgpexprRight)
{
	// NULL arrays are equal
	if (0 == pdrgpexprLeft.size() || 0 == pdrgpexprRight.size())
	{
		return 0 == pdrgpexprLeft.size() && 0 == pdrgpexprRight.size();
	}
	const ULONG length = pdrgpexprLeft.size();
	bool fEqual = (length == pdrgpexprRight.size());
	for (ULONG ul = 0; ul < length && fEqual; ul++)
	{
		fEqual = Equals(pdrgpexprLeft[ul], pdrgpexprRight[ul]);
	}
	return fEqual;
}

}