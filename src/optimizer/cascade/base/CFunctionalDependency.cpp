//---------------------------------------------------------------------------
//	@filename:
//		CFunctionalDependency.cpp
//
//	@doc:
//		Implementation of functional dependency
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CFunctionalDependency.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/utils.h"

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::CFunctionalDependency
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFunctionalDependency::CFunctionalDependency(duckdb::vector<ColumnBinding> pcrsKey, duckdb::vector<ColumnBinding> pcrsDetermined)
	: m_pcrsKey(pcrsKey), m_pcrsDetermined(pcrsDetermined)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::~CFunctionalDependency
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CFunctionalDependency::~CFunctionalDependency()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::FIncluded
//
//	@doc:
//		Determine if all FD columns are included in the given column set
//
//---------------------------------------------------------------------------
BOOL CFunctionalDependency::FIncluded(duckdb::vector<ColumnBinding> pcrs) const
{
	// skip iterating if we can already tell by the sizes
	if (pcrs.size() < m_pcrsKey.size() || pcrs.size() < m_pcrsDetermined.size())
	{
		return false;
	}
	for(size_t m = 0; m < m_pcrsKey.size(); m++)
	{
		bool equal = false;
		for(size_t n = 0; n < pcrs.size(); n++)
		{
			if(m_pcrsKey[m] == pcrs[n])
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
	for(size_t m = 0; m < m_pcrsDetermined.size(); m++)
	{
		bool equal = false;
		for(size_t n = 0; n < pcrs.size(); n++)
		{
			if(m_pcrsDetermined[m] == pcrs[n])
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

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG CFunctionalDependency::HashValue() const
{
	ULONG ulHash = 0;
	for(size_t m = 0; m < m_pcrsKey.size(); m++)
	{
		ulHash = gpos::CombineHashes(ulHash, gpos::HashValue<ColumnBinding>(&m_pcrsKey[m]));
	}
	for(size_t m = 0; m < m_pcrsDetermined.size(); m++)
	{
		ulHash = gpos::CombineHashes(ulHash, gpos::HashValue<ColumnBinding>(&m_pcrsDetermined[m]));
	}
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL CFunctionalDependency::Equals(const shared_ptr<CFunctionalDependency> pfd) const
{
	if (nullptr == pfd)
	{
		return false;
	}
	duckdb::vector<ColumnBinding> pcrsKey = pfd->PcrsKey();
	if(m_pcrsKey.size() != pcrsKey.size() || m_pcrsDetermined.size() != pcrsKey.size())
	{	
		return false;
	}
	for(size_t m = 0; m < m_pcrsKey.size(); m++)
	{
		bool equal = false;
		for(size_t n = 0; n < pcrsKey.size(); n++)
		{
			if(m_pcrsKey[m] == pcrsKey[n])
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
	for(size_t m = 0; m < m_pcrsDetermined.size(); m++)
	{
		bool equal = false;
		for(size_t n = 0; n < pcrsKey.size(); n++)
		{
			if(m_pcrsDetermined[m] == pcrsKey[n])
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

BOOL CFunctionalDependency::FFunctionallyDependent(duckdb::vector<ColumnBinding> pcrsKey, ColumnBinding colref)
{
	if(m_pcrsKey.size() != pcrsKey.size())
	{	
		return false;
	}
	for(size_t m = 0; m < m_pcrsKey.size(); m++)
	{
		bool equal = false;
		for(size_t n = 0; n < pcrsKey.size(); n++)
		{
			if(m_pcrsKey[m] == pcrsKey[n])
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
	for(size_t m = 0; m < m_pcrsDetermined.size(); m++)
	{
		if(m_pcrsDetermined[m] == colref)
		{
			return true;
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG CFunctionalDependency::HashValue(const duckdb::vector<shared_ptr<CFunctionalDependency>> pdrgpfd)
{
	ULONG ulHash = 0;
	if (0 != pdrgpfd.size())
	{
		const ULONG size = pdrgpfd.size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			ulHash = gpos::CombineHashes(ulHash, pdrgpfd[ul]->HashValue());
		}
	}
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL CFunctionalDependency::Equals(const duckdb::vector<shared_ptr<CFunctionalDependency>> pdrgpfdFst, const duckdb::vector<shared_ptr<CFunctionalDependency>> pdrgpfdSnd)
{
	if (0 == pdrgpfdFst.size() && 0 == pdrgpfdSnd.size())
		return true;
	if (0 == pdrgpfdFst.size() || 0 == pdrgpfdSnd.size())
		return false;
	const ULONG ulLenFst = pdrgpfdFst.size();
	const ULONG ulLenSnd = pdrgpfdSnd.size();
	if (ulLenFst != ulLenSnd)
		return false;
	BOOL fEqual = true;
	for (ULONG ulFst = 0; fEqual && ulFst < ulLenFst; ulFst++)
	{
		const shared_ptr<CFunctionalDependency> pfdFst = pdrgpfdFst[ulFst];
		BOOL fMatch = false;
		for (ULONG ulSnd = 0; !fMatch && ulSnd < ulLenSnd; ulSnd++)
		{
			const shared_ptr<CFunctionalDependency> pfdSnd = pdrgpfdSnd[ulSnd];
			fMatch = pfdFst->Equals(pfdSnd);
		}
		fEqual = fMatch;
	}
	return fEqual;
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::PcrsKeys
//
//	@doc:
//		Create a set of all keys
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> CFunctionalDependency::PcrsKeys(const duckdb::vector<shared_ptr<CFunctionalDependency>> pdrgpfd)
{
	duckdb::vector<ColumnBinding> pcrs;
	if (pdrgpfd.size() != 0)
	{
		const ULONG size = pdrgpfd.size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			duckdb::vector<ColumnBinding> v = pdrgpfd[ul]->PcrsKey();
			pcrs.insert(pcrs.end(), v.begin(), v.end());
		}
	}
	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::PdrgpcrKeys
//
//	@doc:
//		Create an array of all keys
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> CFunctionalDependency::PdrgpcrKeys(const duckdb::vector<shared_ptr<CFunctionalDependency>> pdrgpfd)
{
	duckdb::vector<ColumnBinding> pcrs = PcrsKeys(pdrgpfd);
	return pcrs;
}
}