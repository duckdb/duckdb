//---------------------------------------------------------------------------
//	@filename:
//		CDistributionSpec.h
//
//	@doc:
//		Description of distribution;
//		Can be used as required or derived property;
//---------------------------------------------------------------------------
#ifndef GPOPT_IDistributionSpec_H
#define GPOPT_IDistributionSpec_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/utils.h"

#include "duckdb/optimizer/cascade/base/CPropSpec.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpec
//
//	@doc:
//		Base class for representing distribution specification.
//
//---------------------------------------------------------------------------
class CDistributionSpec : public CPropSpec
{
public:
	enum EDistributionType
	{
		EdtHashed,		// data is hashed across all segments
		EdtHashedNoOp,	// same as hashed, used to force multiple slices for parallel union all. The motions always mirror the underlying distributions.
		EdtStrictHashed,  // same as hashed, used to force multiple slices for parallel union all. The motions mirror the distribution of the output columns.
		EdtReplicated,	  // data is replicated across all segments
		EdtAny,		   // data can be anywhere on the segments (required only)
		EdtSingleton,  // data is on a single segment or the master
		EdtStrictSingleton,	 // data is on a single segment or the master (derived only, only compatible with other singleton distributions)
		EdtRandom,			 // data is randomly distributed across all segments
		EdtStrictRandom,  // same as random, used to force multiple slices for parallel union all.
		EdtRouted,	// data is routed to a segment explicitly specified in the tuple,
		EdtUniversal,  // data is available everywhere (derived only)
		EdtNonSingleton,  // data can have any distribution except singleton (required only)
		EdtExternal,  // data is stored on external storage (derived only)

		EdtSentinel
	};

	// description of distribution spec in terms of partitioning data across segments
	enum EDistributionPartitioningType
	{
		EdptPartitioned,  // data partitioned on multiple segments, e.g., hashed/random distribution
		EdptNonPartitioned,	 // data on a single segment, or replicated to all segments
		EdptUnknown,		 // unknown behavior

		EdptSentinel
	};

private:
	// private copy ctor
	CDistributionSpec(const CDistributionSpec &);

public:
	// ctor
	CDistributionSpec()
	{
	}

	// dtor
	virtual ~CDistributionSpec(){};

	// distribution type accessor
	virtual EDistributionType Edt() const = 0;

	// does this distribution satisfy the given one
	virtual BOOL FSatisfies(const CDistributionSpec *pds) const = 0;

	// default hash function for distribution spec
	virtual ULONG
	HashValue() const
	{
		ULONG ulEdt = (ULONG) Edt();
		return gpos::HashValue<ULONG>(&ulEdt);
	}

	// extract columns used by the distribution spec
	virtual CColRefSet *
	PcrsUsed(CMemoryPool *mp) const
	{
		// by default, return an empty set
		return GPOS_NEW(mp) CColRefSet(mp);
	}

	// property type
	virtual EPropSpecType
	Epst() const
	{
		return EpstDistribution;
	}

	// return true if distribution spec can be required
	virtual BOOL
	FRequirable() const
	{
		return true;
	}

	// return true if distribution spec can be derived
	virtual BOOL
	FDerivable() const
	{
		return true;
	}

	// default match function for distribution specs
	virtual BOOL
	Matches(const CDistributionSpec *pds) const
	{
		return Edt() == pds->Edt();
	}

	// default implementation for all the classes inheriting from
	// CDistributionSpec, if any class requires special Equals
	// handling, they should override it.
	virtual BOOL
	Equals(const CDistributionSpec *pds) const
	{
		return Matches(pds);
	}

	// return a copy of the distribution spec with remapped columns
	virtual CDistributionSpec *
	PdsCopyWithRemappedColumns(CMemoryPool *,		//mp,
							   UlongToColRefMap *,	//colref_mapping,
							   BOOL					//must_exist
	)
	{
		this->AddRef();
		return this;
	}

	// strip off any equivalent columns embedded in the distribution spec
	// (default implementation doesn't strip anything)
	virtual CDistributionSpec *
	StripEquivColumns(CMemoryPool *)
	{
		AddRef();
		return this;
	}
	// print
	virtual IOstream &OsPrint(IOstream &os) const = 0;

	// return distribution partitioning type
	virtual EDistributionPartitioningType Edpt() const = 0;

	// check if the distribution spec is either a singleton or a strict singleton distribution
	BOOL
	FSingletonOrStrictSingleton() const
	{
		return CDistributionSpec::EdtSingleton == Edt() ||
			   CDistributionSpec::EdtStrictSingleton == Edt();
	}

};	// class CDistributionSpec

// arrays of distribution spec
typedef CDynamicPtrArray<CDistributionSpec, CleanupRelease>
	CDistributionSpecArray;
}  // namespace gpopt

#endif	// !GPOPT_IDistributionSpec_H

// EOF
