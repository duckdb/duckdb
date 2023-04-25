//---------------------------------------------------------------------------
//	@filename:
//		ICostModelParams.h
//
//	@doc:
//		Interface for the parameters of the underlying cost model
//---------------------------------------------------------------------------
#ifndef GPOPT_ICostModelParams_H
#define GPOPT_ICostModelParams_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/md/IMDRelation.h"

#include "CCost.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		ICostModelParams
//
//	@doc:
//		Interface for the parameters of the underlying cost model
//
//---------------------------------------------------------------------------
class ICostModelParams : public CRefCount
{
public:
	//---------------------------------------------------------------------------
	//	@class:
	//		SCostParam
	//
	//	@doc:
	//		Internal structure to represent cost model parameter
	//
	//---------------------------------------------------------------------------
	struct SCostParam
	{
	private:
		// param identifier
		ULONG m_id;

		// param value
		CDouble m_value;

		// param lower bound
		CDouble m_lower_bound_val;

		// param upper bound
		CDouble m_upper_bound_val;

	public:
		// ctor
		SCostParam(ULONG id, CDouble dVal, CDouble dLowerBound,
				   CDouble dUpperBound)
			: m_id(id),
			  m_value(dVal),
			  m_lower_bound_val(dLowerBound),
			  m_upper_bound_val(dUpperBound)
		{
			GPOS_ASSERT(dVal >= dLowerBound);
			GPOS_ASSERT(dVal <= dUpperBound);
		}

		// dtor
		virtual ~SCostParam(){};

		// return param identifier
		ULONG
		Id() const
		{
			return m_id;
		}

		// return value
		CDouble
		Get() const
		{
			return m_value;
		}

		// return lower bound value
		CDouble
		GetLowerBoundVal() const
		{
			return m_lower_bound_val;
		}

		// return upper bound value
		CDouble
		GetUpperBoundVal() const
		{
			return m_upper_bound_val;
		}

		BOOL
		Equals(SCostParam *pcm) const
		{
			return Id() == pcm->Id() && Get() == pcm->Get() &&
				   GetLowerBoundVal() == pcm->GetLowerBoundVal() &&
				   GetUpperBoundVal() == pcm->GetUpperBoundVal();
		}

	};	// struct SCostParam

	// lookup param by id
	virtual SCostParam *PcpLookup(ULONG id) const = 0;

	// lookup param by name
	virtual SCostParam *PcpLookup(const CHAR *szName) const = 0;

	// set param by id
	virtual void SetParam(ULONG id, CDouble dVal, CDouble dLowerBound,
						  CDouble dUpperBound) = 0;

	// set param by name
	virtual void SetParam(const CHAR *szName, CDouble dVal, CDouble dLowerBound,
						  CDouble dUpperBound) = 0;

	virtual BOOL Equals(ICostModelParams *pcm) const = 0;

	virtual const CHAR *SzNameLookup(ULONG id) const = 0;
};
}  // namespace gpopt

#endif
