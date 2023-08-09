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

using namespace gpos;
using namespace std;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		ICostModelParams
//
//	@doc:
//		Interface for the parameters of the underlying cost model
//
//---------------------------------------------------------------------------
class ICostModelParams
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
		double m_value;

		// param lower bound
		double m_lower_bound_val;

		// param upper bound
		double m_upper_bound_val;

	public:
		// ctor
		SCostParam(ULONG id, double dVal, double dLowerBound, double dUpperBound)
			: m_id(id), m_value(dVal), m_lower_bound_val(dLowerBound), m_upper_bound_val(dUpperBound)
		{
		}

		// dtor
		virtual ~SCostParam(){};

		// return param identifier
		ULONG Id() const
		{
			return m_id;
		}

		// return value
		double Get() const
		{
			return m_value;
		}

		// return lower bound value
		double GetLowerBoundVal() const
		{
			return m_lower_bound_val;
		}

		// return upper bound value
		double GetUpperBoundVal() const
		{
			return m_upper_bound_val;
		}

		bool Equals(shared_ptr<SCostParam> pcm) const
		{
			return Id() == pcm->Id() && Get() == pcm->Get() && GetLowerBoundVal() == pcm->GetLowerBoundVal() && GetUpperBoundVal() == pcm->GetUpperBoundVal();
		}
	};	// struct SCostParam

	// lookup param by id
	virtual shared_ptr<SCostParam> PcpLookup(ULONG id) const = 0;

	// lookup param by name
	virtual shared_ptr<SCostParam> PcpLookup(const CHAR* szName) const = 0;

	// set param by id
	virtual void SetParam(ULONG id, double dVal, double dLowerBound, double dUpperBound) = 0;

	// set param by name
	virtual void SetParam(const CHAR* szName, double dVal, double dLowerBound, double dUpperBound) = 0;

	virtual bool Equals(shared_ptr<ICostModelParams> pcm) const = 0;

	virtual const CHAR* SzNameLookup(ULONG id) const = 0;
};
}  // namespace gpopt
#endif