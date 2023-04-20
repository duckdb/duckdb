//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatisticsConfig.h
//
//	@doc:
//		Statistics configurations
//---------------------------------------------------------------------------
#ifndef GPOPT_CStatisticsConfig_H
#define GPOPT_CStatisticsConfig_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "gpos/common/CRefCount.h"
#include "gpos/memory/CMemoryPool.h"

#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/IMDId.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CStatisticsConfig
//
//	@doc:
//		Statistics configurations
//
//---------------------------------------------------------------------------
class CStatisticsConfig : public CRefCount
{
private:
	// shared memory pool
	CMemoryPool *m_mp;

	// damping factor for filter
	CDouble m_damping_factor_filter;

	// damping factor for join
	CDouble m_damping_factor_join;

	// damping factor for group by
	CDouble m_damping_factor_groupby;

	// hash set of md ids for columns with missing statistics
	MdidHashSet *m_phsmdidcolinfo;

public:
	// ctor
	CStatisticsConfig(CMemoryPool *mp, CDouble damping_factor_filter,
					  CDouble damping_factor_join,
					  CDouble damping_factor_groupby);

	// dtor
	~CStatisticsConfig();

	// damping factor for filter
	CDouble
	DDampingFactorFilter() const
	{
		return m_damping_factor_filter;
	}

	// damping factor for join
	CDouble
	DDampingFactorJoin() const
	{
		return m_damping_factor_join;
	}

	// damping factor for group by
	CDouble
	DDampingFactorGroupBy() const
	{
		return m_damping_factor_groupby;
	}

	// add the information about the column with the missing statistics
	void AddMissingStatsColumn(CMDIdColStats *pmdidCol);

	// collect the missing statistics columns
	void CollectMissingStatsColumns(IMdIdArray *pdrgmdid);

	// generate default optimizer configurations
	static CStatisticsConfig *
	PstatsconfDefault(CMemoryPool *mp)
	{
		return GPOS_NEW(mp) CStatisticsConfig(
			mp, 0.75 /* damping_factor_filter */,
			0.01 /* damping_factor_join */, 0.75 /* damping_factor_groupby */
		);
	}


};	// class CStatisticsConfig
}  // namespace gpopt

#endif	// !GPOPT_CStatisticsConfig_H

// EOF
