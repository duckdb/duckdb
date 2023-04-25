//---------------------------------------------------------------------------
//	@filename:
//		ICostModel.h
//
//	@doc:
//		Interface for the underlying cost model
//---------------------------------------------------------------------------
#ifndef GPOPT_ICostModel_H
#define GPOPT_ICostModel_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/operators/COperator.h"
#include "duckdb/optimizer/cascade/md/IMDRelation.h"
#include "duckdb/optimizer/cascade/statistics/IStatistics.h"

#include "CCost.h"
#include "ICostModelParams.h"

// default number of rebinds (number of times a plan is executed due to rebinding to external parameters)
#define GPOPT_DEFAULT_REBINDS 1

namespace gpopt
{
// fwd declarations
class CExpressionHandle;

using namespace gpos;
using namespace gpmd;
using namespace gpnaucrates;

// dynamic array of cost model params
typedef CDynamicPtrArray<ICostModelParams::SCostParam, CleanupDelete>
	ICostModelParamsArray;

//---------------------------------------------------------------------------
//	@class:
//		ICostModel
//
//	@doc:
//		Interface for the underlying cost model
//
//---------------------------------------------------------------------------
class ICostModel : public CRefCount
{
public:
	enum ECostModelType
	{
		EcmtGPDBLegacy = 0,
		EcmtGPDBCalibrated = 1,
		EcmtSentinel = 2
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		SCostingStas
	//
	//	@doc:
	//		Stast information used during cost computation
	//
	//---------------------------------------------------------------------------
	class CCostingStats : public CRefCount
	{
	private:
		// stats of the root
		IStatistics *m_pstats;

	public:
		// ctor
		CCostingStats(IStatistics *stats) : m_pstats(stats)
		{
			GPOS_ASSERT(NULL != stats);
		}

		// dtor
		~CCostingStats()
		{
			m_pstats->Release();
		}

		// the risk of errors in cardinality estimation
		ULONG
		StatsEstimationRisk() const
		{
			return m_pstats->StatsEstimationRisk();
		}

		// look up the number of distinct values of a particular column
		CDouble
		GetNDVs(const CColRef *colref)
		{
			return m_pstats->GetNDVs(colref);
		}
	};	// class CCostingStats

	//---------------------------------------------------------------------------
	//	@class:
	//		SCostingInfo
	//
	//	@doc:
	//		Information used during cost computation
	//
	//---------------------------------------------------------------------------
	struct SCostingInfo
	{
	private:
		// number of children excluding scalar children
		ULONG m_ulChildren;

		// stats of the root
		CCostingStats *m_pcstats;

		// row estimate of root
		DOUBLE m_rows;

		// width estimate of root
		DOUBLE m_width;

		// number of rebinds of root
		DOUBLE m_num_rebinds;

		// row estimates of child operators
		DOUBLE *m_pdRowsChildren;

		// width estimates of child operators
		DOUBLE *m_pdWidthChildren;

		// number of rebinds of child operators
		DOUBLE *m_pdRebindsChildren;

		// computed cost of child operators
		DOUBLE *m_pdCostChildren;

		// stats of the children
		CCostingStats **m_pdrgstatsChildren;

	public:
		// ctor
		SCostingInfo(CMemoryPool *mp, ULONG ulChildren, CCostingStats *pcstats)
			: m_ulChildren(ulChildren),
			  m_pcstats(pcstats),
			  m_rows(0),
			  m_width(0),
			  m_num_rebinds(GPOPT_DEFAULT_REBINDS),
			  m_pdRowsChildren(NULL),
			  m_pdWidthChildren(NULL),
			  m_pdRebindsChildren(NULL),
			  m_pdCostChildren(NULL),
			  m_pdrgstatsChildren(NULL)
		{
			GPOS_ASSERT(NULL != pcstats);
			if (0 < ulChildren)
			{
				m_pdRowsChildren = GPOS_NEW_ARRAY(mp, DOUBLE, ulChildren);
				m_pdWidthChildren = GPOS_NEW_ARRAY(mp, DOUBLE, ulChildren);
				m_pdRebindsChildren = GPOS_NEW_ARRAY(mp, DOUBLE, ulChildren);
				m_pdCostChildren = GPOS_NEW_ARRAY(mp, DOUBLE, ulChildren);
				m_pdrgstatsChildren =
					GPOS_NEW_ARRAY(mp, CCostingStats *, ulChildren);

				for (ULONG ul = 0; ul < m_ulChildren; ul++)
				{
					m_pdrgstatsChildren[ul] = NULL;
				}
			}
		}

		// dtor
		~SCostingInfo()
		{
			GPOS_DELETE_ARRAY(m_pdRowsChildren);
			GPOS_DELETE_ARRAY(m_pdWidthChildren);
			GPOS_DELETE_ARRAY(m_pdRebindsChildren);
			GPOS_DELETE_ARRAY(m_pdCostChildren);

			for (ULONG ul = 0; ul < m_ulChildren; ul++)
			{
				CRefCount::SafeRelease(m_pdrgstatsChildren[ul]);
			}

			GPOS_DELETE_ARRAY(m_pdrgstatsChildren);
			m_pcstats->Release();
		}

		// children accessor
		ULONG
		ChildCount() const
		{
			return m_ulChildren;
		}

		// rows accessor
		DOUBLE
		Rows() const
		{
			return m_rows;
		}

		// rows setter
		void
		SetRows(DOUBLE rows)
		{
			GPOS_ASSERT(0 <= rows);

			m_rows = rows;
		}

		// width accessor
		DOUBLE
		Width() const
		{
			return m_width;
		}

		// width setter
		void
		SetWidth(DOUBLE width)
		{
			GPOS_ASSERT(0 <= width);

			m_width = width;
		}

		// rebinds accessor
		DOUBLE
		NumRebinds() const
		{
			return m_num_rebinds;
		}

		// rebinds setter
		void
		SetRebinds(DOUBLE num_rebinds)
		{
			GPOS_ASSERT(GPOPT_DEFAULT_REBINDS <= num_rebinds);

			m_num_rebinds = num_rebinds;
		}

		// children rows accessor
		DOUBLE *
		PdRows() const
		{
			return m_pdRowsChildren;
		}

		// child rows setter
		void
		SetChildRows(ULONG ulPos, DOUBLE dRowsChild)
		{
			GPOS_ASSERT(0 <= dRowsChild);
			GPOS_ASSERT(ulPos < m_ulChildren);

			m_pdRowsChildren[ulPos] = dRowsChild;
		}

		// children width accessor
		DOUBLE *
		GetWidth() const
		{
			return m_pdWidthChildren;
		}

		// child width setter
		void
		SetChildWidth(ULONG ulPos, DOUBLE dWidthChild)
		{
			GPOS_ASSERT(0 <= dWidthChild);
			GPOS_ASSERT(ulPos < m_ulChildren);

			m_pdWidthChildren[ulPos] = dWidthChild;
		}

		// children rebinds accessor
		DOUBLE *
		PdRebinds() const
		{
			return m_pdRebindsChildren;
		}

		// child rebinds setter
		void
		SetChildRebinds(ULONG ulPos, DOUBLE dRebindsChild)
		{
			GPOS_ASSERT(GPOPT_DEFAULT_REBINDS <= dRebindsChild);
			GPOS_ASSERT(ulPos < m_ulChildren);

			m_pdRebindsChildren[ulPos] = dRebindsChild;
		}

		// children cost accessor
		DOUBLE *
		PdCost() const
		{
			return m_pdCostChildren;
		}

		// child cost setter
		void
		SetChildCost(ULONG ulPos, DOUBLE dCostChild)
		{
			GPOS_ASSERT(0 <= dCostChild);
			GPOS_ASSERT(ulPos < m_ulChildren);

			m_pdCostChildren[ulPos] = dCostChild;
		}

		// child stats setter
		void
		SetChildStats(ULONG ulPos, CCostingStats *child_stats)
		{
			m_pdrgstatsChildren[ulPos] = child_stats;
		}

		// return additional cost statistics
		CCostingStats *
		Pcstats() const
		{
			return m_pcstats;
		}

		// return additional child statistics
		CCostingStats *
		Pcstats(ULONG child_index) const
		{
			return m_pdrgstatsChildren[child_index];
		}

	};	// struct SCostingInfo

	// return number of hosts (nodes) that store data
	virtual ULONG UlHosts() const = 0;

	// return number of rows per host
	virtual CDouble DRowsPerHost(CDouble dRowsTotal) const = 0;

	// return cost model parameters
	virtual ICostModelParams *GetCostModelParams() const = 0;

	// main driver for cost computation
	virtual CCost Cost(CExpressionHandle &exprhdl,
					   const SCostingInfo *pci) const = 0;

	// cost model type
	virtual ECostModelType Ecmt() const = 0;

	// set cost model params
	void SetParams(ICostModelParamsArray *pdrgpcp);

	// create a default cost model instance
	static ICostModel *PcmDefault(CMemoryPool *mp);
};
}  // namespace gpopt

#endif
