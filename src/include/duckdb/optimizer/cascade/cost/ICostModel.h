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
#include "duckdb/optimizer/cascade/cost/ICostModelParams.h"
#include "duckdb/common/vector.hpp"

// default number of rebinds (number of times a plan is executed due to rebinding to external parameters)
#define GPOPT_DEFAULT_REBINDS 1

using namespace duckdb;
using namespace gpos;

namespace gpopt
{
// fwd declarations
class CExpressionHandle;

//---------------------------------------------------------------------------
//	@class:
//		ICostModel
//
//	@doc:
//		Interface for the underlying cost model
//
//---------------------------------------------------------------------------
class ICostModel
{
public:
	enum ECostModelType
	{ EcmtGPDBLegacy = 0, EcmtGPDBCalibrated = 1, EcmtSentinel = 2 };

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
	public:
		// number of children excluding scalar children
		ULONG m_ulChildren;

		// row estimate of root
		double m_rows;

		// width estimate of root
		double m_width;

		// number of rebinds of root
		double m_num_rebinds;

		// row estimates of child operators
		double* m_pdRowsChildren;

		// width estimates of child operators
		double* m_pdWidthChildren;

		// number of rebinds of child operators
		double* m_pdRebindsChildren;

		// computed cost of child operators
		double* m_pdCostChildren;

	public:
		// ctor
		SCostingInfo(ULONG ulChildren)
			: m_ulChildren(ulChildren), m_rows(0), m_width(0), m_num_rebinds(GPOPT_DEFAULT_REBINDS), m_pdRowsChildren(NULL), m_pdWidthChildren(NULL), m_pdRebindsChildren(NULL), m_pdCostChildren(NULL)
		{
			if (0 < ulChildren)
			{
				m_pdRowsChildren = new double[ulChildren];
				m_pdWidthChildren = new double[ulChildren];
				m_pdRebindsChildren = new double[ulChildren];
				m_pdCostChildren = new double[ulChildren];
			}
		}

		// dtor
		~SCostingInfo()
		{
			delete[] m_pdRowsChildren;
			delete[] m_pdWidthChildren;
			delete[] m_pdRebindsChildren;
			delete[] m_pdCostChildren;
		}

		// rows setter
		void SetRows(double rows)
		{
			m_rows = rows;
		}

		// width setter
		void SetWidth(double width)
		{
			m_width = width;
		}

		// rebinds setter
		void SetRebinds(double num_rebinds)
		{
			m_num_rebinds = num_rebinds;
		}

		// child rows setter
		void SetChildRows(ULONG ulPos, double dRowsChild)
		{
			m_pdRowsChildren[ulPos] = dRowsChild;
		}

		// child width setter
		void SetChildWidth(ULONG ulPos, double dWidthChild)
		{
			m_pdWidthChildren[ulPos] = dWidthChild;
		}

		// child rebinds setter
		void SetChildRebinds(ULONG ulPos, double dRebindsChild)
		{
			m_pdRebindsChildren[ulPos] = dRebindsChild;
		}

		// child cost setter
		void SetChildCost(ULONG ulPos, double dCostChild)
		{
			m_pdCostChildren[ulPos] = dCostChild;
		}
	};	// struct SCostingInfo

	// return number of hosts (nodes) that store data
	virtual ULONG UlHosts() const = 0;

	// return cost model parameters
	virtual ICostModelParams* GetCostModelParams() const = 0;

	// main driver for cost computation
	virtual double Cost(CExpressionHandle &exprhdl, const SCostingInfo* pci) const = 0;

	// cost model type
	virtual ECostModelType Ecmt() const = 0;

	// set cost model params
	void SetParams(duckdb::vector<ICostModelParams::SCostParam*> pdrgpcp);

	// create a default cost model instance
	static ICostModel* PcmDefault();
};
}  // namespace gpopt
#endif