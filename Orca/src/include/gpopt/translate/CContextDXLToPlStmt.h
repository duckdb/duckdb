//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CContextDXLToPlStmt.h
//
//	@doc:
//		Class providing access to CIdGenerators (needed to number initplans, motion
//		nodes as well as params), list of RangeTableEntries and Subplans
//		generated so far during DXL-->PlStmt translation.
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CContextDXLToPlStmt_H
#define GPDXL_CContextDXLToPlStmt_H

#include <vector>

#include "gpos/base.h"

#include "gpopt/gpdbwrappers.h"
#include "gpopt/translate/CDXLTranslateContext.h"
#include "gpopt/translate/CDXLTranslateContextBaseTable.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLScalarIdent.h"

extern "C" {
#include "nodes/plannodes.h"
}

namespace gpdxl
{
// fwd decl
class CDXLTranslateContext;

typedef CHashMap<ULONG, CDXLTranslateContext, gpos::HashValue<ULONG>,
				 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
				 CleanupDelete<CDXLTranslateContext> >
	HMUlDxltrctx;

//---------------------------------------------------------------------------
//	@class:
//		CContextDXLToPlStmt
//
//	@doc:
//		Class providing access to CIdGenerators (needed to number initplans, motion
//		nodes as well as params), list of RangeTableEntries and Subplans
//		generated so far during DXL-->PlStmt translation.
//
//---------------------------------------------------------------------------
class CContextDXLToPlStmt
{
private:
	// cte consumer information
	struct SCTEConsumerInfo
	{
		// list of ShareInputScan represent cte consumers
		List *m_cte_consumer_list;

		// ctor
		SCTEConsumerInfo(List *plan_cte) : m_cte_consumer_list(plan_cte)
		{
		}

		void
		AddCTEPlan(ShareInputScan *share_input_scan)
		{
			GPOS_ASSERT(NULL != share_input_scan);
			m_cte_consumer_list =
				gpdb::LAppend(m_cte_consumer_list, share_input_scan);
		}

		~SCTEConsumerInfo()
		{
			gpdb::ListFree(m_cte_consumer_list);
		}
	};

	// hash maps mapping ULONG -> SCTEConsumerInfo
	typedef CHashMap<ULONG, SCTEConsumerInfo, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupDelete<SCTEConsumerInfo> >
		HMUlCTEConsumerInfo;

	CMemoryPool *m_mp;

	// counter for generating plan ids
	CIdGenerator *m_plan_id_counter;

	// counter for generating motion ids
	CIdGenerator *m_motion_id_counter;

	// counter for generating unique param ids
	CIdGenerator *m_param_id_counter;
	List *m_param_types_list;

	// What operator classes to use for distribution keys?
	DistributionHashOpsKind m_distribution_hashops;

	// list of all rtable entries
	List *m_rtable_entries_list;

	// list of oids of partitioned tables
	List *m_partitioned_tables_list;

	// number of partition selectors for each dynamic scan
	ULongPtrArray *m_num_partition_selectors_array;

	// list of all subplan entries
	List *m_subplan_entries_list;
	List *m_subplan_sliceids_list;

	// List of PlanSlices
	List *m_slices_list;

	PlanSlice *m_current_slice;

	// index of the target relation in the rtable or 0 if not a DML statement
	ULONG m_result_relation_index;

	// hash map of the cte identifiers and the cte consumers with the same cte identifier
	HMUlCTEConsumerInfo *m_cte_consumer_info;

	// into clause
	IntoClause *m_into_clause;

	// CTAS distribution policy
	GpPolicy *m_distribution_policy;

	// FXIME: this uses NEW/DELETE, should we use palloc/pfree/memory pool?
	std::vector<List *> m_static_prune_results;

public:
	// ctor/dtor
	CContextDXLToPlStmt(CMemoryPool *mp, CIdGenerator *plan_id_counter,
						CIdGenerator *motion_id_counter,
						CIdGenerator *param_id_counter,
						DistributionHashOpsKind distribution_hashops);

	// dtor
	~CContextDXLToPlStmt();

	// retrieve the next plan id
	ULONG GetNextPlanId();

	// retrieve the current motion id
	ULONG GetCurrentMotionId();

	// retrieve the next motion id
	ULONG GetNextMotionId();

	// retrieve the current parameter type list
	List *GetParamTypes();

	// retrieve the next parameter id
	ULONG GetNextParamId(OID typeoid);

	// add a newly found CTE consumer
	void AddCTEConsumerInfo(ULONG cte_id, ShareInputScan *share_input_scan);

	// return the list of shared input scan plans representing the CTE consumers
	List *GetCTEConsumerList(ULONG cte_id) const;

	// return list of range table entries
	List *
	GetRTableEntriesList() const
	{
		return m_rtable_entries_list;
	}

	// return list of partitioned table indexes
	List *
	GetPartitionedTablesList() const
	{
		return m_partitioned_tables_list;
	}

	// return list containing number of partition selectors for every scan id
	List *GetNumPartitionSelectorsList() const;

	List *
	GetSubplanEntriesList() const
	{
		return m_subplan_entries_list;
	}

	// index of result relation in the rtable
	ULONG
	GetResultRelationIndex() const
	{
		return m_result_relation_index;
	}


	int *GetSubplanSliceIdArray();

	PlanSlice *GetSlices(int *numSlices_p);

	// add a range table entry
	void AddRTE(RangeTblEntry *rte, BOOL is_result_relation = false);

	// add a partitioned table index
	void AddPartitionedTable(OID oid);

	// increment the number of partition selectors for the given scan id
	void IncrementPartitionSelectors(ULONG scan_id);

	void AddSubplan(Plan *);

	// add a slice table entry
	int AddSlice(PlanSlice *);

	PlanSlice *
	GetCurrentSlice() const
	{
		return m_current_slice;
	}

	void
	SetCurrentSlice(PlanSlice *slice)
	{
		m_current_slice = slice;
	}

	// add CTAS information
	void AddCtasInfo(IntoClause *into_clause, GpPolicy *distribution_policy);

	// into clause
	IntoClause *
	GetIntoClause() const
	{
		return m_into_clause;
	}

	// CTAS distribution policy
	GpPolicy *
	GetDistributionPolicy() const
	{
		return m_distribution_policy;
	}

	// Get the hash opclass or hash function for given datatype,
	// based on decision made by DetermineDistributionHashOpclasses()
	Oid GetDistributionHashOpclassForType(Oid typid);
	Oid GetDistributionHashFuncForType(Oid typid);

	List *GetStaticPruneResult(ULONG scanId);
	void SetStaticPruneResult(ULONG scanId, List *static_prune_result);
};

}  // namespace gpdxl
#endif	// !GPDXL_CContextDXLToPlStmt_H

//EOF
