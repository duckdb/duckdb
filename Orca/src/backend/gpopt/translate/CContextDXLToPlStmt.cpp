//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CContextDXLToPlStmt.cpp
//
//	@doc:
//		Implementation of the functions that provide
//		access to CIdGenerators (needed to number initplans, motion
//		nodes as well as params), list of RangeTableEntires and Subplans
//		generated so far during DXL-->PlStmt translation.
//
//	@test:
//
//
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "utils/rel.h"
}

#include "gpos/base.h"

#include "gpopt/gpdbwrappers.h"
#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "naucrates/exception.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::CContextDXLToPlStmt
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CContextDXLToPlStmt::CContextDXLToPlStmt(
	CMemoryPool *mp, CIdGenerator *plan_id_counter,
	CIdGenerator *motion_id_counter, CIdGenerator *param_id_counter,
	DistributionHashOpsKind distribution_hashops)
	: m_mp(mp),
	  m_plan_id_counter(plan_id_counter),
	  m_motion_id_counter(motion_id_counter),
	  m_param_id_counter(param_id_counter),
	  m_param_types_list(NIL),
	  m_distribution_hashops(distribution_hashops),
	  m_rtable_entries_list(NULL),
	  m_partitioned_tables_list(NULL),
	  m_num_partition_selectors_array(NULL),
	  m_subplan_entries_list(NULL),
	  m_subplan_sliceids_list(NULL),
	  m_slices_list(NULL),
	  m_result_relation_index(0),
	  m_into_clause(NULL),
	  m_distribution_policy(NULL)
{
	m_cte_consumer_info = GPOS_NEW(m_mp) HMUlCTEConsumerInfo(m_mp);
	m_num_partition_selectors_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::~CContextDXLToPlStmt
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CContextDXLToPlStmt::~CContextDXLToPlStmt()
{
	m_cte_consumer_info->Release();
	m_num_partition_selectors_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetNextPlanId
//
//	@doc:
//		Get the next plan id
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::GetNextPlanId()
{
	return m_plan_id_counter->next_id();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetCurrentMotionId
//
//	@doc:
//		Get the current motion id
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::GetCurrentMotionId()
{
	return m_motion_id_counter->current_id();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetNextMotionId
//
//	@doc:
//		Get the next motion id
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::GetNextMotionId()
{
	return m_motion_id_counter->next_id();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetNextParamId
//
//	@doc:
//		Get the next param id, for a parameter of type 'typeoid'
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::GetNextParamId(OID typeoid)
{
	m_param_types_list = gpdb::LAppendOid(m_param_types_list, typeoid);

	return m_param_id_counter->next_id();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetParamTypes
//
//	@doc:
//		Get the current param types list
//
//---------------------------------------------------------------------------
List *
CContextDXLToPlStmt::GetParamTypes()
{
	return m_param_types_list;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddCTEConsumerInfo
//
//	@doc:
//		Add information about the newly found CTE entry
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::AddCTEConsumerInfo(ULONG cte_id,
										ShareInputScan *share_input_scan)
{
	GPOS_ASSERT(NULL != share_input_scan);

	SCTEConsumerInfo *cte_info = m_cte_consumer_info->Find(&cte_id);
	if (NULL != cte_info)
	{
		cte_info->AddCTEPlan(share_input_scan);
		return;
	}

	List *cte_plan = ListMake1(share_input_scan);

	ULONG *key = GPOS_NEW(m_mp) ULONG(cte_id);
	BOOL result GPOS_ASSERTS_ONLY = m_cte_consumer_info->Insert(
		key, GPOS_NEW(m_mp) SCTEConsumerInfo(cte_plan));

	GPOS_ASSERT(result);
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetCTEConsumerList
//
//	@doc:
//		Return the list of GPDB plan nodes representing the CTE consumers
//		with the given CTE identifier
//---------------------------------------------------------------------------
List *
CContextDXLToPlStmt::GetCTEConsumerList(ULONG cte_id) const
{
	SCTEConsumerInfo *cte_info = m_cte_consumer_info->Find(&cte_id);
	if (NULL != cte_info)
	{
		return cte_info->m_cte_consumer_list;
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddRTE
//
//	@doc:
//		Add a RangeTableEntries
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::AddRTE(RangeTblEntry *rte, BOOL is_result_relation)
{
	m_rtable_entries_list = gpdb::LAppend(m_rtable_entries_list, rte);

	rte->inFromCl = true;

	if (is_result_relation)
	{
		GPOS_ASSERT(0 == m_result_relation_index &&
					"Only one result relation supported");
		rte->inFromCl = false;
		m_result_relation_index = gpdb::ListLength(m_rtable_entries_list);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddPartitionedTable
//
//	@doc:
//		Add a partitioned table oid
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::AddPartitionedTable(OID oid)
{
	if (!gpdb::ListMemberOid(m_partitioned_tables_list, oid))
	{
		m_partitioned_tables_list =
			gpdb::LAppendOid(m_partitioned_tables_list, oid);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::IncrementPartitionSelectors
//
//	@doc:
//		Increment the number of partition selectors for the given scan id
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::IncrementPartitionSelectors(ULONG scan_id)
{
	// add extra elements to the array if necessary
	const ULONG len = m_num_partition_selectors_array->Size();
	for (ULONG ul = len; ul <= scan_id; ul++)
	{
		ULONG *pul = GPOS_NEW(m_mp) ULONG(0);
		m_num_partition_selectors_array->Append(pul);
	}

	ULONG *ul = (*m_num_partition_selectors_array)[scan_id];
	(*ul)++;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetNumPartitionSelectorsList
//
//	@doc:
//		Return list containing number of partition selectors for every scan id
//
//---------------------------------------------------------------------------
List *
CContextDXLToPlStmt::GetNumPartitionSelectorsList() const
{
	List *partition_selectors_list = NIL;
	const ULONG len = m_num_partition_selectors_array->Size();
	for (ULONG ul = 0; ul < len; ul++)
	{
		ULONG *num_partition_selectors = (*m_num_partition_selectors_array)[ul];
		partition_selectors_list = gpdb::LAppendInt(partition_selectors_list,
													*num_partition_selectors);
	}

	return partition_selectors_list;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetSubplanSliceIdArray
//
//	@doc:
//		Get the slice IDs of each subplan as an array.
//
//---------------------------------------------------------------------------
int *
CContextDXLToPlStmt::GetSubplanSliceIdArray()
{
	int numSubplans = list_length(m_subplan_entries_list);
	int *sliceIdArray;
	ListCell *lc;
	int i;

	sliceIdArray = (int *) gpdb::GPDBAlloc(numSubplans * sizeof(int));

	i = 0;
	foreach (lc, m_subplan_sliceids_list)
	{
		sliceIdArray[i++] = lfirst_int(lc);
	}

	return sliceIdArray;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetSlices
//
//	@doc:
//		Get the slice table as an array
//
//---------------------------------------------------------------------------
PlanSlice *
CContextDXLToPlStmt::GetSlices(int *numSlices_p)
{
	int numSlices = list_length(m_slices_list);
	PlanSlice *sliceArray;
	ListCell *lc;
	int i;

	sliceArray = (PlanSlice *) gpdb::GPDBAlloc(numSlices * sizeof(PlanSlice));

	i = 0;
	foreach (lc, m_slices_list)
	{
		PlanSlice *src = (PlanSlice *) lfirst(lc);

		memcpy(&sliceArray[i], src, sizeof(PlanSlice));

		i++;
	}

	m_current_slice = NULL;
	gpdb::ListFreeDeep(m_slices_list);

	*numSlices_p = numSlices;
	return sliceArray;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddSubplan
//
//	@doc:
//		Add a subplan
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::AddSubplan(Plan *plan)
{
	m_subplan_entries_list = gpdb::LAppend(m_subplan_entries_list, plan);
	m_subplan_sliceids_list =
		gpdb::LAppendInt(m_subplan_sliceids_list, m_current_slice->sliceIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddSlice
//
//	@doc:
//		Add a plan slice
//
//---------------------------------------------------------------------------
int
CContextDXLToPlStmt::AddSlice(PlanSlice *slice)
{
	slice->sliceIndex = list_length(m_slices_list);
	m_slices_list = gpdb::LAppend(m_slices_list, slice);

	return slice->sliceIndex;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddCtasInfo
//
//	@doc:
//		Add CTAS info
//
//---------------------------------------------------------------------------
// GPDB_92_MERGE_FIXME: we really should care about intoClause
// But planner cheats. FIX that and re-enable ORCA's handling of intoClause
void
CContextDXLToPlStmt::AddCtasInfo(IntoClause *into_clause,
								 GpPolicy *distribution_policy)
{
	//	GPOS_ASSERT(NULL != into_clause);
	GPOS_ASSERT(NULL != distribution_policy);

	m_into_clause = into_clause;
	m_distribution_policy = distribution_policy;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetDistributionHashOpclassForType
//
//	@doc:
//		Return a hash operator class to use for computing
//		distribution key values for the given datatype.
//
//		This returns either the default opclass, or the legacy
//		opclass of the type, depending on what opclasses we have
//		seen being used in tables so far.
//---------------------------------------------------------------------------
Oid
CContextDXLToPlStmt::GetDistributionHashOpclassForType(Oid typid)
{
	Oid opclass = InvalidOid;

	switch (m_distribution_hashops)
	{
		case DistrUseDefaultHashOps:
			opclass = gpdb::GetDefaultDistributionOpclassForType(typid);
			break;

		case DistrUseLegacyHashOps:
			opclass = gpdb::GetLegacyCdbHashOpclassForBaseType(typid);
			break;

		case DistrHashOpsNotDeterminedYet:
			// None of the tables we have seen so far have been
			// hash distributed, so we haven't made up our mind
			// on which opclasses to use yet. But we have to
			// pick something now.
			//
			// FIXME: It's quite unoptimal that this ever happens.
			// To avoid this we should make a pass over the tree to
			// determine the opclasses, before translating
			// anything. But there is no convenient way to "walk"
			// the DXL representation AFAIK.
			//
			// Example query where this happens:
			// select * from dd_singlecol_1 t1,
			//               generate_series(1,10) g
			// where t1.a=g.g and t1.a=1 ;
			//
			// The ORCA plan consists of a join between
			// Result+FunctionScan and TableScan. The
			// Result+FunctionScan side is processed first, and
			// this gets called to generate a "hash filter" for
			// it Result. The TableScan is encountered and
			// added to the range table only later. If it uses
			// legacy ops, we have already decided to use default
			// ops here, and we fall back unnecessarily.
			//
			// On the other hand, when the opclass is not specified in the
			// distributed-by clause one should be decided according to the
			// gp_use_legacy_hashops setting.
			opclass = gpdb::GetColumnDefOpclassForType(NIL, typid);
			// update m_distribution_hashops accordingly
			if (opclass == gpdb::GetDefaultDistributionOpclassForType(typid))
			{
				m_distribution_hashops = DistrUseDefaultHashOps;
			}
			else if (opclass == gpdb::GetLegacyCdbHashOpclassForBaseType(typid))
			{
				m_distribution_hashops = DistrUseLegacyHashOps;
			}
			else
			{
				GPOS_RAISE(
					gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
					GPOS_WSZ_LIT("Unsupported distribution hashops policy"));
			}
			break;
	}

	return opclass;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetDistributionHashFuncForType
//
//	@doc:
//		Return a hash function to use for computing distribution key
//		values for the given datatype.
//
//		This returns the hash function either from the default
//		opclass, or the legacy opclass of the type, depending on
//		what opclasses we have seen being used in tables so far.
//---------------------------------------------------------------------------
Oid
CContextDXLToPlStmt::GetDistributionHashFuncForType(Oid typid)
{
	Oid opclass;
	Oid opfamily;
	Oid hashproc;

	opclass = GetDistributionHashOpclassForType(typid);

	if (opclass == InvalidOid)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
				   GPOS_WSZ_LIT("no default hash opclasses found"));
	}

	opfamily = gpdb::GetOpclassFamily(opclass);
	hashproc = gpdb::GetHashProcInOpfamily(opfamily, typid);

	return hashproc;
}

List *
CContextDXLToPlStmt::GetStaticPruneResult(ULONG scanId)
{
	// GPDB_12_MERGE_FIXME: we haven't seen the scan id yet, this scan id is likely for dynamic pruning.
	// When we can, remove this check
	if ((scanId - 1) >= m_static_prune_results.size())
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion,
				   GPOS_WSZ_LIT("dynamic pruning"));
	return m_static_prune_results[scanId - 1];
}
void
CContextDXLToPlStmt::SetStaticPruneResult(ULONG scanId,
										  List *static_prune_result)
{
	m_static_prune_results.resize(scanId);
	m_static_prune_results[scanId - 1] = static_prune_result;
}

// EOF
