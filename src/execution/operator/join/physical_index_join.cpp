#include "duckdb/execution/operator/join/physical_index_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

namespace duckdb {

class IndexJoinOperatorState : public CachingOperatorState {
public:
	IndexJoinOperatorState(ClientContext &context, const PhysicalIndexJoin &op)
	    : probe_executor(context), arena_allocator(BufferAllocator::Get(context)), keys(STANDARD_VECTOR_SIZE) {
		auto &allocator = Allocator::Get(context);
		rhs_rows.resize(STANDARD_VECTOR_SIZE);
		result_sizes.resize(STANDARD_VECTOR_SIZE);

		join_keys.Initialize(allocator, op.condition_types);
		for (auto &cond : op.conditions) {
			probe_executor.AddExpression(*cond.left);
		}
		if (!op.fetch_types.empty()) {
			rhs_chunk.Initialize(allocator, op.fetch_types);
		}
		rhs_sel.Initialize(STANDARD_VECTOR_SIZE);
	}

	bool first_fetch = true;
	idx_t lhs_idx = 0;
	idx_t rhs_idx = 0;
	idx_t result_size = 0;
	vector<idx_t> result_sizes;
	DataChunk join_keys;
	DataChunk rhs_chunk;
	SelectionVector rhs_sel;

	//! Vector of rows that mush be fetched for every LHS key
	vector<vector<row_t>> rhs_rows;
	ExpressionExecutor probe_executor;

	ArenaAllocator arena_allocator;
	vector<ARTKey> keys;
	unique_ptr<ColumnFetchState> fetch_state;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, probe_executor, "probe_executor", 0);
	}
};

PhysicalIndexJoin::PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                     const vector<idx_t> &left_projection_map_p, vector<idx_t> right_projection_map_p,
                                     vector<column_t> column_ids_p, Index &index_p, bool lhs_first,
                                     idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::INDEX_JOIN, std::move(op.types), estimated_cardinality),
      left_projection_map(left_projection_map_p), right_projection_map(std::move(right_projection_map_p)),
      index(index_p), conditions(std::move(cond)), join_type(join_type), lhs_first(lhs_first) {
	D_ASSERT(right->type == PhysicalOperatorType::TABLE_SCAN);
	auto &tbl_scan = right->Cast<PhysicalTableScan>();
	column_ids = std::move(column_ids_p);
	children.push_back(std::move(left));
	children.push_back(std::move(right));
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}
	//! Only add to fetch_ids columns that are not indexed
	for (auto &index_id : index.column_ids) {
		index_ids.insert(index_id);
	}

	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column_id = column_ids[i];
		auto it = index_ids.find(column_id);
		if (it == index_ids.end()) {
			fetch_ids.push_back(column_id);
			if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
				fetch_types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				fetch_types.push_back(tbl_scan.returned_types[column_id]);
			}
		}
	}
	if (right_projection_map.empty()) {
		for (column_t i = 0; i < column_ids.size(); i++) {
			right_projection_map.push_back(i);
		}
	}
	if (left_projection_map.empty()) {
		for (column_t i = 0; i < children[0]->types.size(); i++) {
			left_projection_map.push_back(i);
		}
	}
}

unique_ptr<OperatorState> PhysicalIndexJoin::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<IndexJoinOperatorState>(context.client, *this);
}

void PhysicalIndexJoin::Output(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                               OperatorState &state_p) const {
	auto &phy_tbl_scan = children[1]->Cast<PhysicalTableScan>();
	auto &bind_tbl = phy_tbl_scan.bind_data->Cast<TableScanBindData>();
	auto &transaction = DuckTransaction::Get(context.client, bind_tbl.table.catalog);
	auto &state = state_p.Cast<IndexJoinOperatorState>();

	auto &tbl = bind_tbl.table.GetStorage();
	idx_t output_sel_idx = 0;
	vector<row_t> fetch_rows;

	while (output_sel_idx < STANDARD_VECTOR_SIZE && state.lhs_idx < input.size()) {
		if (state.rhs_idx < state.result_sizes[state.lhs_idx]) {
			state.rhs_sel.set_index(output_sel_idx++, state.lhs_idx);
			if (!fetch_types.empty()) {
				//! We need to collect the rows we want to fetch
				fetch_rows.push_back(state.rhs_rows[state.lhs_idx][state.rhs_idx]);
			}
			state.rhs_idx++;
		} else {
			//! We are done with the matches from this LHS Key
			state.rhs_idx = 0;
			state.lhs_idx++;
		}
	}
	//! Now we fetch the RHS data
	if (!fetch_types.empty()) {
		if (fetch_rows.empty()) {
			return;
		}
		state.rhs_chunk.Reset();
		state.fetch_state = make_uniq<ColumnFetchState>();
		Vector row_ids(LogicalType::ROW_TYPE, data_ptr_cast(&fetch_rows[0]));
		tbl.Fetch(transaction, state.rhs_chunk, fetch_ids, row_ids, output_sel_idx, *state.fetch_state);
	}

	//! Now we actually produce our result chunk
	idx_t left_offset = lhs_first ? 0 : right_projection_map.size();
	idx_t right_offset = lhs_first ? left_projection_map.size() : 0;
	idx_t rhs_column_idx = 0;
	for (idx_t i = 0; i < right_projection_map.size(); i++) {
		auto it = index_ids.find(column_ids[right_projection_map[i]]);
		if (it == index_ids.end()) {
			chunk.data[right_offset + i].Reference(state.rhs_chunk.data[rhs_column_idx++]);
		} else {
			chunk.data[right_offset + i].Slice(state.join_keys.data[0], state.rhs_sel, output_sel_idx);
		}
	}
	for (idx_t i = 0; i < left_projection_map.size(); i++) {
		chunk.data[left_offset + i].Slice(input.data[left_projection_map[i]], state.rhs_sel, output_sel_idx);
	}

	state.result_size = output_sel_idx;
	chunk.SetCardinality(state.result_size);
}

void PhysicalIndexJoin::GetRHSMatches(ExecutionContext &context, DataChunk &input, OperatorState &state_p) const {

	auto &state = state_p.Cast<IndexJoinOperatorState>();
	auto &art = index.Cast<ART>();

	// generate the keys for this chunk
	state.arena_allocator.Reset();
	ART::GenerateKeys(state.arena_allocator, state.join_keys, state.keys);

	for (idx_t i = 0; i < input.size(); i++) {
		state.rhs_rows[i].clear();
		if (!state.keys[i].Empty()) {
			if (fetch_types.empty()) {
				IndexLock lock;
				index.InitializeLock(lock);
				art.SearchEqualJoinNoFetch(state.keys[i], state.result_sizes[i]);
			} else {
				IndexLock lock;
				index.InitializeLock(lock);
				art.SearchEqual(state.keys[i], (idx_t)-1, state.rhs_rows[i]);
				state.result_sizes[i] = state.rhs_rows[i].size();
			}
		} else {
			//! This is null so no matches
			state.result_sizes[i] = 0;
		}
	}
	for (idx_t i = input.size(); i < STANDARD_VECTOR_SIZE; i++) {
		//! No LHS chunk value so result size is empty
		state.result_sizes[i] = 0;
	}
}

OperatorResultType PhysicalIndexJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                      GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<IndexJoinOperatorState>();

	state.result_size = 0;
	if (state.first_fetch) {
		state.probe_executor.Execute(input, state.join_keys);

		//! Fill Matches for the current LHS chunk
		GetRHSMatches(context, input, state_p);
		state.first_fetch = false;
	}
	//! Check if we need to get a new LHS chunk
	if (state.lhs_idx >= input.size()) {
		state.lhs_idx = 0;
		state.rhs_idx = 0;
		state.first_fetch = true;
		// reset the LHS chunk to reset the validity masks
		state.join_keys.Reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}
	//! Output vectors
	if (state.lhs_idx < input.size()) {
		Output(context, input, chunk, state_p);
	}
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalIndexJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	// index join: we only continue into the LHS
	// the right side is probed by the index join
	// so we don't need to do anything in the pipeline with this child
	meta_pipeline.GetState().AddPipelineOperator(current, *this);
	children[0]->BuildPipelines(current, meta_pipeline);
}

vector<const_reference<PhysicalOperator>> PhysicalIndexJoin::GetSources() const {
	return children[0]->GetSources();
}

} // namespace duckdb
