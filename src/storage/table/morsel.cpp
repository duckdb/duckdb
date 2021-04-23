#include "duckdb/storage/table/morsel.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

constexpr const idx_t Morsel::MORSEL_VECTOR_COUNT;
constexpr const idx_t Morsel::MORSEL_SIZE;
constexpr const idx_t Morsel::MORSEL_LAYER_COUNT;
constexpr const idx_t Morsel::MORSEL_LAYER_SIZE;

Morsel::Morsel(DatabaseInstance &db, DataTableInfo &table_info, idx_t start, idx_t count) :
    SegmentBase(start, count), db(db), table_info(table_info) {
}

Morsel::~Morsel() {}


void Morsel::InitializeScan(MorselScanState &state) {
	auto &column_ids = state.parent.column_ids;
	state.morsel = this;
	state.vector_index = 0;
	state.max_row = this->count;
	state.column_scans = unique_ptr<ColumnScanState[]>(new ColumnScanState[column_ids.size()]);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			columns[column]->InitializeScan(state.column_scans[i]);
		} else {
			state.column_scans[i].current = nullptr;
		}
	}
}

void Morsel::Scan(Transaction &transaction, MorselScanState &state, DataChunk &result) {
	auto &table_filters = state.parent.table_filters;
	auto &column_ids = state.parent.column_ids;
	auto &adaptive_filter = state.parent.adaptive_filter;
	while(true) {
		if (state.vector_index * STANDARD_VECTOR_SIZE >= state.max_row) {
			// exceeded the amount of rows to scan
			return;
		}
		idx_t current_row = state.vector_index * STANDARD_VECTOR_SIZE;
		auto max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.max_row - current_row);
		// idx_t vector_offset = (current_row - state.base_row) / STANDARD_VECTOR_SIZE;
		// //! first check the zonemap if we have to scan this partition
		// if (!CheckZonemap(state, column_ids, state.table_filters, current_row)) {
		// 	return true;
		// }
		// // second, scan the version chunk manager to figure out which tuples to load for this transaction
		SelectionVector valid_sel(STANDARD_VECTOR_SIZE);
		// while (vector_offset >= Morsel::MORSEL_VECTOR_COUNT) {
		// 	state.version_info = (MorselInfo *)state.version_info->next.get();
		// 	state.base_row += Morsel::MORSEL_SIZE;
		// 	vector_offset -= Morsel::MORSEL_VECTOR_COUNT;
		// }
		idx_t count = state.morsel->GetSelVector(transaction, state.vector_index, valid_sel, max_count);
		if (count == 0) {
			// nothing to scan for this vector, skip the entire vector
			state.vector_index++;
			continue;
		}
		idx_t approved_tuple_count = count;
		if (count == max_count && !table_filters) {
			//! If we don't have any deleted tuples or filters we can just run a regular scan
			for (idx_t i = 0; i < column_ids.size(); i++) {
				auto column = column_ids[i];
				if (column == COLUMN_IDENTIFIER_ROW_ID) {
					// scan row id
					D_ASSERT(result.data[i].GetType().InternalType() == ROW_TYPE);
					result.data[i].Sequence(current_row, 1);
				} else {
					columns[column]->Scan(state.column_scans[i], result.data[i]);
				}
			}
		} else {
			SelectionVector sel;

			if (count != max_count) {
				sel.Initialize(valid_sel);
			} else {
				sel.Initialize(FlatVector::INCREMENTAL_SELECTION_VECTOR);
			}
			//! First, we scan the columns with filters, fetch their data and generate a selection vector.
			//! get runtime statistics
			auto start_time = high_resolution_clock::now();
			if (table_filters) {
				for (idx_t i = 0; i < table_filters->filters.size(); i++) {
					auto tf_idx = adaptive_filter->permutation[i];
					auto col_idx = column_ids[tf_idx];
					columns[col_idx]->Select(state.column_scans[tf_idx], result.data[tf_idx], sel,
											approved_tuple_count, table_filters->filters[tf_idx]);
				}
				for (auto &table_filter : table_filters->filters) {
					result.data[table_filter.first].Slice(sel, approved_tuple_count);
				}
			}
			if (approved_tuple_count == 0) {
				result.Reset();
				state.vector_index++;
				continue;
			}

			//! Now we use the selection vector to fetch data for the other columns.
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (!table_filters || table_filters->filters.find(i) == table_filters->filters.end()) {
					auto column = column_ids[i];
					if (column == COLUMN_IDENTIFIER_ROW_ID) {
						D_ASSERT(result.data[i].GetType().InternalType() == PhysicalType::INT64);
						result.data[i].SetVectorType(VectorType::FLAT_VECTOR);
						auto result_data = (int64_t *)FlatVector::GetData(result.data[i]);
						for (size_t sel_idx = 0; sel_idx < approved_tuple_count; sel_idx++) {
							result_data[sel_idx] = current_row + sel.get_index(sel_idx);
						}
					} else {
						columns[column]->FilterScan(state.column_scans[i], result.data[i], sel,
													approved_tuple_count);
					}
				}
			}
			auto end_time = high_resolution_clock::now();
			if (adaptive_filter && table_filters->filters.size() > 1) {
				adaptive_filter->AdaptRuntimeStatistics(
					duration_cast<duration<double>>(end_time - start_time).count());
			}
		}
		D_ASSERT(approved_tuple_count > 0);
		result.SetCardinality(approved_tuple_count);
		state.vector_index++;
		break;
	}
}


ChunkInfo *Morsel::GetChunkInfo(idx_t vector_idx) {
	if (!version_info) {
		return nullptr;
	}
	return version_info->info[vector_idx].get();
}

idx_t Morsel::GetSelVector(Transaction &transaction, idx_t vector_idx, SelectionVector &sel_vector,
                               idx_t max_count) {
	lock_guard<mutex> lock(morsel_lock);

	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetSelVector(transaction, sel_vector, max_count);
}

bool Morsel::Fetch(Transaction &transaction, idx_t row) {
	D_ASSERT(row < Morsel::MORSEL_SIZE);
	lock_guard<mutex> lock(morsel_lock);

	idx_t vector_index = row / STANDARD_VECTOR_SIZE;
	auto info = GetChunkInfo(vector_index);
	if (!info) {
		return true;
	}
	return info->Fetch(transaction, row - vector_index * STANDARD_VECTOR_SIZE);
}

void Morsel::AppendVersionInfo(Transaction &transaction, idx_t morsel_start, idx_t count, transaction_t commit_id) {
	idx_t morsel_end = morsel_start + count;
	lock_guard<mutex> lock(morsel_lock);

	this->count += count;
	D_ASSERT(this->count <= Morsel::MORSEL_SIZE);

	// create the version_info if it doesn't exist yet
	if (!version_info) {
		version_info = make_unique<VersionNode>();
	}
	idx_t start_vector_idx = morsel_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (morsel_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t start = vector_idx == start_vector_idx ? morsel_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? morsel_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;
		if (start == 0 && end == STANDARD_VECTOR_SIZE) {
			// entire vector is encapsulated by append: append a single constant
			auto constant_info = make_unique<ChunkConstantInfo>(this->start + vector_idx * STANDARD_VECTOR_SIZE, *this);
			constant_info->insert_id = commit_id;
			constant_info->delete_id = NOT_DELETED_ID;
			version_info->info[vector_idx] = move(constant_info);
		} else {
			// part of a vector is encapsulated: append to that part
			ChunkVectorInfo *info;
			if (!version_info->info[vector_idx]) {
				// first time appending to this vector: create new info
				auto insert_info = make_unique<ChunkVectorInfo>(this->start + vector_idx * STANDARD_VECTOR_SIZE, *this);
				info = insert_info.get();
				version_info->info[vector_idx] = move(insert_info);
			} else {
				D_ASSERT(version_info->info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
				// use existing vector
				info = (ChunkVectorInfo *)version_info->info[vector_idx].get();
			}
			info->Append(start, end, commit_id);
		}
	}
}

void Morsel::CommitAppend(transaction_t commit_id, idx_t morsel_start, idx_t count) {
	D_ASSERT(version_info.get());
	idx_t morsel_end = morsel_start + count;
	lock_guard<mutex> lock(morsel_lock);

	idx_t start_vector_idx = morsel_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (morsel_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t start = vector_idx == start_vector_idx ? morsel_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? morsel_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;

		auto info = version_info->info[vector_idx].get();
		info->CommitAppend(commit_id, start, end);
	}
}

void Morsel::RevertAppend(idx_t morsel_start) {
	if (!version_info) {
		return;
	}
	idx_t start_row = morsel_start - this->start;
	idx_t start_vector_idx = (start_row + (STANDARD_VECTOR_SIZE - 1)) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx < Morsel::MORSEL_VECTOR_COUNT; vector_idx++) {
		version_info->info[vector_idx].reset();
	}
}

void Morsel::InitializeAppend(Transaction &transaction, MorselAppendState &append_state, idx_t remaining_append_count) {
	append_state.morsel = this;
	append_state.offset_in_morsel = this->count;
	// for each column, initialize the append state
	append_state.states = unique_ptr<ColumnAppendState[]>(new ColumnAppendState[columns.size()]);
	for (idx_t i = 0; i < columns.size(); i++) {
		columns[i]->InitializeAppend(append_state.states[i]);
	}
	// append the version info for this morsel
	idx_t append_count = MinValue<idx_t>(remaining_append_count, Morsel::MORSEL_SIZE - this->count);
	AppendVersionInfo(transaction, this->count, append_count, transaction.transaction_id);
}

void Morsel::Append(MorselAppendState &state, DataChunk &chunk, idx_t append_count) {
	// append to the current morsel
	for (idx_t i = 0; i < columns.size(); i++) {
		columns[i]->Append(state.states[i], chunk.data[i], append_count);
	}
	state.offset_in_morsel += append_count;
}

class VersionDeleteState {
public:
	VersionDeleteState(Morsel &info, Transaction &transaction, DataTable *table, idx_t base_row)
	    : info(info), transaction(transaction), table(table), current_info(nullptr), current_chunk(INVALID_INDEX),
	      count(0), base_row(base_row) {
	}

	Morsel &info;
	Transaction &transaction;
	DataTable *table;
	ChunkVectorInfo *current_info;
	idx_t current_chunk;
	row_t rows[STANDARD_VECTOR_SIZE];
	idx_t count;
	idx_t base_row;
	idx_t chunk_row;

public:
	void Delete(row_t row_id);
	void Flush();
};

void Morsel::Delete(Transaction &transaction, DataTable *table, Vector &row_ids, idx_t count) {
	lock_guard<mutex> lock(morsel_lock);
	VersionDeleteState del_state(*this, transaction, table, this->start);

	VectorData rdata;
	row_ids.Orrify(count, rdata);
	// obtain a write lock
	auto ids = (row_t *)rdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto ridx = rdata.sel->get_index(i);
		del_state.Delete(ids[ridx] - this->start);
	}
	del_state.Flush();
}

void VersionDeleteState::Delete(row_t row_id) {
	idx_t vector_idx = row_id / STANDARD_VECTOR_SIZE;
	idx_t idx_in_vector = row_id - vector_idx * STANDARD_VECTOR_SIZE;
	if (current_chunk != vector_idx) {
		Flush();

		if (!info.version_info) {
			info.version_info = make_unique<VersionNode>();
		}

		if (!info.version_info->info[vector_idx]) {
			// no info yet: create it
			info.version_info->info[vector_idx] =
			    make_unique<ChunkVectorInfo>(info.start + vector_idx * STANDARD_VECTOR_SIZE, info);
		} else if (info.version_info->info[vector_idx]->type == ChunkInfoType::CONSTANT_INFO) {
			auto &constant = (ChunkConstantInfo &)*info.version_info->info[vector_idx];
			// info exists but it's a constant info: convert to a vector info
			auto new_info = make_unique<ChunkVectorInfo>(info.start + vector_idx * STANDARD_VECTOR_SIZE, info);
			new_info->insert_id = constant.insert_id;
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				new_info->inserted[i] = constant.insert_id;
			}
			info.version_info->info[vector_idx] = move(new_info);
		}
		D_ASSERT(info.version_info->info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
		current_info = (ChunkVectorInfo *)info.version_info->info[vector_idx].get();
		current_chunk = vector_idx;
		chunk_row = vector_idx * STANDARD_VECTOR_SIZE;
	}
	rows[count++] = idx_in_vector;
}

void VersionDeleteState::Flush() {
	if (count == 0) {
		return;
	}
	// delete in the current info
	current_info->Delete(transaction, rows, count);
	// now push the delete into the undo buffer
	transaction.PushDelete(table, current_info, rows, count, base_row + chunk_row);
	count = 0;
}

} // namespace duckdb
