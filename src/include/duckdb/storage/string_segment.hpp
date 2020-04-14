//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/string_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/uncompressed_segment.hpp"

namespace duckdb {
class OverflowStringWriter {
public:
	virtual ~OverflowStringWriter() {
	}

	virtual void WriteString(string_t string, block_id_t &result_block, int32_t &result_offset) = 0;
};

struct StringBlock {
	block_id_t block_id;
	idx_t offset;
	idx_t size;
	unique_ptr<StringBlock> next;
};

struct string_location_t {
	string_location_t(block_id_t block_id, int32_t offset) : block_id(block_id), offset(offset) {
	}

	string_location_t() {
	}

	bool IsValid() {
		return offset < Storage::BLOCK_SIZE && (block_id == INVALID_BLOCK || block_id >= MAXIMUM_BLOCK);
	}

	block_id_t block_id;
	int32_t offset;
};

struct StringUpdateInfo {
	sel_t count;
	sel_t ids[STANDARD_VECTOR_SIZE];
	block_id_t block_ids[STANDARD_VECTOR_SIZE];
	int32_t offsets[STANDARD_VECTOR_SIZE];
};

typedef unique_ptr<StringUpdateInfo> string_update_info_t;

class StringSegment : public UncompressedSegment {
public:
	StringSegment(BufferManager &manager, idx_t row_start, block_id_t block_id = INVALID_BLOCK);

	~StringSegment() override;

	//! The current dictionary offset
	idx_t dictionary_offset;
	//! The string block holding strings that do not fit in the main block
	//! FIXME: this should be replaced by a heap that also allows freeing of unused strings
	unique_ptr<StringBlock> head;
	//! Blocks that hold string updates (if any)
	unique_ptr<string_update_info_t[]> string_updates;
	//! Overflow string writer (if any), if not set overflow strings will be written to memory blocks
	unique_ptr<OverflowStringWriter> overflow_writer;

public:
	void InitializeScan(ColumnScanState &state) override;

	//! Fetch a single value and append it to the vector
	void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
	              idx_t result_idx) override;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	idx_t Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count) override;

	//! Rollback a previous update
	void RollbackUpdate(UpdateInfo *info) override;

protected:
	void Update(ColumnData &column_data, SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids,
	            idx_t count, idx_t vector_index, idx_t vector_offset, UpdateInfo *node) override;

	void Select(ColumnScanState &state, Vector &result, SelectionVector &sel, SelectionVector &valid_sel,
	            idx_t &approved_tuple_count, idx_t count, bool use_valid_sel,
	            vector<TableFilter> &tableFilter) override;

	void FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) override;

	void FetchUpdateData(ColumnScanState &state, Transaction &transaction, UpdateInfo *versions,
	                     Vector &result) override;

	void FilterFetchBaseData(ColumnScanState &state, Vector &result, SelectionVector &sel,
	                         idx_t &approved_tuple_count) override;

private:
	void AppendData(SegmentStatistics &stats, data_ptr_t target, data_ptr_t end, idx_t target_offset, Vector &source,
	                idx_t offset, idx_t count);

	//! Fetch all the strings of a vector from the base table and place their locations in the result vector
	void FetchBaseData(ColumnScanState &state, data_ptr_t base_data, idx_t vector_index, Vector &result, idx_t count);

	string_location_t FetchStringLocation(data_ptr_t baseptr, int32_t dict_offset);

	string_t FetchString(buffer_handle_set_t &handles, data_ptr_t baseptr, string_location_t location);

	//! Fetch a single string from the dictionary and returns it, potentially pins a buffer manager page and adds it to
	//! the set of pinned pages
	string_t FetchStringFromDict(buffer_handle_set_t &handles, data_ptr_t baseptr, int32_t dict_offset);

	//! Fetch string locations for a subset of the strings
	void FetchStringLocations(data_ptr_t baseptr, row_t *ids, idx_t vector_index, idx_t vector_offset, idx_t count,
	                          string_location_t result[]);

	void WriteString(string_t string, block_id_t &result_block, int32_t &result_offset);

	string_t ReadString(buffer_handle_set_t &handles, block_id_t block, int32_t offset);

	string_t ReadString(data_ptr_t target, int32_t offset);

	void WriteStringMemory(string_t string, block_id_t &result_block, int32_t &result_offset);

	void WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset);

	void ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset);

	//! Expand the string segment, adding an additional maximum vector to the segment
	void ExpandStringSegment(data_ptr_t baseptr);

	string_update_info_t CreateStringUpdate(SegmentStatistics &stats, Vector &update, row_t *ids, idx_t count,
	                                        idx_t vector_offset);

	string_update_info_t MergeStringUpdate(SegmentStatistics &stats, Vector &update, row_t *ids, idx_t count,
	                                       idx_t vector_offset, StringUpdateInfo &update_info);

	void MergeUpdateInfo(UpdateInfo *node, row_t *ids, idx_t update_count, idx_t vector_offset,
	                     string_location_t string_locations[], nullmask_t original_nullmask);

	//! The amount of bytes remaining to store in the block
	idx_t RemainingSpace() {
		return Storage::BLOCK_SIZE - dictionary_offset - max_vector_count * vector_size;
	}

	template <class OP>
	void Select_String(buffer_handle_set_t &handles, Vector &result, data_ptr_t baseptr, int32_t *dict_offset,
	                   SelectionVector &sel, SelectionVector &valid_sel, const string &constant,
	                   idx_t &approved_tuple_count, unsigned long size, nullmask_t *source_nullmask, bool use_valid_sel,
	                   size_t vector_index) {
	    	result.vector_type = VectorType::FLAT_VECTOR;

		auto result_data = FlatVector::GetData<string_t>(result);
		FlatVector::SetNullmask(result, *source_nullmask);
		string_t data_str;
		if (approved_tuple_count == 0) {
			idx_t update_idx = 0;

			//! This is the first filter we are applying, we need to scan the full vector
			for (idx_t i = 0; i < size; i++) {
				idx_t src_idx;
				if (use_valid_sel) {
					src_idx = valid_sel.get_index(i);
				} else {
					src_idx = i;
				}

				if (string_updates && string_updates[vector_index]) {
					auto &info = *string_updates[vector_index];
					if (update_idx < info.count && info.ids[update_idx] == src_idx) {
						data_str = ReadString(handles, info.block_ids[update_idx], info.offsets[update_idx]);
						update_idx++;
					} else {
						data_str = FetchStringFromDict(handles, baseptr, dict_offset[src_idx]);
					}
				} else {
					data_str = FetchStringFromDict(handles, baseptr, dict_offset[src_idx]);
				}

				if (!(*source_nullmask)[src_idx] && OP::Operation(data_str.GetString(), constant)) {
					result_data[src_idx] = data_str;
					sel.set_index(approved_tuple_count++, src_idx);
				}
			}
		} else {
			//! We already applied at least one filter, we only need to check the selection vector
			idx_t update_idx = 0;

			for (idx_t i = 0; i < approved_tuple_count; i++) {
				idx_t src_idx = sel.get_index(i);
				if (string_updates && string_updates[vector_index]) {
					auto &info = *string_updates[vector_index];

					if (update_idx < info.count && info.ids[update_idx] == i) {
						data_str =
						    ReadString(handles, info.block_ids[update_idx], info.offsets[update_idx]).GetString();
						update_idx++;
					} else {
						data_str = FetchStringFromDict(handles, baseptr, dict_offset[src_idx]).GetString();
					}
				} else {
					data_str = FetchStringFromDict(handles, baseptr, dict_offset[src_idx]).GetString();
				}
				if (!(*source_nullmask)[src_idx] && !OP::Operation(data_str.GetString(), constant)) {
					sel.swap(i, approved_tuple_count - 1);
					approved_tuple_count--;
					i--;
				} else {
					result_data[src_idx] = data_str;
				}
			}
		}
	}

	template <class OPL, class OPR>
	void Select_String_Between(buffer_handle_set_t &handles, Vector &result, data_ptr_t baseptr, int32_t *dict_offset,
	                           SelectionVector &sel, SelectionVector &valid_sel, string constantLeft,
	                           string constantRight, idx_t &approved_tuple_count, nullmask_t *source_nullmask,
	                           unsigned long size, bool use_valid_sel, size_t vector_index) {
	    	result.vector_type = VectorType::FLAT_VECTOR;

		auto result_data = FlatVector::GetData<string_t>(result);
		FlatVector::SetNullmask(result, *source_nullmask);
		string_t data_str;
		if (approved_tuple_count == 0) {
			//! This is the first filter we are applying, we need to scan the full vector
			idx_t update_idx = 0;

			//! This is the first filter we are applying, we need to scan the full vector
			for (idx_t i = 0; i < size; i++) {
				idx_t src_idx;
				if (use_valid_sel) {
					src_idx = valid_sel.get_index(i);
				} else {
					src_idx = i;
				}

				if (string_updates && string_updates[vector_index]) {
					auto &info = *string_updates[vector_index];
					if (update_idx < info.count && info.ids[update_idx] == i) {
						data_str = ReadString(handles, info.block_ids[update_idx], info.offsets[update_idx]);
						update_idx++;
					} else {
						data_str = FetchStringFromDict(handles, baseptr, dict_offset[src_idx]);
					}
				} else {
					data_str = FetchStringFromDict(handles, baseptr, dict_offset[src_idx]);
				}
				if (!(*source_nullmask)[src_idx] && OPL::Operation(data_str.GetString(), constantLeft) &&
				    OPR::Operation(data_str.GetString(), constantRight)) {
					result_data[src_idx] = data_str;
					sel.set_index(approved_tuple_count++, src_idx);
				}
			}

		} else {
			idx_t update_idx = 0;

			//! We already applied at least one filter, we only need to check the selection vector
			for (idx_t i = 0; i < approved_tuple_count; i++) {
				idx_t src_idx = sel.get_index(i);
				if (string_updates && string_updates[vector_index]) {
					auto &info = *string_updates[vector_index];

					if (update_idx < info.count && info.ids[update_idx] == i) {
						data_str = ReadString(handles, info.block_ids[update_idx], info.offsets[update_idx]);
						update_idx++;
					} else {
						data_str = FetchStringFromDict(handles, baseptr, dict_offset[src_idx]);
					}
				} else {
					data_str = FetchStringFromDict(handles, baseptr, dict_offset[src_idx]);
				}
				if (!(*source_nullmask)[src_idx] && !(OPL::Operation(data_str.GetString(), constantLeft) &&
				                                      OPR::Operation(data_str.GetString(), constantRight))) {
					sel.swap(i, approved_tuple_count - 1);
					approved_tuple_count--;
					i--;
				} else {
					result_data[src_idx] = data_str;
				}
			}
		}
	}

private:
	//! The max string size that is allowed within a block. Strings bigger than this will be labeled as a BIG STRING and
	//! offloaded to the overflow blocks.
	static constexpr uint16_t STRING_BLOCK_LIMIT = 4096;
	//! Marker used in length field to indicate the presence of a big string
	static constexpr uint16_t BIG_STRING_MARKER = (uint16_t)-1;
	//! Base size of big string marker (block id + offset)
	static constexpr idx_t BIG_STRING_MARKER_BASE_SIZE = sizeof(block_id_t) + sizeof(int32_t);
	//! The marker size of the big string
	static constexpr idx_t BIG_STRING_MARKER_SIZE = BIG_STRING_MARKER_BASE_SIZE + sizeof(uint16_t);
};

} // namespace duckdb
