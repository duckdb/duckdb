#include "storage/table/version_chunk_info.hpp"

using namespace duckdb;
using namespace std;

VersionChunkInfo::VersionChunkInfo(VersionChunk &chunk, index_t start) : chunk(chunk), start(start) {
}

void VersionChunkInfo::Cleanup(VersionInfo *info) {
	index_t entry = info->entry;
	version_pointers[entry] = info->next;
	if (info->next) {
		info->next->prev = nullptr;
	}
}

void VersionChunkInfo::Undo(VersionInfo *info) {
	index_t entry = info->entry;
	assert(version_pointers[entry] == info);
	if (!info->tuple_data) {
		deleted[entry] = true;
	} else {
		// move data back to the original chunk
		deleted[entry] = false;
		auto tuple_data = info->tuple_data;
		auto row_id = info->GetRowId();
		for (index_t i = 0; i < chunk.table.types.size(); i++) {
			assert(chunk.columns[i].segment->segment_type == ColumnSegmentType::TRANSIENT);
			auto &transient = (TransientSegment &)*chunk.columns[i].segment;
			transient.Update(row_id, tuple_data);
			tuple_data += transient.type_size;
		}
	}
	version_pointers[entry] = info->next;
	if (info->next) {
		info->next->prev = nullptr;
	}
}
