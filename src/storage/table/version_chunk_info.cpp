#include "storage/table/version_chunk_info.hpp"

using namespace duckdb;
using namespace std;

VersionChunkInfo::VersionChunkInfo(VersionChunk &chunk, index_t start) :
	chunk(chunk), start(start) {

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

		vector<data_ptr_t> data_pointers;
		for (index_t i = 0; i < chunk.table.types.size(); i++) {
			data_pointers.push_back(chunk.GetPointerToRow(i, chunk.start + start + entry));
		}
		chunk.table.serializer.Deserialize(data_pointers, 0, tuple_data);
	}
	version_pointers[entry] = info->next;
	if (info->next) {
		info->next->prev = nullptr;
	}
}
