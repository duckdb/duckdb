#include "duckdb/common/types/chunk_collection.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

ChunkCollection::ChunkCollection(Allocator &allocator) : allocator(allocator), count(0) {
}

ChunkCollection::ChunkCollection(ClientContext &context) : ChunkCollection(Allocator::Get(context)) {
}

void ChunkCollection::Verify() {
#ifdef DEBUG
	for (auto &chunk : chunks) {
		chunk->Verify();
	}
#endif
}

void ChunkCollection::Append(ChunkCollection &other) {
	for (auto &chunk : other.chunks) {
		Append(*chunk);
	}
}

void ChunkCollection::Merge(ChunkCollection &other) {
	if (other.count == 0) {
		return;
	}
	if (count == 0) {
		chunks = std::move(other.chunks);
		types = std::move(other.types);
		count = other.count;
		return;
	}
	unique_ptr<DataChunk> old_back;
	if (!chunks.empty() && chunks.back()->size() != STANDARD_VECTOR_SIZE) {
		old_back = std::move(chunks.back());
		chunks.pop_back();
		count -= old_back->size();
	}
	for (auto &chunk : other.chunks) {
		chunks.push_back(std::move(chunk));
	}
	count += other.count;
	if (old_back) {
		Append(*old_back);
	}
	Verify();
}

void ChunkCollection::Append(DataChunk &new_chunk) {
	if (new_chunk.size() == 0) {
		return;
	}
	new_chunk.Verify();

	// we have to ensure that every chunk in the ChunkCollection is completely
	// filled, otherwise our O(1) lookup in GetValue and SetValue does not work
	// first fill the latest chunk, if it exists
	count += new_chunk.size();

	idx_t remaining_data = new_chunk.size();
	idx_t offset = 0;
	if (chunks.empty()) {
		// first chunk
		types = new_chunk.GetTypes();
	} else {
		// the types of the new chunk should match the types of the previous one
		D_ASSERT(types.size() == new_chunk.ColumnCount());
		auto new_types = new_chunk.GetTypes();
		for (idx_t i = 0; i < types.size(); i++) {
			if (new_types[i] != types[i]) {
				throw TypeMismatchException(new_types[i], types[i], "Type mismatch when combining rows");
			}
			if (types[i].InternalType() == PhysicalType::LIST) {
				// need to check all the chunks because they can have only-null list entries
				for (auto &chunk : chunks) {
					auto &chunk_vec = chunk->data[i];
					auto &new_vec = new_chunk.data[i];
					auto &chunk_type = chunk_vec.GetType();
					auto &new_type = new_vec.GetType();
					if (chunk_type != new_type) {
						throw TypeMismatchException(chunk_type, new_type, "Type mismatch when combining lists");
					}
				}
			}
			// TODO check structs, too
		}

		// first append data to the current chunk
		DataChunk &last_chunk = *chunks.back();
		idx_t added_data = MinValue<idx_t>(remaining_data, STANDARD_VECTOR_SIZE - last_chunk.size());
		if (added_data > 0) {
			// copy <added_data> elements to the last chunk
			new_chunk.Flatten();
			// have to be careful here: setting the cardinality without calling normalify can cause incorrect partial
			// decompression
			idx_t old_count = new_chunk.size();
			new_chunk.SetCardinality(added_data);

			last_chunk.Append(new_chunk);
			remaining_data -= added_data;
			// reset the chunk to the old data
			new_chunk.SetCardinality(old_count);
			offset = added_data;
		}
	}

	if (remaining_data > 0) {
		// create a new chunk and fill it with the remainder
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(allocator, types);
		new_chunk.Copy(*chunk, offset);
		chunks.push_back(std::move(chunk));
	}
}

void ChunkCollection::Append(unique_ptr<DataChunk> new_chunk) {
	if (types.empty()) {
		types = new_chunk->GetTypes();
	}
	D_ASSERT(types == new_chunk->GetTypes());
	count += new_chunk->size();
	chunks.push_back(std::move(new_chunk));
}

void ChunkCollection::Fuse(ChunkCollection &other) {
	if (count == 0) {
		chunks.reserve(other.ChunkCount());
		for (idx_t chunk_idx = 0; chunk_idx < other.ChunkCount(); ++chunk_idx) {
			auto lhs = make_uniq<DataChunk>();
			auto &rhs = other.GetChunk(chunk_idx);
			lhs->data.reserve(rhs.data.size());
			for (auto &v : rhs.data) {
				lhs->data.emplace_back(v);
			}
			lhs->SetCardinality(rhs.size());
			chunks.push_back(std::move(lhs));
		}
		count = other.Count();
	} else {
		D_ASSERT(this->ChunkCount() == other.ChunkCount());
		for (idx_t chunk_idx = 0; chunk_idx < ChunkCount(); ++chunk_idx) {
			auto &lhs = this->GetChunk(chunk_idx);
			auto &rhs = other.GetChunk(chunk_idx);
			D_ASSERT(lhs.size() == rhs.size());
			for (auto &v : rhs.data) {
				lhs.data.emplace_back(v);
			}
		}
	}
	types.insert(types.end(), other.types.begin(), other.types.end());
}

Value ChunkCollection::GetValue(idx_t column, idx_t index) {
	return chunks[LocateChunk(index)]->GetValue(column, index % STANDARD_VECTOR_SIZE);
}

void ChunkCollection::SetValue(idx_t column, idx_t index, const Value &value) {
	chunks[LocateChunk(index)]->SetValue(column, index % STANDARD_VECTOR_SIZE, value);
}

void ChunkCollection::CopyCell(idx_t column, idx_t index, Vector &target, idx_t target_offset) {
	auto &chunk = GetChunkForRow(index);
	auto &source = chunk.data[column];
	const auto source_offset = index % STANDARD_VECTOR_SIZE;
	VectorOperations::Copy(source, target, source_offset + 1, source_offset, target_offset);
}

string ChunkCollection::ToString() const {
	return chunks.empty() ? "ChunkCollection [ 0 ]"
	                      : "ChunkCollection [ " + std::to_string(count) + " ]: \n" + chunks[0]->ToString();
}

void ChunkCollection::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb
