#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/leaf_segment.hpp"

namespace duckdb {

void Leaf::Free(ART &art, ARTNode &node) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());

	auto leaf = node.Get<Leaf>(art);

	// delete all leaf segments
	if (!leaf->IsInlined()) {
		auto position = leaf->row_ids.position;
		while (position != DConstants::INVALID_INDEX) {
			auto next_position = LeafSegment::Get(art, position)->next;
			art.DecreaseMemorySize(sizeof(LeafSegment));
			LeafSegment::Free(art, position);
			position = next_position;
		}
	}

	art.DecreaseMemorySize(sizeof(Leaf));
}

Leaf *Leaf::Initialize(ART &art, const ARTNode &node, const Key &key, const uint32_t &depth, const row_t &row_id) {

	auto leaf = node.Get<Leaf>(art);
	art.IncreaseMemorySize(sizeof(Leaf));

	// set the fields of the leaf
	leaf->count = 1;
	leaf->row_ids.inlined = row_id;

	// initialize the prefix
	D_ASSERT(key.len >= depth);
	leaf->prefix.Initialize(art, key, depth, key.len - depth);

	return leaf;
}

Leaf *Leaf::Initialize(ART &art, const ARTNode &node, const Key &key, const uint32_t &depth, const row_t *row_ids,
                       const idx_t &count) {

	auto leaf = node.Get<Leaf>(art);
	art.IncreaseMemorySize(sizeof(Leaf));

	// inlined leaf
	D_ASSERT(count >= 1);
	if (count == 1) {
		return Leaf::Initialize(art, node, key, depth, row_ids[0]);
	}

	// set the fields of the leaf
	leaf->count = 0;

	// copy the row IDs
	leaf->row_ids.position = LeafSegment::New(art);
	auto segment = LeafSegment::Initialize(art, leaf->row_ids.position);
	for (idx_t i = 0; i < count; i++) {
		segment = segment->Append(art, leaf->count, row_ids[i]);
	}

	// set the prefix
	D_ASSERT(key.len >= depth);
	leaf->prefix.Initialize(art, key, depth, key.len - depth);
	return leaf;
}

void Leaf::Vacuum(ART &art) {

	if (IsInlined()) {
		return;
	}

	// first position has special treatment because we don't obtain it from a leaf segment
	D_ASSERT(art.nodes.find(ARTNodeType::LEAF_SEGMENT) != art.nodes.end());
	auto &allocator = art.nodes.at(ARTNodeType::LEAF_SEGMENT);
	if (allocator.NeedsVacuum(row_ids.position)) {
		row_ids.position = allocator.Vacuum(row_ids.position);
	}

	auto position = row_ids.position;
	while (position != DConstants::INVALID_INDEX) {
		auto segment = LeafSegment::Get(art, position);
		if (segment->next != DConstants::INVALID_INDEX && allocator.NeedsVacuum(segment->next)) {
			segment->next = allocator.Vacuum(segment->next);
		}
		position = segment->next;
	}
}

void Leaf::InitializeMerge(ART &art, const idx_t &buffer_count) {

	if (IsInlined()) {
		return;
	}

	auto position = row_ids.position;
	D_ASSERT((row_ids.position & 0xffff0000) == ((row_ids.position + buffer_count) & 0xffff0000));
	row_ids.position += buffer_count;

	while (position != DConstants::INVALID_INDEX) {
		auto segment = LeafSegment::Get(art, position);
		position = segment->next;
		D_ASSERT((segment->next & 0xffff0000) == ((segment->next + buffer_count) & 0xffff0000));
		segment->next += buffer_count;
	}
}

void Leaf::Merge(ART &art, ARTNode &other) {

	auto other_leaf = other.Get<Leaf>(art);

	// copy inlined row ID
	if (other_leaf->IsInlined()) {
		Insert(art, other_leaf->row_ids.inlined);
		ARTNode::Free(art, other);
		return;
	}

	// get the first segment to copy to
	LeafSegment *segment;
	if (IsInlined()) {
		// row ID was inlined, move to a new segment
		auto position = LeafSegment::New(art);
		segment = LeafSegment::Initialize(art, position);
		D_ASSERT(ARTNode::LEAF_SEGMENT_SIZE >= 1);
		segment->row_ids[0] = row_ids.inlined;
		row_ids.position = position;
	} else {
		// get the tail of the segments of this leaf
		segment = LeafSegment::Get(art, row_ids.position)->GetTail(art);
	}

	// initialize loop variables
	auto position = other_leaf->row_ids.position;
	auto remaining = other_leaf->count;

	// copy row IDs
	while (position != DConstants::INVALID_INDEX) {
		auto other_segment = LeafSegment::Get(art, position);
		auto copy_count = ARTNode::LEAF_SEGMENT_SIZE < remaining ? ARTNode::LEAF_SEGMENT_SIZE : remaining;

		// adjust the loop variables
		position = other_segment->next;
		remaining -= copy_count;

		// now copy the data
		for (idx_t i = 0; i < copy_count; i++) {
			segment = segment->Append(art, count, other_segment->row_ids[i]);
		}
	}
	D_ASSERT(remaining == 0);

	ARTNode::Free(art, other);
}

void Leaf::Insert(ART &art, const row_t &row_id) {

	if (count == 0) {
		row_ids.inlined = row_id;
		count++;
		return;
	}

	if (count == 1) {
		MoveInlinedToSegment(art);
	}

	// append to the tail
	auto first_segment = LeafSegment::Get(art, row_ids.position);
	first_segment->GetTail(art)->Append(art, count, row_id);
}

void Leaf::Remove(ART &art, const row_t &row_id) {

	if (count == 0) {
		return;
	}

	if (IsInlined()) {
		if (row_ids.inlined == row_id) {
			count--;
		}
		return;
	}

	// possibly inline the row ID
	if (count == 2) {
		auto segment = LeafSegment::Get(art, row_ids.position);
		if (segment->row_ids[0] != row_id && segment->row_ids[1] != row_id) {
			return;
		}

		auto temp_row_id = segment->row_ids[0] == row_id ? segment->row_ids[1] : segment->row_ids[0];

		art.DecreaseMemorySize(sizeof(LeafSegment));
		LeafSegment::Free(art, row_ids.position);
		row_ids.inlined = temp_row_id;
		count--;
		return;
	}

	// find the row ID, and the segment containing that row ID
	auto position = row_ids.position;
	auto row_id_position = FindRowId(art, position, row_id);
	if (row_id_position == DConstants::INVALID_INDEX) {
		return;
	}

	// iterate all remaining segments and move the row IDs one field to the left
	LeafSegment *prev_segment = nullptr;
	while (position != DConstants::INVALID_INDEX) {

		auto segment = LeafSegment::Get(art, position);
		if (prev_segment) {
			// this segment has at least one element, and we need to copy it into the previous segment
			prev_segment->row_ids[ARTNode::LEAF_SEGMENT_SIZE - 1] = segment->row_ids[0];
		}

		// move the data
		auto start = row_id_position % ARTNode::LEAF_SEGMENT_SIZE;
		auto len = std::min(count - row_id_position + 1, (idx_t)ARTNode::LEAF_SEGMENT_SIZE);
		memmove(segment->row_ids + start, segment->row_ids + start + 1, len);

		// adjust loop variables
		row_id_position += len;
		prev_segment = segment;
		position = segment->next;
	}

	// true, if we need to delete the last segment
	if (count % ARTNode::LEAF_SEGMENT_SIZE == 1) {
		position = row_ids.position;
		while (position != DConstants::INVALID_INDEX) {

			// get the segment succeeding the current segment
			auto segment = LeafSegment::Get(art, position);
			D_ASSERT(segment->next != DConstants::INVALID_INDEX);
			auto next_segment = LeafSegment::Get(art, segment->next);

			// the segment following next_segment is the tail of the segment list
			if (next_segment->next == DConstants::INVALID_INDEX) {
				art.DecreaseMemorySize(sizeof(LeafSegment));
				LeafSegment::Free(art, segment->next);
				segment->next = DConstants::INVALID_INDEX;
			}

			// adjust loop variables
			position = segment->next;
		}
	}
	count--;
}

bool Leaf::IsInlined() const {
	return count <= 1;
}

uint8_t Leaf::GetRowId(ART &art, const idx_t &position) const {

	D_ASSERT(position < count);
	if (IsInlined()) {
		return row_ids.inlined;
	}

	// get the correct segment
	auto segment = LeafSegment::Get(art, row_ids.position);
	for (idx_t i = 0; i < position / ARTNode::LEAF_SEGMENT_SIZE; i++) {
		D_ASSERT(segment->next != DConstants::INVALID_INDEX);
		segment = LeafSegment::Get(art, segment->next);
	}

	return segment->row_ids[position % ARTNode::PREFIX_SEGMENT_SIZE];
}

idx_t Leaf::FindRowId(ART &art, idx_t &position, const row_t &row_id) const {
	D_ASSERT(!IsInlined());

	auto next_position = position;
	auto remaining = count;
	while (next_position != DConstants::INVALID_INDEX) {

		position = next_position;
		auto segment = LeafSegment::Get(art, next_position);
		auto search_count = ARTNode::LEAF_SEGMENT_SIZE < remaining ? ARTNode::LEAF_SEGMENT_SIZE : remaining;

		// search in this segment
		for (idx_t i = 0; i < search_count; i++) {
			if (segment->row_ids[i] == row_id) {
				return count - remaining + i;
			}
		}

		// adjust loop variables
		remaining -= search_count;
		next_position = segment->next;
	}
	return DConstants::INVALID_INDEX;
}

string Leaf::ToString(ART &art) {

	string str = "Leaf: [";

	if (IsInlined()) {
		str += to_string(row_ids.inlined);
		return str + "]";
	}

	auto position = row_ids.position;
	auto remaining = count;
	while (position != DConstants::INVALID_INDEX) {
		auto segment = LeafSegment::Get(art, position);
		auto to_string_count = ARTNode::LEAF_SEGMENT_SIZE < remaining ? ARTNode::LEAF_SEGMENT_SIZE : remaining;

		for (idx_t i = 0; i < to_string_count; i++) {
			str += i == 0 ? to_string(segment->row_ids[i]) : ", " + to_string(segment->row_ids[i]);
		}
		remaining -= to_string_count;
		position = segment->next;
	}
	return str + "]";
}

BlockPointer Leaf::Serialize(ART &art, MetaBlockWriter &writer) {

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(ARTNodeType::LEAF);
	writer.Write<uint32_t>(count);
	prefix.Serialize(art, writer);

	if (IsInlined()) {
		writer.Write(row_ids.inlined);
		return block_pointer;
	}

	D_ASSERT(row_ids.position);
	auto position = row_ids.position;
	auto remaining = count;

	// iterate all leaf segments and write their row IDs
	while (position != DConstants::INVALID_INDEX) {
		auto segment = LeafSegment::Get(art, position);
		auto copy_count = ARTNode::LEAF_SEGMENT_SIZE < remaining ? ARTNode::LEAF_SEGMENT_SIZE : remaining;

		// write the row IDs
		for (idx_t i = 0; i < copy_count; i++) {
			writer.Write(segment->row_ids[i]);
		}

		// adjust loop variables
		remaining -= copy_count;
		position = segment->next;
	}

	return block_pointer;
}

void Leaf::Deserialize(ART &art, MetaBlockReader &reader) {

	auto count_p = reader.Read<uint32_t>();
	prefix.Deserialize(art, reader);

	// inlined
	if (count_p == 1) {
		row_ids.inlined = reader.Read<row_t>();
		count = count_p;
		return;
	}

	// copy into segments
	row_ids.position = LeafSegment::New(art);
	auto segment = LeafSegment::Initialize(art, row_ids.position);
	for (idx_t i = 0; i < count_p; i++) {
		segment = segment->Append(art, count, reader.Read<uint8_t>());
	}
	D_ASSERT(count_p == count);
}

void Leaf::MoveInlinedToSegment(ART &art) {
	D_ASSERT(IsInlined());
	auto position = LeafSegment::New(art);
	auto segment = LeafSegment::Initialize(art, position);

	// move row ID
	D_ASSERT(ARTNode::LEAF_SEGMENT_SIZE >= 1);
	segment->row_ids[0] = row_ids.inlined;
	row_ids.position = position;
}

} // namespace duckdb
