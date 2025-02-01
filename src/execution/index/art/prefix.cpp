#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

Prefix::Prefix(const ART &art, const Node ptr_p, const bool is_mutable, const bool set_in_memory) {
	if (!set_in_memory) {
		data = Node::GetAllocator(art, PREFIX).Get(ptr_p, is_mutable);
	} else {
		data = Node::GetAllocator(art, PREFIX).GetIfLoaded(ptr_p);
		if (!data) {
			ptr = nullptr;
			in_memory = false;
			return;
		}
	}
	ptr = reinterpret_cast<Node *>(data + Count(art) + 1);
	in_memory = true;
}

Prefix::Prefix(unsafe_unique_ptr<FixedSizeAllocator> &allocator, const Node ptr_p, const idx_t count) {
	data = allocator->Get(ptr_p, true);
	ptr = reinterpret_cast<Node *>(data + count + 1);
	in_memory = true;
}

idx_t Prefix::GetMismatchWithOther(const Prefix &l_prefix, const Prefix &r_prefix, const idx_t max_count) {
	for (idx_t i = 0; i < max_count; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

optional_idx Prefix::GetMismatchWithKey(ART &art, const Node &node, const ARTKey &key, idx_t &depth) {
	Prefix prefix(art, node);
	for (idx_t i = 0; i < prefix.data[Prefix::Count(art)]; i++) {
		if (prefix.data[i] != key[depth]) {
			return i;
		}
		depth++;
	}
	return optional_idx::Invalid();
}

uint8_t Prefix::GetByte(const ART &art, const Node &node, const uint8_t pos) {
	D_ASSERT(node.GetType() == PREFIX);
	Prefix prefix(art, node);
	return prefix.data[pos];
}

Prefix Prefix::NewInternal(ART &art, Node &node, const data_ptr_t data, const uint8_t count, const idx_t offset,
                           const NType type) {
	node = Node::GetAllocator(art, type).New();
	node.SetMetadata(static_cast<uint8_t>(type));

	Prefix prefix(art, node, true);
	prefix.data[Count(art)] = count;
	if (data) {
		D_ASSERT(count);
		memcpy(prefix.data, data + offset, count);
	}
	return prefix;
}

void Prefix::New(ART &art, reference<Node> &ref, const ARTKey &key, const idx_t depth, idx_t count) {
	idx_t offset = 0;

	while (count) {
		auto min = MinValue(UnsafeNumericCast<idx_t>(Count(art)), count);
		auto this_count = UnsafeNumericCast<uint8_t>(min);
		auto prefix = NewInternal(art, ref, key.data, this_count, offset + depth, PREFIX);

		ref = *prefix.ptr;
		offset += this_count;
		count -= this_count;
	}
}

void Prefix::Free(ART &art, Node &node) {
	Node next;

	while (node.HasMetadata() && node.GetType() == PREFIX) {
		Prefix prefix(art, node, true);
		next = *prefix.ptr;
		Node::GetAllocator(art, PREFIX).Free(node);
		node = next;
	}

	Node::Free(art, node);
	node.Clear();
}

void Prefix::InitializeMerge(ART &art, Node &node, const unsafe_vector<idx_t> &upper_bounds) {
	auto buffer_count = upper_bounds[Node::GetAllocatorIdx(PREFIX)];
	Node next = node;
	Prefix prefix(art, next, true);

	while (next.GetType() == PREFIX) {
		next = *prefix.ptr;
		if (prefix.ptr->GetType() == PREFIX) {
			prefix.ptr->IncreaseBufferId(buffer_count);
			prefix = Prefix(art, next, true);
		}
	}

	node.IncreaseBufferId(buffer_count);
	prefix.ptr->InitMerge(art, upper_bounds);
}

void Prefix::Concat(ART &art, Node &parent, uint8_t byte, const GateStatus old_status, const Node &child,
                    const GateStatus status) {
	D_ASSERT(!parent.IsAnyLeaf());
	D_ASSERT(child.HasMetadata());

	if (old_status == GateStatus::GATE_SET) {
		// Concat Node4.
		D_ASSERT(status == GateStatus::GATE_SET);
		return ConcatGate(art, parent, byte, child);
	}
	if (child.GetGateStatus() == GateStatus::GATE_SET) {
		// Concat Node4.
		D_ASSERT(status == GateStatus::GATE_NOT_SET);
		return ConcatChildIsGate(art, parent, byte, child);
	}

	if (status == GateStatus::GATE_SET && child.GetType() == NType::LEAF_INLINED) {
		auto row_id = child.GetRowId();
		Free(art, parent);
		Leaf::New(parent, row_id);
		return;
	}

	if (parent.GetType() != PREFIX) {
		auto prefix = NewInternal(art, parent, &byte, 1, 0, PREFIX);
		if (child.GetType() == PREFIX) {
			prefix.Append(art, child);
		} else {
			*prefix.ptr = child;
		}
		return;
	}

	auto tail = GetTail(art, parent);
	tail = tail.Append(art, byte);

	if (child.GetType() == PREFIX) {
		tail.Append(art, child);
	} else {
		*tail.ptr = child;
	}
}

template <class NODE>
optional_idx TraverseInternal(ART &art, reference<NODE> &node, const ARTKey &key, idx_t &depth,
                              const bool is_mutable = false) {
	D_ASSERT(node.get().HasMetadata());
	D_ASSERT(node.get().GetType() == NType::PREFIX);

	while (node.get().GetType() == NType::PREFIX) {
		auto pos = Prefix::GetMismatchWithKey(art, node, key, depth);
		if (pos.IsValid()) {
			return pos;
		}

		Prefix prefix(art, node, is_mutable);
		node = *prefix.ptr;
		if (node.get().GetGateStatus() == GateStatus::GATE_SET) {
			break;
		}
	}

	// We return an invalid index, if (and only if) the next node is:
	// 1. not a prefix, or
	// 2. a gate.
	return optional_idx::Invalid();
}

optional_idx Prefix::Traverse(ART &art, reference<const Node> &node, const ARTKey &key, idx_t &depth) {
	return TraverseInternal<const Node>(art, node, key, depth);
}

optional_idx Prefix::TraverseMutable(ART &art, reference<Node> &node, const ARTKey &key, idx_t &depth) {
	return TraverseInternal<Node>(art, node, key, depth, true);
}

bool Prefix::Traverse(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &pos, const GateStatus status) {
	D_ASSERT(l_node.get().HasMetadata());
	D_ASSERT(r_node.get().HasMetadata());

	Prefix l_prefix(art, l_node, true);
	Prefix r_prefix(art, r_node, true);

	idx_t max_count = MinValue(l_prefix.data[Count(art)], r_prefix.data[Count(art)]);
	pos = GetMismatchWithOther(l_prefix, r_prefix, max_count);
	if (pos != DConstants::INVALID_INDEX) {
		return true;
	}

	// Match.
	if (l_prefix.data[Count(art)] == r_prefix.data[Count(art)]) {
		auto r_child = *r_prefix.ptr;
		r_prefix.ptr->Clear();
		Node::Free(art, r_node);
		return l_prefix.ptr->MergeInternal(art, r_child, status);
	}

	pos = max_count;
	if (r_prefix.ptr->GetType() != PREFIX && r_prefix.data[Count(art)] == max_count) {
		// l_prefix contains r_prefix.
		swap(l_node.get(), r_node.get());
		l_node = *r_prefix.ptr;
		return true;
	}
	// r_prefix contains l_prefix.
	l_node = *l_prefix.ptr;
	return true;
}

void Prefix::Reduce(ART &art, Node &node, const idx_t pos) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(pos < Count(art));

	Prefix prefix(art, node);
	if (pos == idx_t(prefix.data[Count(art)] - 1)) {
		auto next = *prefix.ptr;
		prefix.ptr->Clear();
		Node::Free(art, node);
		node = next;
		return;
	}

	for (idx_t i = 0; i < Count(art) - pos - 1; i++) {
		prefix.data[i] = prefix.data[pos + i + 1];
	}

	prefix.data[Count(art)] -= pos + 1;
	prefix.Append(art, *prefix.ptr);
}

GateStatus Prefix::Split(ART &art, reference<Node> &node, Node &child, const uint8_t pos) {
	D_ASSERT(node.get().HasMetadata());

	Prefix prefix(art, node, true);

	// The split is at the last prefix byte. Decrease the count and return.
	if (pos + 1 == Count(art)) {
		prefix.data[Count(art)]--;
		node = *prefix.ptr;
		child = *prefix.ptr;
		return GateStatus::GATE_NOT_SET;
	}

	if (pos + 1 < prefix.data[Count(art)]) {
		// Create a new prefix and
		// 1. copy the remaining bytes of this prefix.
		// 2. append remaining prefix nodes.
		auto new_prefix = NewInternal(art, child, nullptr, 0, 0, PREFIX);
		new_prefix.data[Count(art)] = prefix.data[Count(art)] - pos - 1;
		memcpy(new_prefix.data, prefix.data + pos + 1, new_prefix.data[Count(art)]);

		if (prefix.ptr->GetType() == PREFIX && prefix.ptr->GetGateStatus() == GateStatus::GATE_NOT_SET) {
			new_prefix.Append(art, *prefix.ptr);
		} else {
			*new_prefix.ptr = *prefix.ptr;
		}

	} else if (pos + 1 == prefix.data[Count(art)]) {
		// No prefix bytes after the split.
		child = *prefix.ptr;
	}

	// Set the new count of this node.
	prefix.data[Count(art)] = pos;

	// No bytes left before the split, free this node.
	if (pos == 0) {
		auto old_status = node.get().GetGateStatus();
		prefix.ptr->Clear();
		Node::Free(art, node);
		return old_status;
	}

	// There are bytes left before the split.
	// The subsequent node replaces the split byte.
	node = *prefix.ptr;
	return GateStatus::GATE_NOT_SET;
}

ARTConflictType Prefix::Insert(ART &art, Node &node, const ARTKey &key, idx_t depth, const ARTKey &row_id,
                               const GateStatus status, optional_ptr<ART> delete_art,
                               const IndexAppendMode append_mode) {
	reference<Node> next(node);
	auto pos = TraverseMutable(art, next, key, depth);

	// We recurse into the next node, if
	// (1) the prefix matches the key.
	// (2) we reach a gate.
	if (!pos.IsValid()) {
		D_ASSERT(next.get().GetType() != NType::PREFIX || next.get().GetGateStatus() == GateStatus::GATE_SET);
		return art.Insert(next, key, depth, row_id, status, delete_art, append_mode);
	}

	Node remainder;
	auto byte = GetByte(art, next, UnsafeNumericCast<uint8_t>(pos.GetIndex()));
	auto split_status = Split(art, next, remainder, UnsafeNumericCast<uint8_t>(pos.GetIndex()));
	Node4::New(art, next);
	next.get().SetGateStatus(split_status);

	// Insert the remaining prefix into the new Node4.
	Node4::InsertChild(art, next, byte, remainder);

	if (status == GateStatus::GATE_SET) {
		D_ASSERT(pos.GetIndex() != ROW_ID_COUNT);
		Node new_row_id;
		Leaf::New(new_row_id, key.GetRowId());
		Node::InsertChild(art, next, key[depth], new_row_id);
		return ARTConflictType::NO_CONFLICT;
	}

	Node leaf;
	reference<Node> ref(leaf);
	if (depth + 1 < key.len) {
		// Create the prefix.
		auto count = key.len - depth - 1;
		Prefix::New(art, ref, key, depth + 1, count);
	}

	// Create the inlined leaf.
	Leaf::New(ref, row_id.GetRowId());
	Node4::InsertChild(art, next, key[depth], leaf);
	return ARTConflictType::NO_CONFLICT;
}

string Prefix::VerifyAndToString(ART &art, const Node &node, const bool only_verify) {
	string str = "";
	reference<const Node> ref(node);

	Iterator(art, ref, true, false, [&](Prefix &prefix) {
		D_ASSERT(prefix.data[Count(art)] != 0);
		D_ASSERT(prefix.data[Count(art)] <= Count(art));

		str += " Prefix :[ ";
		for (idx_t i = 0; i < prefix.data[Count(art)]; i++) {
			str += to_string(prefix.data[i]) + "-";
		}
		str += " ] ";
	});

	auto child = ref.get().VerifyAndToString(art, only_verify);
	return only_verify ? "" : str + child;
}

void Prefix::VerifyAllocations(ART &art, const Node &node, unordered_map<uint8_t, idx_t> &node_counts) {
	auto idx = Node::GetAllocatorIdx(PREFIX);
	reference<const Node> ref(node);
	Iterator(art, ref, false, false, [&](Prefix &prefix) { node_counts[idx]++; });
	return ref.get().VerifyAllocations(art, node_counts);
}

void Prefix::Vacuum(ART &art, Node &node, const unordered_set<uint8_t> &indexes) {
	bool set = indexes.find(Node::GetAllocatorIdx(PREFIX)) != indexes.end();
	auto &allocator = Node::GetAllocator(art, PREFIX);

	reference<Node> ref(node);
	while (ref.get().GetType() == PREFIX) {
		if (set && allocator.NeedsVacuum(ref)) {
			auto status = ref.get().GetGateStatus();
			ref.get() = allocator.VacuumPointer(ref);
			ref.get().SetMetadata(static_cast<uint8_t>(PREFIX));
			ref.get().SetGateStatus(status);
		}
		Prefix prefix(art, ref, true);
		ref = *prefix.ptr;
	}

	ref.get().Vacuum(art, indexes);
}

void Prefix::TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator) {
	// Early-out, if we do not need any transformations.
	if (!allocator) {
		reference<Node> ref(node);
		while (ref.get().GetType() == PREFIX && ref.get().GetGateStatus() == GateStatus::GATE_NOT_SET) {
			Prefix prefix(art, ref, true, true);
			if (!prefix.in_memory) {
				return;
			}
			ref = *prefix.ptr;
		}
		return Node::TransformToDeprecated(art, ref, allocator);
	}

	// We need to create a new prefix (chain).
	Node new_node;
	new_node = allocator->New();
	new_node.SetMetadata(static_cast<uint8_t>(PREFIX));
	Prefix new_prefix(allocator, new_node, DEPRECATED_COUNT);

	Node current_node = node;
	while (current_node.GetType() == PREFIX && current_node.GetGateStatus() == GateStatus::GATE_NOT_SET) {
		Prefix prefix(art, current_node, true, true);
		if (!prefix.in_memory) {
			return;
		}

		for (idx_t i = 0; i < prefix.data[Count(art)]; i++) {
			new_prefix = new_prefix.TransformToDeprecatedAppend(art, allocator, prefix.data[i]);
		}

		*new_prefix.ptr = *prefix.ptr;
		prefix.ptr->Clear();
		Node::Free(art, current_node);
		current_node = *new_prefix.ptr;
	}

	node = new_node;
	return Node::TransformToDeprecated(art, *new_prefix.ptr, allocator);
}

Prefix Prefix::Append(ART &art, const uint8_t byte) {
	if (data[Count(art)] != Count(art)) {
		data[data[Count(art)]] = byte;
		data[Count(art)]++;
		return *this;
	}

	auto prefix = NewInternal(art, *ptr, nullptr, 0, 0, PREFIX);
	return prefix.Append(art, byte);
}

void Prefix::Append(ART &art, Node other) {
	D_ASSERT(other.HasMetadata());

	Prefix prefix = *this;
	while (other.GetType() == PREFIX) {
		if (other.GetGateStatus() == GateStatus::GATE_SET) {
			*prefix.ptr = other;
			return;
		}

		Prefix other_prefix(art, other, true);
		for (idx_t i = 0; i < other_prefix.data[Count(art)]; i++) {
			prefix = prefix.Append(art, other_prefix.data[i]);
		}

		*prefix.ptr = *other_prefix.ptr;
		Node::GetAllocator(art, PREFIX).Free(other);
		other = *prefix.ptr;
	}
}

Prefix Prefix::GetTail(ART &art, const Node &node) {
	Prefix prefix(art, node, true);
	while (prefix.ptr->GetType() == PREFIX) {
		prefix = Prefix(art, *prefix.ptr, true);
	}
	return prefix;
}

void Prefix::ConcatGate(ART &art, Node &parent, uint8_t byte, const Node &child) {
	D_ASSERT(child.HasMetadata());
	Node new_prefix = Node();

	// Inside gates, inlined row IDs are not prefixed.
	if (child.GetType() == NType::LEAF_INLINED) {
		Leaf::New(new_prefix, child.GetRowId());

	} else if (child.GetType() == PREFIX) {
		// At least one more row ID in this gate.
		auto prefix = NewInternal(art, new_prefix, &byte, 1, 0, PREFIX);
		prefix.ptr->Clear();
		prefix.Append(art, child);
		new_prefix.SetGateStatus(GateStatus::GATE_SET);

	} else {
		// At least one more row ID in this gate.
		auto prefix = NewInternal(art, new_prefix, &byte, 1, 0, PREFIX);
		*prefix.ptr = child;
		new_prefix.SetGateStatus(GateStatus::GATE_SET);
	}

	if (parent.GetType() != PREFIX) {
		parent = new_prefix;
		return;
	}
	*GetTail(art, parent).ptr = new_prefix;
}

void Prefix::ConcatChildIsGate(ART &art, Node &parent, uint8_t byte, const Node &child) {
	// Create a new prefix and point it to the gate.
	if (parent.GetType() != PREFIX) {
		auto prefix = NewInternal(art, parent, &byte, 1, 0, PREFIX);
		*prefix.ptr = child;
		return;
	}

	auto tail = GetTail(art, parent);
	tail = tail.Append(art, byte);
	*tail.ptr = child;
}

Prefix Prefix::TransformToDeprecatedAppend(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator, uint8_t byte) {
	if (data[DEPRECATED_COUNT] != DEPRECATED_COUNT) {
		data[data[DEPRECATED_COUNT]] = byte;
		data[DEPRECATED_COUNT]++;
		return *this;
	}

	*ptr = allocator->New();
	ptr->SetMetadata(static_cast<uint8_t>(PREFIX));
	Prefix prefix(allocator, *ptr, DEPRECATED_COUNT);
	return prefix.TransformToDeprecatedAppend(art, allocator, byte);
}

} // namespace duckdb
