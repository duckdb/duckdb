#include "duckdb/execution/index/art/prefix_handle.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

PrefixHandle::PrefixHandle(const ART &art, const Node node)
    : segment_handle(make_uniq<SegmentHandle>(Node::GetAllocator(art, PREFIX).GetHandle(node))) {
	data = segment_handle->GetPtr();
	ptr = reinterpret_cast<Node *>(data + Count(art) + 1);
	segment_handle->MarkModified();
}

PrefixHandle::PrefixHandle(unsafe_unique_ptr<FixedSizeAllocator> &allocator, const Node node, const uint8_t count)
    : segment_handle(make_uniq<SegmentHandle>(allocator->GetHandle(node))) {
	data = segment_handle->GetPtr();
	ptr = reinterpret_cast<Node *>(data + count + 1);
	segment_handle->MarkModified();
}

PrefixHandle::PrefixHandle(PrefixHandle &&other) noexcept
    : segment_handle(std::move(other.segment_handle)), data(other.data), ptr(other.ptr) {
	other.data = nullptr;
	other.ptr = nullptr;
}

PrefixHandle &PrefixHandle::operator=(PrefixHandle &&other) noexcept {
	if (this != &other) {
		segment_handle = std::move(other.segment_handle);
		data = other.data;
		ptr = other.ptr;

		other.data = nullptr;
		other.ptr = nullptr;
	}
	return *this;
}

void PrefixHandle::TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator) {
	// Early-out, if we do not need any transformations.
	if (!allocator) {
		reference<Node> ref(node);
		while (ref.get().GetType() == PREFIX && ref.get().GetGateStatus() == GateStatus::GATE_NOT_SET) {
			auto &alloc = Node::GetAllocator(art, PREFIX);
			if (!alloc.IsLoaded(ref)) {
				return;
			}
			PrefixHandle handle(art, ref);
			ref = *handle.ptr;
		}
		return Node::TransformToDeprecated(art, ref, allocator);
	}

	// We need to create a new prefix (chain) in the deprecated format.
	Node new_node;
	new_node = allocator->New();
	new_node.SetMetadata(static_cast<uint8_t>(PREFIX));
	PrefixHandle new_handle(allocator, new_node, DEPRECATED_COUNT);
	new_handle.data[DEPRECATED_COUNT] = 0;

	Node current_node = node;
	while (current_node.GetType() == PREFIX && current_node.GetGateStatus() == GateStatus::GATE_NOT_SET) {
		auto &alloc = Node::GetAllocator(art, PREFIX);
		if (!alloc.IsLoaded(current_node)) {
			return;
		}

		PrefixHandle current_handle(art, current_node);

		for (idx_t i = 0; i < current_handle.data[Count(art)]; i++) {
			new_handle = new_handle.TransformToDeprecatedAppend(art, allocator, current_handle.data[i]);
		}

		*new_handle.ptr = *current_handle.ptr;
		Node::FreeNode(art, current_node);
		current_node = *new_handle.ptr;
	}

	node = new_node;
	return Node::TransformToDeprecated(art, *new_handle.ptr, allocator);
}

PrefixHandle PrefixHandle::TransformToDeprecatedAppend(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator,
                                                       const uint8_t byte) {
	if (data[DEPRECATED_COUNT] != DEPRECATED_COUNT) {
		data[data[DEPRECATED_COUNT]] = byte;
		data[DEPRECATED_COUNT]++;
		return std::move(*this);
	}

	*ptr = allocator->New();
	ptr->SetMetadata(static_cast<uint8_t>(PREFIX));

	PrefixHandle new_prefix(allocator, *ptr, DEPRECATED_COUNT);
	new_prefix.data[DEPRECATED_COUNT] = 0;
	return new_prefix.TransformToDeprecatedAppend(art, allocator, byte);
}

} // namespace duckdb
