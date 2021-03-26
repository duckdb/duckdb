#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/assert.hpp"

namespace duckdb {

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(PhysicalType type) {
	return make_buffer<VectorBuffer>(STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(PhysicalType type) {
	return make_buffer<VectorBuffer>(GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(VectorType vector_type, const LogicalType &type) {
	return make_buffer<VectorBuffer>(vector_type, type, GetTypeIdSize(type.InternalType()));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(VectorType vector_type, const LogicalType &type) {
	return make_buffer<VectorBuffer>(vector_type, type, STANDARD_VECTOR_SIZE * GetTypeIdSize(type.InternalType()));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(VectorType vector_type, PhysicalType type) {
	return make_buffer<VectorBuffer>(vector_type, STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
}

VectorStringBuffer::VectorStringBuffer() : VectorBuffer(VectorBufferType::STRING_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer() : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
}

VectorStructBuffer::~VectorStructBuffer() {
}

VectorListBuffer::VectorListBuffer() : VectorBuffer(VectorBufferType::LIST_BUFFER) {
}

void VectorListBuffer::SetChild(unique_ptr<Vector> new_child) {
	child = move(new_child);
	capacity = STANDARD_VECTOR_SIZE;
}

struct CopyArrays {
	Vector &from;
	Vector &to;

	CopyArrays(Vector &from, Vector &to) : from(from), to(to) {};
};

void FindChildren(std::vector<CopyArrays> &to_copy, VectorBuffer &from_auxiliary, VectorBuffer &to_auxiliary) {
	if (from_auxiliary.GetBufferType() == VectorBufferType::LIST_BUFFER) {
		auto &from_buffer = (VectorListBuffer &)from_auxiliary;
		auto &from_child = from_buffer.GetChild();
		auto from_data = from_child.GetData();
		auto &to_buffer = (VectorListBuffer &)to_auxiliary;
		auto &to_child = to_buffer.GetChild();
		if (!from_data) {
			//! Nested type
			FindChildren(to_copy, *from_child.GetAuxiliary(), *to_child.GetAuxiliary());
		} else {
			to_copy.emplace_back(from_child, to_child);
		}
	} else if (from_auxiliary.GetBufferType() == VectorBufferType::STRUCT_BUFFER) {
		auto &from_buffer = (VectorStructBuffer &)from_auxiliary;
		auto &from_children = from_buffer.GetChildren();
		auto &to_buffer = (VectorStructBuffer &)to_auxiliary;
		auto &to_children = to_buffer.GetChildren();
		for (size_t i = 0; i < from_children.size(); i++) {
			auto from_data = from_children[i].second->GetData();
			if (!from_data) {
				//! Nested type
				FindChildren(to_copy, *from_children[i].second->GetAuxiliary(), *to_children[i].second->GetAuxiliary());
			} else {
				CopyArrays ar(*from_children[i].second, *to_children[i].second);
				to_copy.emplace_back(ar);
			}
		}
	}
}

void VectorListBuffer::Append(Vector &to_append, idx_t to_append_size, idx_t source_offset) {

	while (size + to_append_size - source_offset > capacity) {
		if (child->GetType().id() == LogicalTypeId::STRUCT && size == 0) {
			// Empty struct, gotta initialize it first
			auto &source_children = StructVector::GetEntries(to_append);
			for (auto &src_child : source_children) {
				auto child_copy = make_unique<Vector>(src_child.second->GetType());
				StructVector::AddEntry(*child, src_child.first, move(child_copy));
			}
		}
		// Drink chocomel to grow strong
		child->Resize(capacity);
		capacity *= 2;
	}
	VectorOperations::Copy(to_append, *child, to_append_size, source_offset, size);

	size += to_append_size - source_offset;
}

void VectorListBuffer::PushBack(Value &insert) {
	if (size + 1 > capacity) {
		// Drink chocomel to grow strong
		child->Resize(capacity);
		capacity *= 2;
	}
	child->SetValue(size++, insert);
}

VectorListBuffer::~VectorListBuffer() {
}

ManagedVectorBuffer::ManagedVectorBuffer(unique_ptr<BufferHandle> handle)
    : VectorBuffer(VectorBufferType::MANAGED_BUFFER), handle(move(handle)) {
}

ManagedVectorBuffer::~ManagedVectorBuffer() {
}

} // namespace duckdb
