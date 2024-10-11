#include "duckdb/common/types/vector_cache.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

class VectorCacheBuffer : public VectorBuffer {
public:
	explicit VectorCacheBuffer(Allocator &allocator, const LogicalType &type_p, idx_t capacity_p = STANDARD_VECTOR_SIZE)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), type(type_p), capacity(capacity_p) {
		auto internal_type = type.InternalType();
		switch (internal_type) {
		case PhysicalType::LIST: {
			// memory for the list offsets
			owned_data = allocator.Allocate(capacity * GetTypeIdSize(internal_type));
			// child data of the list
			auto &child_type = ListType::GetChildType(type);
			child_caches.push_back(make_buffer<VectorCacheBuffer>(allocator, child_type, capacity));
			auto child_vector = make_uniq<Vector>(child_type, false, false);
			auxiliary = make_shared_ptr<VectorListBuffer>(std::move(child_vector));
			break;
		}
		case PhysicalType::ARRAY: {
			auto &child_type = ArrayType::GetChildType(type);
			auto array_size = ArrayType::GetSize(type);
			child_caches.push_back(make_buffer<VectorCacheBuffer>(allocator, child_type, array_size * capacity));
			auto child_vector = make_uniq<Vector>(child_type, true, false, array_size * capacity);
			auxiliary = make_shared_ptr<VectorArrayBuffer>(std::move(child_vector), array_size, capacity);
			break;
		}
		case PhysicalType::STRUCT: {
			auto &child_types = StructType::GetChildTypes(type);
			for (auto &child_type : child_types) {
				child_caches.push_back(make_buffer<VectorCacheBuffer>(allocator, child_type.second, capacity));
			}
			auto struct_buffer = make_shared_ptr<VectorStructBuffer>(type);
			auxiliary = std::move(struct_buffer);
			break;
		}
		default:
			owned_data = allocator.Allocate(capacity * GetTypeIdSize(internal_type));
			break;
		}
	}

	void ResetFromCache(Vector &result, const buffer_ptr<VectorBuffer> &buffer) {
		D_ASSERT(type == result.GetType());
		auto internal_type = type.InternalType();
		result.vector_type = VectorType::FLAT_VECTOR;
		AssignSharedPointer(result.buffer, buffer);
		result.validity.Reset(capacity);
		switch (internal_type) {
		case PhysicalType::LIST: {
			result.data = owned_data.get();
			// reinitialize the VectorListBuffer
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through child
			auto &child_cache = child_caches[0]->Cast<VectorCacheBuffer>();
			auto &list_buffer = result.auxiliary->Cast<VectorListBuffer>();
			list_buffer.SetCapacity(child_cache.capacity);
			list_buffer.SetSize(0);
			list_buffer.SetAuxiliaryData(nullptr);

			auto &list_child = list_buffer.GetChild();
			child_cache.ResetFromCache(list_child, child_caches[0]);
			break;
		}
		case PhysicalType::ARRAY: {
			// fixed size list does not have own data
			result.data = nullptr;
			// reinitialize the VectorArrayBuffer
			// auxiliary->SetAuxiliaryData(nullptr);
			AssignSharedPointer(result.auxiliary, auxiliary);

			// propagate through child
			auto &child_cache = child_caches[0]->Cast<VectorCacheBuffer>();
			auto &array_child = result.auxiliary->Cast<VectorArrayBuffer>().GetChild();
			child_cache.ResetFromCache(array_child, child_caches[0]);
			break;
		}
		case PhysicalType::STRUCT: {
			// struct does not have data
			result.data = nullptr;
			// reinitialize the VectorStructBuffer
			auxiliary->SetAuxiliaryData(nullptr);
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through children
			auto &children = result.auxiliary->Cast<VectorStructBuffer>().GetChildren();
			for (idx_t i = 0; i < children.size(); i++) {
				auto &child_cache = child_caches[i]->Cast<VectorCacheBuffer>();
				child_cache.ResetFromCache(*children[i], child_caches[i]);
			}
			break;
		}
		default:
			// regular type: no aux data and reset data to cached data
			result.data = owned_data.get();
			result.auxiliary.reset();
			break;
		}
	}

	const LogicalType &GetType() {
		return type;
	}

private:
	//! The type of the vector cache
	LogicalType type;
	//! Owned data
	AllocatedData owned_data;
	//! Child caches (if any). Used for nested types.
	vector<buffer_ptr<VectorBuffer>> child_caches;
	//! Aux data for the vector (if any)
	buffer_ptr<VectorBuffer> auxiliary;
	//! Capacity of the vector
	idx_t capacity;
};

VectorCache::VectorCache(Allocator &allocator, const LogicalType &type_p, idx_t capacity_p) {
	buffer = make_buffer<VectorCacheBuffer>(allocator, type_p, capacity_p);
}

void VectorCache::ResetFromCache(Vector &result) const {
	D_ASSERT(buffer);
	auto &vcache = buffer->Cast<VectorCacheBuffer>();
	vcache.ResetFromCache(result, buffer);
}

const LogicalType &VectorCache::GetType() const {
	auto &vcache = buffer->Cast<VectorCacheBuffer>();
	return vcache.GetType();
}

} // namespace duckdb
