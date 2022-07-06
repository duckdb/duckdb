#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {

class VectorCacheBuffer : public VectorBuffer {
public:
	explicit VectorCacheBuffer(Allocator &allocator, const LogicalType &type_p)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), type(type_p) {
		auto internal_type = type.InternalType();
		switch (internal_type) {
		case PhysicalType::LIST: {
			// memory for the list offsets
			owned_data = allocator.Allocate(STANDARD_VECTOR_SIZE * GetTypeIdSize(internal_type));
			// child data of the list
			auto &child_type = ListType::GetChildType(type);
			child_caches.push_back(make_buffer<VectorCacheBuffer>(allocator, child_type));
			auto child_vector = make_unique<Vector>(child_type, false, false);
			auxiliary = make_unique<VectorListBuffer>(move(child_vector));
			break;
		}
		case PhysicalType::STRUCT: {
			auto &child_types = StructType::GetChildTypes(type);
			for (auto &child_type : child_types) {
				child_caches.push_back(make_buffer<VectorCacheBuffer>(allocator, child_type.second));
			}
			auto struct_buffer = make_unique<VectorStructBuffer>(type);
			auxiliary = move(struct_buffer);
			break;
		}
		default:
			owned_data = allocator.Allocate(STANDARD_VECTOR_SIZE * GetTypeIdSize(internal_type));
			break;
		}
	}

	void ResetFromCache(Vector &result, const buffer_ptr<VectorBuffer> &buffer) {
		D_ASSERT(type == result.GetType());
		auto internal_type = type.InternalType();
		result.vector_type = VectorType::FLAT_VECTOR;
		AssignSharedPointer(result.buffer, buffer);
		result.validity.Reset();
		switch (internal_type) {
		case PhysicalType::LIST: {
			result.data = owned_data->get();
			// reinitialize the VectorListBuffer
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through child
			auto &list_buffer = (VectorListBuffer &)*result.auxiliary;
			list_buffer.capacity = STANDARD_VECTOR_SIZE;
			list_buffer.size = 0;

			auto &list_child = list_buffer.GetChild();
			auto &child_cache = (VectorCacheBuffer &)*child_caches[0];
			child_cache.ResetFromCache(list_child, child_caches[0]);
			break;
		}
		case PhysicalType::STRUCT: {
			// struct does not have data
			result.data = nullptr;
			// reinitialize the VectorStructBuffer
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through children
			auto &children = ((VectorStructBuffer &)*result.auxiliary).GetChildren();
			for (idx_t i = 0; i < children.size(); i++) {
				auto &child_cache = (VectorCacheBuffer &)*child_caches[i];
				child_cache.ResetFromCache(*children[i], child_caches[i]);
			}
			break;
		}
		default:
			// regular type: no aux data and reset data to cached data
			result.data = owned_data->get();
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
	unique_ptr<AllocatedData> owned_data;
	//! Child caches (if any). Used for nested types.
	vector<buffer_ptr<VectorBuffer>> child_caches;
	//! Aux data for the vector (if any)
	buffer_ptr<VectorBuffer> auxiliary;
};

VectorCache::VectorCache(Allocator &allocator, const LogicalType &type_p) {
	buffer = make_unique<VectorCacheBuffer>(allocator, type_p);
}

void VectorCache::ResetFromCache(Vector &result) const {
	D_ASSERT(buffer);
	auto &vcache = (VectorCacheBuffer &)*buffer;
	vcache.ResetFromCache(result, buffer);
}

const LogicalType &VectorCache::GetType() const {
	auto &vcache = (VectorCacheBuffer &)*buffer;
	return vcache.GetType();
}

} // namespace duckdb
