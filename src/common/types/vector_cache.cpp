#include "duckdb/common/types/vector_cache.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

namespace duckdb {

class VectorCacheEntry {
public:
	explicit VectorCacheEntry(Allocator &allocator, const LogicalType &type_p, idx_t capacity_p = STANDARD_VECTOR_SIZE)
	    : type(type_p), capacity(capacity_p) {
		auto internal_type = type.InternalType();
		switch (internal_type) {
		case PhysicalType::LIST: {
			// child data of the list
			auto &child_type = ListType::GetChildType(type);
			child_caches.push_back(make_uniq<VectorCacheEntry>(allocator, child_type, capacity));
			auto child_vector = make_uniq<Vector>(child_type, nullptr);
			buffer = make_buffer<VectorListBuffer>(allocator, capacity, std::move(child_vector));
			break;
		}
		case PhysicalType::ARRAY: {
			auto &child_type = ArrayType::GetChildType(type);
			auto array_size = ArrayType::GetSize(type);
			child_caches.push_back(make_uniq<VectorCacheEntry>(allocator, child_type, array_size * capacity));
			auto child_vector = make_uniq<Vector>(child_type, array_size * capacity);
			buffer = make_shared_ptr<VectorArrayBuffer>(std::move(child_vector), array_size, capacity);
			break;
		}
		case PhysicalType::STRUCT: {
			auto &child_types = StructType::GetChildTypes(type);
			for (auto &child_type : child_types) {
				child_caches.push_back(make_uniq<VectorCacheEntry>(allocator, child_type.second, capacity));
			}
			buffer = make_buffer<VectorStructBuffer>(type);
			break;
		}
		case PhysicalType::VARCHAR:
			buffer = make_buffer<VectorStringBuffer>(allocator, capacity);
			break;
		default:
			buffer = make_buffer<StandardVectorBuffer>(allocator, capacity, GetTypeIdSize(internal_type));
			break;
		}
	}

	void ResetFromCache(Vector &result) {
		D_ASSERT(type == result.GetType());
		auto internal_type = type.InternalType();
		buffer->ClearAuxiliaryData();
		result.SetBuffer(buffer_ptr<VectorBuffer>(buffer));
		result.BufferMutable().ResetCapacity(capacity);
		// use SetVectorTypeOnly to avoid propagating to children
		// for nested types (struct/array/list) children may have stale incompatible buffers
		// from a previous execution - they will be reset individually below
		result.BufferMutable().SetVectorTypeOnly(VectorType::FLAT_VECTOR);
		switch (internal_type) {
		case PhysicalType::LIST: {
			// reinitialize the VectorListBuffer
			// propagate through child
			auto &child_cache = *child_caches[0];
			auto &list_buffer = result.BufferMutable().Cast<VectorListBuffer>();

			auto &list_child = list_buffer.GetChild();
			child_cache.ResetFromCache(list_child);
			list_buffer.SetChildSize(0);
			break;
		}
		case PhysicalType::ARRAY: {
			// reinitialize the VectorArrayBuffer
			// propagate through child
			auto &child_cache = *child_caches[0];
			auto &array_child = result.BufferMutable().Cast<VectorArrayBuffer>().GetChild();
			child_cache.ResetFromCache(array_child);
			break;
		}
		case PhysicalType::STRUCT: {
			// reinitialize the VectorStructBuffer
			// propagate through children
			auto &children = result.BufferMutable().Cast<VectorStructBuffer>().GetChildren();
			for (idx_t i = 0; i < children.size(); i++) {
				auto &child_cache = *child_caches[i];
				child_cache.ResetFromCache(children[i]);
			}
			break;
		}
		default:
			break;
		}
	}

	const LogicalType &GetType() {
		return type;
	}

private:
	//! The type of the vector cache
	LogicalType type;
	//! Child caches (if any). Used for nested types.
	vector<unique_ptr<VectorCacheEntry>> child_caches;
	//! Buffer data for the vector (if any)
	buffer_ptr<VectorBuffer> buffer;
	//! Capacity of the vector
	capacity_t capacity;
};

VectorCache::VectorCache() {
}

VectorCache::VectorCache(Allocator &allocator, const LogicalType &type_p, const idx_t capacity_p) {
	cache_entry = make_uniq<VectorCacheEntry>(allocator, type_p, capacity_p);
}

VectorCache::~VectorCache() {
}

VectorCache::VectorCache(VectorCache &&other) noexcept : cache_entry(std::move(other.cache_entry)) {
}

VectorCache &VectorCache::operator=(VectorCache &&other) noexcept {
	cache_entry = std::move(other.cache_entry);
	return *this;
}

void VectorCache::ResetFromCache(Vector &result) const {
	if (!cache_entry) {
		return;
	}
	cache_entry->ResetFromCache(result);
}

const LogicalType &VectorCache::GetType() const {
	return cache_entry->GetType();
}

} // namespace duckdb
