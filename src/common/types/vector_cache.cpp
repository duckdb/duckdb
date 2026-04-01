#include "duckdb/common/types/vector_cache.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
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
			auto child_vector = make_uniq<Vector>(child_type, false, false);
			buffer = make_buffer<VectorListBuffer>(allocator, capacity, std::move(child_vector), capacity);
			break;
		}
		case PhysicalType::ARRAY: {
			auto &child_type = ArrayType::GetChildType(type);
			auto array_size = ArrayType::GetSize(type);
			child_caches.push_back(make_uniq<VectorCacheEntry>(allocator, child_type, array_size * capacity));
			auto child_vector = make_uniq<Vector>(child_type, true, false, array_size * capacity);
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
			buffer = make_buffer<VectorStringBuffer>(allocator, capacity * GetTypeIdSize(internal_type));
			break;
		default:
			buffer = make_buffer<StandardVectorBuffer>(allocator, capacity * GetTypeIdSize(internal_type));
			break;
		}
	}

	void ResetFromCache(Vector &result) {
		D_ASSERT(type == result.GetType());
		auto internal_type = type.InternalType();
		result.vector_type = VectorType::FLAT_VECTOR;
		buffer->ClearAuxiliaryData();
		AssignSharedPointer(result.buffer, buffer);
		result.validity.Reset(capacity);
		switch (internal_type) {
		case PhysicalType::LIST: {
			// reinitialize the VectorListBuffer
			result.auxiliary.reset();
			// propagate through child
			auto &child_cache = *child_caches[0];
			auto &list_buffer = result.buffer->Cast<VectorListBuffer>();
			list_buffer.SetCapacity(child_cache.capacity);
			list_buffer.SetSize(0);

			auto &list_child = list_buffer.GetChild();
			child_cache.ResetFromCache(list_child);
			break;
		}
		case PhysicalType::ARRAY: {
			// reinitialize the VectorArrayBuffer
			result.auxiliary.reset();

			// propagate through child
			auto &child_cache = *child_caches[0];
			auto &array_child = result.buffer->Cast<VectorArrayBuffer>().GetChild();
			child_cache.ResetFromCache(array_child);
			break;
		}
		case PhysicalType::STRUCT: {
			// reinitialize the VectorStructBuffer
			result.auxiliary.reset();
			// propagate through children
			auto &children = result.buffer->Cast<VectorStructBuffer>().GetChildren();
			for (idx_t i = 0; i < children.size(); i++) {
				auto &child_cache = *child_caches[i];
				child_cache.ResetFromCache(children[i]);
			}
			break;
		}
		default:
			// regular type: no aux data and reset data to cached data
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
	//! Child caches (if any). Used for nested types.
	vector<unique_ptr<VectorCacheEntry>> child_caches;
	//! Buffer data for the vector (if any)
	buffer_ptr<VectorBuffer> buffer;
	//! Aux data for the vector (if any)
	buffer_ptr<VectorBuffer> auxiliary;
	//! Capacity of the vector
	idx_t capacity;
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
