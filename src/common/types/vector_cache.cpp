#include "duckdb/common/types/vector_cache.hpp"

namespace duckdb {

VectorCache::VectorCache(const LogicalType &type_p) :
	type(type_p) {
	auto internal_type = type.InternalType();
	switch(internal_type) {
	case PhysicalType::LIST:
		// memory for the list offsets
		owned_data = unique_ptr<data_t[]>(new data_t[STANDARD_VECTOR_SIZE * GetTypeIdSize(internal_type)]);
		// child data of the list
		child_caches.push_back(make_unique<VectorCache>(ListType::GetChildType(type)));
		auxiliary = make_unique<VectorListBuffer>(type);
		break;
	case PhysicalType::STRUCT: {
		auto &child_types = StructType::GetChildTypes(type);
		for(auto &child_type : child_types) {
			child_caches.push_back(make_unique<VectorCache>(child_type.second));
		}
		auto struct_buffer = make_unique<VectorStructBuffer>(type);
		auxiliary = move(struct_buffer);
		break;
	}
	default:
		owned_data = unique_ptr<data_t[]>(new data_t[STANDARD_VECTOR_SIZE * GetTypeIdSize(internal_type)]);
		break;
	}
}

}
