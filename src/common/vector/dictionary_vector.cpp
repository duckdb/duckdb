#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

buffer_ptr<VectorChildBuffer> DictionaryVector::CreateReusableDictionary(const LogicalType &type, const idx_t &size) {
	auto res = make_buffer<VectorChildBuffer>(Vector(type, size));
	res->size = size;
	res->id = UUID::ToString(UUID::GenerateRandomUUID());
	return res;
}

const Vector &DictionaryVector::GetCachedHashes(Vector &input) {
	D_ASSERT(CanCacheHashes(input));

	auto &child = input.auxiliary->Cast<VectorChildBuffer>();
	lock_guard<mutex> guard(child.cached_hashes_lock);

	auto data_ptr = FlatVector::GetData(child.cached_hashes);
	if (!data_ptr) {
		// Uninitialized: hash the dictionary
		const auto dictionary_size = DictionarySize(input).GetIndex();
		D_ASSERT(!child.size.IsValid() || child.size.GetIndex() == dictionary_size);
		child.cached_hashes.Initialize(false, dictionary_size);
		VectorOperations::Hash(child.data, child.cached_hashes, dictionary_size);
	}
	return child.cached_hashes;
}

} // namespace duckdb
