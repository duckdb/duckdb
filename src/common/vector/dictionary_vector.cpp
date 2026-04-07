#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

buffer_ptr<DictionaryEntry> DictionaryVector::CreateReusableDictionary(const LogicalType &type, const idx_t &size) {
	auto entry = make_buffer<DictionaryEntry>(Vector(type, size));
	entry->size = size;
	entry->id = UUID::ToString(UUID::GenerateRandomUUID());
	return entry;
}

const Vector &DictionaryVector::GetCachedHashes(Vector &input) {
	D_ASSERT(CanCacheHashes(input));

	auto &entry = input.buffer->Cast<DictionaryBuffer>().GetEntry();
	lock_guard<mutex> guard(entry.cached_hashes_lock);

	if (!entry.cached_hashes) {
		// Uninitialized: hash the dictionary
		const auto dictionary_size = DictionarySize(input).GetIndex();
		D_ASSERT(!entry.size.IsValid() || entry.size.GetIndex() == dictionary_size);
		entry.cached_hashes = make_uniq<Vector>(LogicalType::HASH, dictionary_size);
		VectorOperations::Hash(entry.data, *entry.cached_hashes, dictionary_size);
	}
	return *entry.cached_hashes;
}

} // namespace duckdb
