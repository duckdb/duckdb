//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/index/art/node.hpp"
namespace duckdb {
class Prefix {
public:
	explicit Prefix(uint32_t prefix_length);

	// Returns the Prefix's size
	uint32_t Size();

	// Subscript operator
	uint8_t &operator[](idx_t idx);
	// Assing operator
	void operator=(Prefix &src);
	// Reduces the prefix in n elements
	void Reduce(uint32_t n);
	// Serializes Prefix
	void Serialize(duckdb::MetaBlockWriter &writer);
	// Deserializes Prefix
	void Deserialize(duckdb::MetaBlockReader &reader);
	// If prefix equals key (starting from depth)
	bool EqualKey(Key &key, unsigned depth);
	// If prefix > key (starting from depth)
	bool GTKey(Key &key, unsigned depth);
	// If prefix >= key (starting from depth)
	bool GTEKey(Key &key, unsigned depth);

	// Compare the key with the prefix of the node, return the number matching bytes
	uint32_t KeyMismatch(Key &key, uint64_t depth);

private:
	unique_ptr<uint8_t[]> prefix;
};

} // namespace duckdb
