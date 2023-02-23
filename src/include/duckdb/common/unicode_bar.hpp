//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unicode_bar.hpp
//
//
//===----------------------------------------------------------------------===//

namespace duckdb {
struct UnicodeBar {
private:
	static constexpr idx_t PARTIAL_BLOCKS_COUNT = 8;

public:
	static constexpr idx_t PartialBlocksCount() {
		return PARTIAL_BLOCKS_COUNT;
	}

	static const char *const *PartialBlocks() {
		static const char *PARTIAL_BLOCKS[PARTIAL_BLOCKS_COUNT] = {" ",
		                                                           "\xE2\x96\x8F",
		                                                           "\xE2\x96\x8E",
		                                                           "\xE2\x96\x8D",
		                                                           "\xE2\x96\x8C",
		                                                           "\xE2\x96\x8B",
		                                                           "\xE2\x96\x8A",
		                                                           "\xE2\x96\x89"};
		return PARTIAL_BLOCKS;
	}

	static const char *FullBlock() {
		return "\xE2\x96\x88";
	}
};
} // namespace duckdb
