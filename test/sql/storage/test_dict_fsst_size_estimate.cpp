#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/compression/dict_fsst/analyze.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace duckdb::dict_fsst;

namespace {

//! Size estimate of the DICT_FSST analyze for 'count' distinct values of 'length' bytes each
idx_t EstimateDistinctValues(BlockManager &block_manager, idx_t count, idx_t length) {
	Vector input(LogicalType::VARCHAR, count);
	{
		auto writer = FlatVector::Writer<string_t>(input, count);
		for (idx_t i = 0; i < count; i++) {
			// the index makes the values distinct, the filler gives them their length
			string value = to_string(i);
			value.resize(length, 'x');
			writer.WriteValue(string_t(value));
		}
	}
	DictFSSTAnalyzeState state(block_manager);
	REQUIRE(state.Analyze(input));
	// values this large cannot be FSST encoded, so the estimate is that of the plain dictionary layout
	REQUIRE(state.disable_fsst);
	return state.FinalAnalyze();
}

} // namespace

TEST_CASE("DICT_FSST size estimate charges segments the way the partial block manager stores them", "[storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto path = TestCreatePath("dict_fsst_size_estimate.db");
	DeleteDatabase(path);
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS small (BLOCK_SIZE 16384)"));

	con.BeginTransaction();
	auto &block_manager = StorageManager::Get(Catalog::GetCatalog(*con.context, "small")).GetBlockManager();
	con.Commit();
	// the usable block size is the allocation size minus the block header
	const idx_t block_size = block_manager.GetBlockSize();
	REQUIRE(block_size == 16384 - Storage::DEFAULT_BLOCK_HEADER_SIZE);
	const idx_t partial_block_limit = block_size / 5 * 4;
	const idx_t count = 40;

	// Only one of these values fits in a block, so every value becomes a segment of its own. A segment that takes
	// up to 80% of a block is packed with other segments, so it is charged its own size.
	const idx_t shared_length = 9000;
	const idx_t shared_segment =
	    AlignValue<idx_t>(DictFSSTAnalyzeState::RequiredSpace(1, 1, shared_length, shared_length));
	REQUIRE(shared_segment <= partial_block_limit);
	REQUIRE(EstimateDistinctValues(block_manager, count, shared_length) == count * shared_segment);

	// A segment above that limit gets a block of its own and the rest of the block is lost.
	const idx_t exclusive_length = 14000;
	const idx_t exclusive_segment = DictFSSTAnalyzeState::RequiredSpace(1, 1, exclusive_length, exclusive_length);
	REQUIRE(exclusive_segment > partial_block_limit);
	REQUIRE(exclusive_segment < block_size);
	REQUIRE(EstimateDistinctValues(block_manager, count, exclusive_length) == count * block_size);
}
