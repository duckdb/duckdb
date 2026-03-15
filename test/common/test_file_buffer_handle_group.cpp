#include "catch.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/file_buffer_handle_group.hpp"
#include "test_helpers.hpp"

#include <cstring>

using namespace duckdb;

namespace {
BufferHandle AllocateAndFill(BufferManager &bm, idx_t size, uint8_t fill) {
	auto handle = bm.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, size);
	memset(handle.Ptr(), fill, size);
	return handle;
}
} // namespace

TEST_CASE("FileBufferHandleGroup copy with single handle", "[file_buffer_handle_group]") {
	DuckDB db(":memory:");
	auto &bm = BufferManager::GetBufferManager(*db.instance);

	constexpr idx_t BUF_SIZE = 256;
	auto handle = AllocateAndFill(bm, BUF_SIZE, 0);

	for (idx_t i = 0; i < BUF_SIZE; i++) {
		handle.Ptr()[i] = static_cast<uint8_t>(i & 0xFF);
	}

	FileBufferHandleGroup group;
	group.handles.push_back({std::move(handle), /*start_offset=*/0, /*length=*/BUF_SIZE});

	array<uint8_t, 64> dest {};
	group.CopyTo(dest.data(), dest.size());

	for (idx_t i = 0; i < dest.size(); i++) {
		REQUIRE(dest[i] == static_cast<uint8_t>(i));
	}
}

TEST_CASE("FileBufferHandleGroup copy with single handle with offset", "[file_buffer_handle_group]") {
	DuckDB db(":memory:");
	auto &bm = BufferManager::GetBufferManager(*db.instance);

	constexpr idx_t BUF_SIZE = 256;
	auto handle = AllocateAndFill(bm, BUF_SIZE, 0);
	for (idx_t i = 0; i < BUF_SIZE; i++) {
		handle.Ptr()[i] = static_cast<uint8_t>(i & 0xFF);
	}

	FileBufferHandleGroup group;
	constexpr idx_t OFFSET = 50;
	group.handles.push_back({std::move(handle), OFFSET, BUF_SIZE - OFFSET});

	array<uint8_t, 32> dest {};
	group.CopyTo(dest.data(), dest.size());

	for (idx_t i = 0; i < dest.size(); i++) {
		REQUIRE(dest[i] == static_cast<uint8_t>(OFFSET + i));
	}
}

TEST_CASE("FileBufferHandleGroup copy with multiple handles", "[file_buffer_handle_group]") {
	DuckDB db(":memory:");
	auto &bm = BufferManager::GetBufferManager(*db.instance);

	constexpr idx_t CHUNK = 64;

	auto h1 = AllocateAndFill(bm, CHUNK, 0xAA);
	auto h2 = AllocateAndFill(bm, CHUNK, 0xBB);
	auto h3 = AllocateAndFill(bm, CHUNK, 0xCC);

	FileBufferHandleGroup group;
	group.handles.push_back({std::move(h1), 0, CHUNK});
	group.handles.push_back({std::move(h2), 0, CHUNK});
	group.handles.push_back({std::move(h3), 0, CHUNK});

	array<uint8_t, CHUNK * 3> dest {};
	group.CopyTo(dest.data(), dest.size());

	for (idx_t i = 0; i < CHUNK; i++) {
		REQUIRE(dest[i] == 0xAA);
	}
	for (idx_t i = CHUNK; i < CHUNK * 2; i++) {
		REQUIRE(dest[i] == 0xBB);
	}
	for (idx_t i = CHUNK * 2; i < CHUNK * 3; i++) {
		REQUIRE(dest[i] == 0xCC);
	}
}

TEST_CASE("FileBufferHandleGroup copy partial across handles", "[file_buffer_handle_group]") {
	DuckDB db(":memory:");
	auto &bm = BufferManager::GetBufferManager(*db.instance);

	constexpr idx_t BUF_SIZE = 64;
	constexpr idx_t H1_OFFSET = 20;
	constexpr idx_t H1_LENGTH = BUF_SIZE - H1_OFFSET; // 44 bytes from first block
	constexpr idx_t H2_LENGTH = 16;                    // 16 bytes from second block
	constexpr idx_t COPY_SIZE = H1_LENGTH + H2_LENGTH;  // 60 total

	auto h1 = AllocateAndFill(bm, BUF_SIZE, 0x11);
	auto h2 = AllocateAndFill(bm, BUF_SIZE, 0x22);

	FileBufferHandleGroup group;
	group.handles.push_back({std::move(h1), /*start_offset=*/H1_OFFSET, /*length=*/H1_LENGTH});
	group.handles.push_back({std::move(h2), /*start_offset=*/0, /*length=*/H2_LENGTH});

	array<uint8_t, COPY_SIZE> dest {};
	group.CopyTo(dest.data(), dest.size());

	for (idx_t i = 0; i < H1_LENGTH; i++) {
		REQUIRE(dest[i] == 0x11);
	}
	for (idx_t i = H1_LENGTH; i < COPY_SIZE; i++) {
		REQUIRE(dest[i] == 0x22);
	}
}
