#include "test_capi_v2.hpp"

#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include <cstdint>
#include <cstring>
#include <thread>

// ---------------------------------------------------------------------------
// V2 allocator: an extension's allocator, its alloc / alloc_zeroed / dealloc / realloc, and how its memory is
// accounted for in duckdb_extension_memory(), duckdb_memory() and against memory_limit.
// ---------------------------------------------------------------------------

#if defined(__SANITIZE_ADDRESS__)
#define TEST_CAPI_V2_ALLOCATOR_ASAN
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define TEST_CAPI_V2_ALLOCATOR_ASAN
#endif
#endif

static thread_local duckdb_v2_allocator_handle loaded_allocator = nullptr;

extern "C" void capi_v2_allocator_test_init_c_api_v2(duckdb_v2_extension_input *input) {
	duckdb_v2_allocator_create_with_extension(input->extension, &loaded_allocator, input->err);
}

extern "C" void capi_v2_context_allocator_test_init_c_api_v2(duckdb_v2_extension_input *input) {
	duckdb_v2_allocator_create_with_context(input->context, &loaded_allocator, input->err);
}

namespace test_capi_v2 {
namespace {

constexpr const char *EXTENSION_NAME = "capi_v2_allocator_test";
constexpr idx_t MIB = 1 << 20;

constexpr idx_t ALIGNMENTS[] = {1, 2, 4, 8, 16, 32, 64, 128, 4096, 65536};
// Sizes below, at and above the alignments.
constexpr idx_t SIZES[] = {1, 3, 4, 7, 8, 15, 16, 17, 100, 4097, 1 << 20};

duckdb_v2_allocator_handle LoadExtension(duckdb::DuckDB &db, const char *name = EXTENSION_NAME) {
	loaded_allocator = nullptr;
	db.LoadStaticCAPIExtensionV2(name, capi_v2_allocator_test_init_c_api_v2);
	REQUIRE(loaded_allocator != nullptr);
	return loaded_allocator;
}

DUCKDB_V2_ERROR RegisterOnLoad(duckdb::DuckDB &db, const char *name, duckdb_v2_allocator_handle allocator) {
	static thread_local duckdb::optional<DUCKDB_V2_ERROR> result;
	loaded_allocator = allocator;
	result.reset();
	db.LoadStaticCAPIExtensionV2(name, [](duckdb_v2_extension_input *input) {
		result = duckdb_v2_allocator_register_extension(loaded_allocator, input->extension, nullptr);
	});
	REQUIRE(result.has_value());
	return *result;
}

struct AllocatorFixture {
	duckdb::DuckDB db {nullptr};
	duckdb_v2_allocator_handle allocator = LoadExtension(db);

	~AllocatorFixture() {
		duckdb_v2_allocator_destroy(&allocator);
	}
};

int64_t QueryBigint(duckdb::DuckDB &db, const std::string &sql) {
	duckdb::Connection con(db);
	auto result = con.Query(sql);
	REQUIRE_NO_FAIL(*result);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	return chunk->GetValue(0, 0).GetValue<int64_t>();
}

int64_t ExtensionMemory(duckdb::DuckDB &db, const char *name = EXTENSION_NAME) {
	return QueryBigint(db, std::string("SELECT memory_usage_bytes FROM duckdb_extension_memory() WHERE "
	                                   "extension_name = '") +
	                           name + "'");
}

int64_t ExtensionTagMemory(duckdb::DuckDB &db) {
	return QueryBigint(db, "SELECT memory_usage_bytes FROM duckdb_memory() WHERE tag = 'EXTENSION'");
}

bool IsAligned(const void *ptr, idx_t align) {
	return reinterpret_cast<uintptr_t>(ptr) % align == 0;
}

void FillPattern(void *ptr, idx_t size) {
	auto bytes = static_cast<uint8_t *>(ptr);
	for (idx_t i = 0; i < size; i++) {
		bytes[i] = static_cast<uint8_t>(i * 31 + 7);
	}
}

bool HasPattern(const void *ptr, idx_t size) {
	auto bytes = static_cast<const uint8_t *>(ptr);
	for (idx_t i = 0; i < size; i++) {
		if (bytes[i] != static_cast<uint8_t>(i * 31 + 7)) {
			return false;
		}
	}
	return true;
}

bool IsZeroed(const void *ptr, idx_t size) {
	auto bytes = static_cast<const uint8_t *>(ptr);
	for (idx_t i = 0; i < size; i++) {
		if (bytes[i] != 0) {
			return false;
		}
	}
	return true;
}

DUCKDB_V2_ERROR ErrorCode(duckdb_v2_error_info_handle err) {
	auto code = static_cast<DUCKDB_V2_ERROR>(DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_error_info_get_code(err, &code) == DUCKDB_V2_ERROR_NONE);
	return code;
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------
// Allocator handles
//----------------------------------------------------------------------------------------------------------------------

TEST_CASE("V2: allocator creation and destruction handle null arguments", "[capi_v2][allocator]") {
	duckdb_v2_allocator_handle allocator = reinterpret_cast<duckdb_v2_allocator_handle>(0x1);
	SECTION("extension") {
		REQUIRE(duckdb_v2_allocator_create_with_extension(nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_allocator_create_with_extension(nullptr, &allocator, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(allocator != nullptr);
		REQUIRE(duckdb_v2_allocator_register_extension(allocator, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_allocator_destroy(&allocator) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("connection") {
		REQUIRE(duckdb_v2_allocator_create_with_connection(nullptr, &allocator, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_allocator_create_with_connection(nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("context") {
		REQUIRE(duckdb_v2_allocator_create_with_context(nullptr, &allocator, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_allocator_create_with_context(nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	REQUIRE(allocator == nullptr);
	REQUIRE(duckdb_v2_allocator_destroy(&allocator) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
}

//----------------------------------------------------------------------------------------------------------------------
// alloc / alloc_zeroed / dealloc / realloc
//----------------------------------------------------------------------------------------------------------------------

TEST_CASE("V2: alloc and dealloc honor size and alignment", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	for (auto align : ALIGNMENTS) {
		for (auto size : SIZES) {
			void *ptr = nullptr;
			REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, size, align, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
			REQUIRE(ptr != nullptr);
			REQUIRE(IsAligned(ptr, align));
			FillPattern(ptr, size);
			REQUIRE(HasPattern(ptr, size));
			REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, ptr, size, align) == DUCKDB_V2_ERROR_NONE);
		}
	}
}

TEST_CASE("V2: alloc_zeroed returns zeroed memory", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	for (auto align : ALIGNMENTS) {
		for (auto size : SIZES) {
			// Dirty and release a block of the same layout first, so a recycled block is not zero by chance.
			void *dirty = nullptr;
			REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, size, align, &dirty, nullptr) == DUCKDB_V2_ERROR_NONE);
			memset(dirty, 0xAB, size);
			REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, dirty, size, align) == DUCKDB_V2_ERROR_NONE);

			void *ptr = nullptr;
			REQUIRE(duckdb_v2_allocator_alloc_zeroed(fx.allocator, size, align, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
			REQUIRE(ptr != nullptr);
			REQUIRE(IsAligned(ptr, align));
			REQUIRE(IsZeroed(ptr, size));
			REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, ptr, size, align) == DUCKDB_V2_ERROR_NONE);
		}
	}
}

TEST_CASE("V2: zero-sized allocations are NULL", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	void *ptr = reinterpret_cast<void *>(0x1);
	REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, 0, 8, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ptr == nullptr);

	ptr = reinterpret_cast<void *>(0x1);
	REQUIRE(duckdb_v2_allocator_alloc_zeroed(fx.allocator, 0, 4096, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ptr == nullptr);

	REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, nullptr, 0, 8) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, nullptr, 16, 4096) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: allocator rejects invalid alignments", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	for (idx_t align : {idx_t(0), idx_t(3), idx_t(24), idx_t(1000)}) {
		void *ptr = reinterpret_cast<void *>(0x1);
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, 16, align, &ptr, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(ptr == nullptr);
		REQUIRE(err != nullptr);
		REQUIRE(ErrorCode(err) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_error_info_destroy(&err);

		ptr = reinterpret_cast<void *>(0x1);
		REQUIRE(duckdb_v2_allocator_alloc_zeroed(fx.allocator, 16, align, &ptr, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(ptr == nullptr);

		REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, nullptr, 16, align) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}

	// realloc leaves the original allocation untouched on a bad alignment.
	void *ptr = nullptr;
	REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, 16, 8, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	FillPattern(ptr, 16);
	void *out = reinterpret_cast<void *>(0x1);
	REQUIRE(duckdb_v2_allocator_realloc(fx.allocator, ptr, 16, 3, 32, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(HasPattern(ptr, 16));
	REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, ptr, 16, 8) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: allocator null args", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	void *ptr = reinterpret_cast<void *>(0x1);
	REQUIRE(duckdb_v2_allocator_alloc(nullptr, 16, 8, &ptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(ptr == nullptr);
	ptr = reinterpret_cast<void *>(0x1);
	REQUIRE(duckdb_v2_allocator_alloc_zeroed(nullptr, 16, 8, &ptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(ptr == nullptr);
	ptr = reinterpret_cast<void *>(0x1);
	REQUIRE(duckdb_v2_allocator_realloc(nullptr, nullptr, 0, 8, 16, &ptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(ptr == nullptr);
	REQUIRE(duckdb_v2_allocator_dealloc(nullptr, nullptr, 16, 8) == DUCKDB_V2_ERROR_INPUT_INVALID);

	REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, 16, 8, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_allocator_alloc_zeroed(fx.allocator, 16, 8, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_allocator_realloc(fx.allocator, nullptr, 0, 8, 16, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2: realloc preserves contents and alignment", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	for (auto align : ALIGNMENTS) {
		for (auto old_size : SIZES) {
			for (auto new_size : SIZES) {
				void *ptr = nullptr;
				REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, old_size, align, &ptr, nullptr) ==
				        DUCKDB_V2_ERROR_NONE);
				FillPattern(ptr, old_size);

				void *resized = nullptr;
				REQUIRE(duckdb_v2_allocator_realloc(fx.allocator, ptr, old_size, align, new_size, &resized, nullptr) ==
				        DUCKDB_V2_ERROR_NONE);
				REQUIRE(resized != nullptr);
				REQUIRE(IsAligned(resized, align));
				REQUIRE(HasPattern(resized, old_size < new_size ? old_size : new_size));
				// The whole new range is writable.
				memset(resized, 0xCD, new_size);
				REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, resized, new_size, align) == DUCKDB_V2_ERROR_NONE);
			}
		}
	}
}

TEST_CASE("V2: realloc from and to zero size", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	for (auto align : ALIGNMENTS) {
		// A NULL pointer with size 0 allocates.
		void *ptr = nullptr;
		REQUIRE(duckdb_v2_allocator_realloc(fx.allocator, nullptr, 0, align, 100, &ptr, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(ptr != nullptr);
		REQUIRE(IsAligned(ptr, align));
		FillPattern(ptr, 100);

		// A new size of 0 releases.
		void *out = reinterpret_cast<void *>(0x1);
		REQUIRE(duckdb_v2_allocator_realloc(fx.allocator, ptr, 100, align, 0, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out == nullptr);

		out = reinterpret_cast<void *>(0x1);
		REQUIRE(duckdb_v2_allocator_realloc(fx.allocator, nullptr, 0, align, 0, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out == nullptr);
	}
}

TEST_CASE("V2: realloc rejects a mismatched pointer and size", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	void *out = reinterpret_cast<void *>(0x1);
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_allocator_realloc(fx.allocator, nullptr, 16, 8, 32, &out, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	void *ptr = nullptr;
	REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, 16, 8, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	FillPattern(ptr, 16);
	out = reinterpret_cast<void *>(0x1);
	REQUIRE(duckdb_v2_allocator_realloc(fx.allocator, ptr, 0, 8, 32, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(HasPattern(ptr, 16));
	REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, ptr, 16, 8) == DUCKDB_V2_ERROR_NONE);
}

// ASan aborts on oversized requests instead of letting them fail.
#ifndef TEST_CAPI_V2_ALLOCATOR_ASAN
TEST_CASE("V2: allocator reports out of memory", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	for (idx_t huge_size : {idx_t(1) << 47, idx_t(1) << 62, ~idx_t(0)}) {
		for (idx_t align : {idx_t(8), idx_t(4096), idx_t(1) << 63}) {
			void *ptr = reinterpret_cast<void *>(0x1);
			duckdb_v2_error_info_handle err = nullptr;
			REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, huge_size, align, &ptr, &err) ==
			        DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY);
			REQUIRE(ptr == nullptr);
			REQUIRE(err != nullptr);
			REQUIRE(ErrorCode(err) == DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY);
			duckdb_v2_error_info_destroy(&err);

			ptr = reinterpret_cast<void *>(0x1);
			REQUIRE(duckdb_v2_allocator_alloc_zeroed(fx.allocator, huge_size, align, &ptr, nullptr) ==
			        DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY);
			REQUIRE(ptr == nullptr);
		}

		// A failed realloc keeps the original allocation.
		for (idx_t align : {idx_t(8), idx_t(4096)}) {
			void *original = nullptr;
			REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, 64, align, &original, nullptr) == DUCKDB_V2_ERROR_NONE);
			FillPattern(original, 64);
			void *out = reinterpret_cast<void *>(0x1);
			REQUIRE(duckdb_v2_allocator_realloc(fx.allocator, original, 64, align, huge_size, &out, nullptr) ==
			        DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY);
			REQUIRE(out == nullptr);
			REQUIRE(HasPattern(original, 64));
			REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, original, 64, align) == DUCKDB_V2_ERROR_NONE);
		}
	}
}
#endif

//----------------------------------------------------------------------------------------------------------------------
// Accounting
//----------------------------------------------------------------------------------------------------------------------

TEST_CASE("V2: allocator memory shows in duckdb_extension_memory()", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	// The counter is process-wide, so measure against what it holds already.
	auto baseline = ExtensionMemory(fx.db);

	void *plain = nullptr;
	REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, 3 * MIB, 8, &plain, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ExtensionMemory(fx.db) == baseline + int64_t(3 * MIB));

	// Over-aligned allocations are padded by their alignment.
	void *aligned = nullptr;
	REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, MIB, 4096, &aligned, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ExtensionMemory(fx.db) == baseline + int64_t(4 * MIB + 4096));

	// Small requests are rounded up to their alignment.
	void *small = nullptr;
	REQUIRE(duckdb_v2_allocator_alloc(fx.allocator, 1, 8, &small, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ExtensionMemory(fx.db) == baseline + int64_t(4 * MIB + 4096 + 8));

	void *resized = nullptr;
	REQUIRE(duckdb_v2_allocator_realloc(fx.allocator, plain, 3 * MIB, 8, MIB, &resized, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(ExtensionMemory(fx.db) == baseline + int64_t(2 * MIB + 4096 + 8));

	REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, resized, MIB, 8) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, aligned, MIB, 4096) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_dealloc(fx.allocator, small, 1, 8) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ExtensionMemory(fx.db) == baseline);
}

TEST_CASE("V2: duckdb_extension_memory() lists only extensions loaded in the database", "[capi_v2][allocator]") {
	duckdb::DuckDB db(nullptr);
	REQUIRE(QueryBigint(db, "SELECT count(*) FROM duckdb_extension_memory()") == 0);
	auto allocator = LoadExtension(db);
	REQUIRE(QueryBigint(db, std::string("SELECT count(*) FROM duckdb_extension_memory() WHERE extension_name = '") +
	                            EXTENSION_NAME + "'") == 1);
	REQUIRE(duckdb_v2_allocator_destroy(&allocator) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: extension allocators share a counter, not a database lifetime or budget", "[capi_v2][allocator]") {
	duckdb_v2_allocator_handle allocator;
	duckdb::weak_ptr<duckdb::DatabaseInstance> lifetime;
	void *ptr = nullptr;
	{
		duckdb::DBConfig config;
		config.options.maximum_memory = 16 * MIB;
		duckdb::DuckDB db(nullptr, &config);
		lifetime = db.instance;
		allocator = LoadExtension(db);
		auto baseline = ExtensionMemory(db);
		auto db_baseline = ExtensionTagMemory(db);
		REQUIRE(duckdb_v2_allocator_alloc(allocator, 32 * MIB, 8, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
		AllocatorFixture second;
		REQUIRE(ExtensionMemory(second.db) == baseline + int64_t(32 * MIB));
		REQUIRE(ExtensionTagMemory(second.db) == 0);
		REQUIRE(duckdb_v2_allocator_destroy(&second.allocator) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(second.allocator == nullptr);
		REQUIRE(ExtensionMemory(db) == baseline + int64_t(32 * MIB));
		REQUIRE(ExtensionTagMemory(db) == db_baseline);
	}
	REQUIRE(lifetime.expired());
	void *resized = nullptr;
	REQUIRE(duckdb_v2_allocator_realloc(allocator, ptr, 32 * MIB, 8, MIB, &resized, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_ERROR rc;
	std::thread worker([&] { rc = duckdb_v2_allocator_dealloc(allocator, resized, MIB, 8); });
	worker.join();
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_destroy(&allocator) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(allocator == nullptr);
}

TEST_CASE("V2: a static extension can attribute an existing allocator during load", "[capi_v2][allocator]") {
	duckdb_v2_allocator_handle early = nullptr;
	REQUIRE(duckdb_v2_allocator_create_with_extension(nullptr, &early, nullptr) == DUCKDB_V2_ERROR_NONE);
	void *ptr = nullptr;
	REQUIRE(duckdb_v2_allocator_alloc(early, 128, 8, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	FillPattern(ptr, 128);

	duckdb_v2_allocator_handle other = nullptr;
	REQUIRE(duckdb_v2_allocator_create_with_extension(nullptr, &other, nullptr) == DUCKDB_V2_ERROR_NONE);
	void *other_ptr = nullptr;
	REQUIRE(duckdb_v2_allocator_alloc(other, 64, 8, &other_ptr, nullptr) == DUCKDB_V2_ERROR_NONE);

	const char *name = "capi_v2_allocator_late_registration";
	duckdb::DuckDB first(nullptr);
	REQUIRE(RegisterOnLoad(first, name, other) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ExtensionMemory(first, name) == 64);
	{
		duckdb::DuckDB second(nullptr);
		REQUIRE(RegisterOnLoad(second, name, early) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(ExtensionMemory(second, name) == 192);
	}
	{
		duckdb::DuckDB third(nullptr);
		REQUIRE(RegisterOnLoad(third, name, early) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RegisterOnLoad(third, "capi_v2_allocator_wrong_extension", early) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	REQUIRE(ExtensionMemory(first, name) == 192);
	void *resized = nullptr;
	REQUIRE(duckdb_v2_allocator_realloc(early, ptr, 128, 8, 256, &resized, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(HasPattern(resized, 128));
	REQUIRE(ExtensionMemory(first, name) == 320);
	REQUIRE(duckdb_v2_allocator_dealloc(early, resized, 256, 8) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_destroy(&early) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ExtensionMemory(first, name) == 64);
	REQUIRE(duckdb_v2_allocator_dealloc(other, other_ptr, 64, 8) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_destroy(&other) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ExtensionMemory(first, name) == 0);
}

TEST_CASE("V2: different extensions have independent counters", "[capi_v2][allocator]") {
	AllocatorFixture fx;
	const char *other_name = "capi_v2_other_allocator_test";
	auto other = LoadExtension(fx.db, other_name);
	auto baseline = ExtensionMemory(fx.db);
	void *ptr = nullptr;
	REQUIRE(duckdb_v2_allocator_alloc(other, MIB, 8, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ExtensionMemory(fx.db, other_name) == int64_t(MIB));
	REQUIRE(ExtensionMemory(fx.db) == baseline);
	REQUIRE(duckdb_v2_allocator_dealloc(other, ptr, MIB, 8) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_destroy(&other) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: database allocators reserve only from their database and outlive their source", "[capi_v2][allocator]") {
	duckdb_v2_allocator_handle allocator = nullptr;
	duckdb::weak_ptr<duckdb::DatabaseInstance> lifetime;
	{
		EnvFixture fx;
		auto &db = duckdb::capiv2::Convert(fx.instance)->GetDatabase();
		lifetime = db.instance;
		REQUIRE_NO_FAIL(*duckdb::Connection(db).Query("SET memory_limit='16MiB'"));
		SECTION("connection") {
			REQUIRE(duckdb_v2_allocator_create_with_connection(fx.conn, &allocator, nullptr) == DUCKDB_V2_ERROR_NONE);
		}
		SECTION("callback context") {
			loaded_allocator = nullptr;
			db.LoadStaticCAPIExtensionV2("capi_v2_context_allocator_test",
			                             capi_v2_context_allocator_test_init_c_api_v2);
			allocator = loaded_allocator;
		}
		REQUIRE(allocator != nullptr);
		REQUIRE(RegisterOnLoad(db, "capi_v2_allocator_db_registration", allocator) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(RegisterOnLoad(db, "capi_v2_allocator_null_registration", nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_connection_destroy(&fx.conn) == DUCKDB_V2_ERROR_NONE);
		AllocatorFixture other;
		auto baseline = ExtensionTagMemory(db);
		auto global = ExtensionMemory(other.db);
		void *ptr = nullptr;
		constexpr idx_t align = 4096;
		REQUIRE(duckdb_v2_allocator_alloc(allocator, 32 * MIB, align, &ptr, nullptr) ==
		        DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY);
		REQUIRE(ptr == nullptr);
		REQUIRE(duckdb_v2_allocator_alloc(allocator, MIB, align, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
		FillPattern(ptr, MIB);
		void *out = nullptr;
		REQUIRE(duckdb_v2_allocator_realloc(allocator, ptr, MIB, align, 32 * MIB, &out, nullptr) ==
		        DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY);
		REQUIRE(out == nullptr);
		REQUIRE(HasPattern(ptr, MIB));
		REQUIRE(ExtensionTagMemory(db) == baseline + int64_t(MIB + align));
		REQUIRE(duckdb_v2_allocator_realloc(allocator, ptr, MIB, align, 2 * MIB, &out, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(ExtensionTagMemory(db) == baseline + int64_t(2 * MIB + align));
		REQUIRE(duckdb_v2_allocator_realloc(allocator, out, 2 * MIB, align, MIB, &ptr, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(ExtensionTagMemory(db) == baseline + int64_t(MIB + align));
		REQUIRE(HasPattern(ptr, MIB));
		REQUIRE(ExtensionTagMemory(other.db) == 0);
		REQUIRE(ExtensionMemory(other.db) == global);
		REQUIRE(duckdb_v2_allocator_dealloc(allocator, ptr, MIB, align) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(ExtensionTagMemory(db) == baseline);
	}
	REQUIRE_FALSE(lifetime.expired());
	void *ptr = nullptr;
	REQUIRE(duckdb_v2_allocator_alloc(allocator, 64, 8, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_dealloc(allocator, ptr, 64, 8) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_destroy(&allocator) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(lifetime.expired());
}

TEST_CASE("V2: a failed backing realloc refunds the database reservation", "[capi_v2][allocator]") {
	constexpr idx_t fail_size = MIB + 17;
	auto realloc = [](duckdb::PrivateAllocatorData *data, duckdb::data_ptr_t ptr, idx_t old_size, idx_t size) {
		return size == fail_size ? nullptr : duckdb::Allocator::DefaultReallocate(data, ptr, old_size, size);
	};
	duckdb::DBConfig config;
	config.allocator = duckdb::make_uniq<duckdb::Allocator>(duckdb::Allocator::DefaultAllocate,
	                                                        duckdb::Allocator::DefaultFree, realloc, nullptr);
	duckdb::DuckDB db(nullptr, &config);
	loaded_allocator = nullptr;
	db.LoadStaticCAPIExtensionV2("capi_v2_context_allocator_test", capi_v2_context_allocator_test_init_c_api_v2);
	auto allocator = loaded_allocator;
	void *ptr = nullptr;
	REQUIRE(duckdb_v2_allocator_alloc(allocator, 64, 8, &ptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	FillPattern(ptr, 64);
	auto charged = ExtensionTagMemory(db);
	void *out = nullptr;
	REQUIRE(duckdb_v2_allocator_realloc(allocator, ptr, 64, 8, fail_size, &out, nullptr) ==
	        DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY);
	REQUIRE(out == nullptr);
	REQUIRE(HasPattern(ptr, 64));
	REQUIRE(ExtensionTagMemory(db) == charged);
	REQUIRE(duckdb_v2_allocator_dealloc(allocator, ptr, 64, 8) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_allocator_destroy(&allocator) == DUCKDB_V2_ERROR_NONE);
}

} // namespace test_capi_v2
