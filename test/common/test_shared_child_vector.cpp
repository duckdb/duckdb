#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector/shared_child_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "test_helpers.hpp"

#include <thread>

// SharedChildStorage and its views are vector buffers. SQL cannot tell sharing from copying except by timing, and
// whether a write through the vector buffer API reaches shared storage, pins, block lifetimes, allocation sizes and
// claim interleavings are not observable from SQL, so these invariants are pinned at the C++ level.

using namespace duckdb;

namespace {

//! 'count' BIGINTs base, base + 1, ...
Vector MakeSource(idx_t count, int64_t base) {
	Vector source(LogicalType::BIGINT, count);
	auto data = FlatVector::GetDataMutable<int64_t>(source);
	for (idx_t i = 0; i < count; i++) {
		data[i] = base + int64_t(i);
	}
	FlatVector::SetSize(source, count_t(count));
	return source;
}

shared_ptr<SharedChildStorage> MakeStorage(BufferManager &buffer_manager, idx_t count, int64_t base) {
	auto source = MakeSource(count, base);
	return SharedChildVector::Materialize(buffer_manager, source, *FlatVector::IncrementalSelectionVector(), 0, count);
}

Vector MakeView(const shared_ptr<SharedChildStorage> &storage, idx_t count) {
	Vector view(LogicalType::BIGINT);
	SharedChildVector::SetView(view, storage, count);
	return view;
}

} // namespace

TEST_CASE("Shared child storage only holds NULL-free entries", "[shared_child_vector]") {
	DuckDB db(nullptr);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const idx_t count = 3000;
	auto &incremental = *FlatVector::IncrementalSelectionVector();
	auto source = MakeSource(count + 1, 1);
	FlatVector::ValidityMutable(source).SetInvalid(count);
	// a NULL among the selected entries is not shared
	REQUIRE(!SharedChildVector::Materialize(buffer_manager, source, incremental, 0, count + 1));
	// an allocated mask whose selected entries are all valid is
	auto storage = SharedChildVector::Materialize(buffer_manager, source, incremental, 0, count);
	REQUIRE(storage);
	REQUIRE(MakeView(storage, count).GetValue(count - 1) == Value::BIGINT(int64_t(count)));

	// an extension with a NULL among the appended entries is concatenated the regular way
	auto prefix = MakeView(storage, count);
	auto extra_source = MakeSource(2, 7);
	FlatVector::ValidityMutable(extra_source).SetInvalid(1);
	Vector result(LogicalType::BIGINT);
	idx_t offset;
	vector<SharedChildExtra> with_null {SharedChildExtra {extra_source, list_entry_t {0, 2}}};
	REQUIRE(!SharedChildVector::TryExtend(buffer_manager, prefix, list_entry_t {0, count}, with_null, result, offset));
	vector<SharedChildExtra> valid {SharedChildExtra {extra_source, list_entry_t {0, 1}}};
	REQUIRE(SharedChildVector::TryExtend(buffer_manager, prefix, list_entry_t {0, count}, valid, result, offset));
	REQUIRE(result.GetValue(offset + count) == Value::BIGINT(7));
}

TEST_CASE("Writes to shared child views never reach the storage", "[shared_child_vector]") {
	DuckDB db(nullptr);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const idx_t count = 3000;
	auto storage = MakeStorage(buffer_manager, count, 1);
	auto check_storage = [&]() {
		auto fresh = MakeView(storage, count);
		REQUIRE(fresh.GetValue(0) == Value::BIGINT(1));
		REQUIRE(fresh.GetValue(1) == Value::BIGINT(2));
		REQUIRE(fresh.GetValue(2999) == Value::BIGINT(3000));
		REQUIRE(SharedChildVector::GetUnchangedStorage(fresh) == storage);
	};

	// a value write copies the entries
	auto value_view = MakeView(storage, count);
	REQUIRE(SharedChildVector::GetUnchangedStorage(value_view) == storage);
	value_view.SetValue(0, Value::BIGINT(99));
	REQUIRE(value_view.GetValue(0) == Value::BIGINT(99));
	REQUIRE(!SharedChildVector::GetUnchangedStorage(value_view));
	check_storage();

	// a NULL write allocates a mask on the view, the values stay shared
	auto null_view = MakeView(storage, count);
	auto shared_data = FlatVector::GetData(null_view);
	null_view.SetValue(1, Value(LogicalType::BIGINT));
	REQUIRE(null_view.GetValue(1).IsNull());
	REQUIRE(FlatVector::GetData(null_view) == shared_data);
	REQUIRE(!SharedChildVector::GetUnchangedStorage(null_view));
	check_storage();

	// a slice is a view itself, writing through it copies the slice only
	auto parent = MakeView(storage, count);
	Vector slice(parent, 1, 11);
	FlatVector::GetDataMutable<int64_t>(slice)[0] = 99;
	REQUIRE(slice.GetValue(0) == Value::BIGINT(99));
	REQUIRE(SharedChildVector::GetUnchangedStorage(parent) == storage);
	check_storage();

	// copying into a view copies its entries first
	auto copy_view = MakeView(storage, count);
	auto copy_source = MakeSource(10, -100);
	VectorOperations::Copy(copy_source, copy_view, 10, 0, 0);
	REQUIRE(copy_view.GetValue(9) == Value::BIGINT(-91));
	REQUIRE(copy_view.GetValue(10) == Value::BIGINT(11));
	check_storage();

	// the validity a view forwards to another vector is the view's own empty mask, a NULL written to that vector
	// stays there
	auto forward_view = MakeView(storage, count);
	Vector derived(LogicalType::BIGINT, count);
	FlatVector::CopyValidity(derived, forward_view, count);
	FlatVector::ValidityMutable(derived).SetInvalid(7);
	REQUIRE(FlatVector::Validity(forward_view).RowIsValid(7));
	REQUIRE(SharedChildVector::GetUnchangedStorage(forward_view) == storage);
	check_storage();
}

TEST_CASE("Slices of shared child views keep NULLs written to the view", "[shared_child_vector]") {
	DuckDB db(nullptr);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const idx_t count = 3000;
	auto storage = MakeStorage(buffer_manager, count, 1);
	for (auto through_mask : {false, true}) {
		auto view = MakeView(storage, count);
		if (through_mask) {
			FlatVector::ValidityMutable(view).SetInvalid(1);
		} else {
			view.SetValue(1, Value(LogicalType::BIGINT));
		}
		Vector from_start(view, 0, 10);
		REQUIRE(from_start.GetValue(1).IsNull());
		REQUIRE(from_start.GetValue(2) == Value::BIGINT(3));
		REQUIRE(!SharedChildVector::GetUnchangedStorage(from_start));
		Vector offset(view, 1, 11);
		REQUIRE(offset.GetValue(0).IsNull());
		REQUIRE(offset.GetValue(1) == Value::BIGINT(3));
		REQUIRE(MakeView(storage, count).GetValue(1) == Value::BIGINT(2));
	}
}

TEST_CASE("Read access to shared child views does not copy", "[shared_child_vector]") {
	DuckDB db(nullptr);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const idx_t count = 3000;
	auto storage = MakeStorage(buffer_manager, count, 1);
	auto view = MakeView(storage, count);
	auto shared_data = FlatVector::GetData(view);
	// the mutable validity is the view's own, handing it out copies nothing and keeps the view a prefix
	REQUIRE(FlatVector::ValidityMutable(view).RowIsValid(7));
	REQUIRE(FlatVector::GetData(view) == shared_data);
	REQUIRE(SharedChildVector::GetUnchangedStorage(view) == storage);

	// growing a view copies its entries once into the larger buffer
	view.Reserve(5000);
	REQUIRE(FlatVector::GetData(view) != shared_data);
	REQUIRE(view.GetValue(2999) == Value::BIGINT(3000));
	REQUIRE(!SharedChildVector::GetUnchangedStorage(view));
}

TEST_CASE("Detached shared child views release the block", "[shared_child_vector]") {
	DuckDB db(nullptr);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const idx_t count = 1024 * 1024;
	const auto before = buffer_manager.GetUsedMemory();
	auto storage = MakeStorage(buffer_manager, count, 1);
	const auto block_size = storage->AllocationSize();
	REQUIRE(buffer_manager.GetUsedMemory() >= before + block_size);

	auto parent = make_uniq<Vector>(MakeView(storage, count));
	Vector slice(*parent, 1, 11);
	slice.SetValue(0, Value::BIGINT(99));
	parent.reset();
	storage.reset();
	// only the detached ten-entry slice is left, nothing references or pins the 8 MiB block anymore
	REQUIRE(buffer_manager.GetUsedMemory() < before + block_size);
	REQUIRE(slice.GetValue(0) == Value::BIGINT(99));
	REQUIRE(slice.GetValue(9) == Value::BIGINT(11));
}

TEST_CASE("Only the view that created shared storage accounts it", "[shared_child_vector]") {
	DuckDB db(nullptr);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const idx_t count = 1024 * 1024;
	auto storage = MakeStorage(buffer_manager, count, 1);
	const auto storage_size = storage->AllocationSize();
	// views of storage someone else created borrow it, also when they are its only holders
	auto first = MakeView(storage, count);
	auto second = MakeView(storage, count);
	storage.reset();
	REQUIRE(first.GetAllocationSize() + second.GetAllocationSize() < storage_size);
	// a NULL written to a view allocates a mask the view accounts
	const auto before_null = first.GetAllocationSize();
	first.SetValue(3, Value(LogicalType::BIGINT));
	REQUIRE(first.GetAllocationSize() >= before_null + ValidityMask::ValidityMaskSize(count));

	// an extension that copies into new storage creates it and accounts it once, its slices borrow it
	auto extra_source = MakeSource(3, -1);
	vector<SharedChildExtra> extras {SharedChildExtra {extra_source, list_entry_t {0, 3}}};
	Vector grown(LogicalType::BIGINT);
	idx_t grown_offset;
	REQUIRE(SharedChildVector::TryExtend(buffer_manager, second, list_entry_t {0, count}, extras, grown, grown_offset));
	auto grown_storage = SharedChildVector::GetUnchangedStorage(grown);
	REQUIRE(grown_storage);
	const auto grown_size = grown_storage->AllocationSize();
	REQUIRE(grown.GetAllocationSize() >= grown_size);
	Vector grown_slice(grown, 0, 100);
	REQUIRE(grown_slice.GetAllocationSize() < grown_size);
	// an in-place extension of that storage borrows it, and keeps borrowing once the creator is gone: like a slice
	// of a StandardVectorBuffer, a vector that keeps another holder's allocation alive does not account it
	Vector extended(LogicalType::BIGINT);
	idx_t extended_offset;
	REQUIRE(SharedChildVector::TryExtend(buffer_manager, grown, list_entry_t {0, count + 3}, extras, extended,
	                                     extended_offset));
	REQUIRE(SharedChildVector::GetUnchangedStorage(extended) == grown_storage);
	REQUIRE(extended.GetAllocationSize() < grown_size);
}

TEST_CASE("Concurrent extensions of one shared prefix", "[shared_child_vector]") {
	DuckDB db(nullptr);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const idx_t count = 3000;
	// room behind the prefix, so exactly one extension can claim it
	BufferHandle pin;
	auto storage = SharedChildStorage::Create(buffer_manager, sizeof(int64_t), 2 * count, pin);
	auto prefix_source = MakeSource(count, 1);
	memcpy(pin.GetDataMutable(), FlatVector::GetData(prefix_source), count * sizeof(int64_t));
	REQUIRE(storage->TryClaim(0, count));
	pin = BufferHandle();
	auto prefix = MakeView(storage, count);
	const list_entry_t prefix_entry {0, count};

	const idx_t thread_count = 2;
	vector<unique_ptr<Vector>> results;
	vector<idx_t> offsets(thread_count);
	vector<uint8_t> extended(thread_count, 0);
	vector<Vector> extras;
	for (idx_t t = 0; t < thread_count; t++) {
		results.push_back(make_uniq<Vector>(LogicalType::BIGINT));
		// an allocated mask whose selected entries are all valid
		extras.push_back(MakeSource(4, -10 * int64_t(t + 1)));
		FlatVector::ValidityMutable(extras.back()).SetInvalid(3);
	}
	atomic<idx_t> ready {0};
	atomic<bool> done {false};
	atomic<idx_t> reader_errors {0};
	std::thread reader([&]() {
		// scan the published prefix while the extensions run
		ready++;
		while (!done.load()) {
			auto data = FlatVector::GetData<int64_t>(prefix);
			if (data[0] != 1 || data[count - 1] != int64_t(count) || !FlatVector::Validity(prefix).CannotHaveNull()) {
				reader_errors++;
			}
		}
	});
	vector<std::thread> threads;
	for (idx_t t = 0; t < thread_count; t++) {
		threads.emplace_back([&, t]() {
			vector<SharedChildExtra> extra {SharedChildExtra {extras[t], list_entry_t {0, 3}}};
			// start both extensions together with the reader
			ready++;
			while (ready.load() < thread_count + 1) {
			}
			extended[t] =
			    SharedChildVector::TryExtend(buffer_manager, prefix, prefix_entry, extra, *results[t], offsets[t]);
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	done = true;
	reader.join();
	REQUIRE(reader_errors.load() == 0);
	idx_t in_place = 0;
	for (idx_t t = 0; t < thread_count; t++) {
		REQUIRE(extended[t]);
		auto &result = *results[t];
		const auto offset = offsets[t];
		REQUIRE(result.GetValue(offset) == Value::BIGINT(1));
		REQUIRE(result.GetValue(offset + count - 1) == Value::BIGINT(int64_t(count)));
		for (idx_t i = 0; i < 3; i++) {
			REQUIRE(result.GetValue(offset + count + i) == Value::BIGINT(-10 * int64_t(t + 1) + int64_t(i)));
		}
		if (SharedChildVector::GetUnchangedStorage(result) == storage) {
			in_place++;
		}
	}
	// one extension claimed the region behind the prefix, the other copied the prefix
	REQUIRE(in_place == 1);
	REQUIRE(storage->GetClaimedCount() == count + 3);
	REQUIRE(prefix.GetValue(count - 1) == Value::BIGINT(int64_t(count)));
}
