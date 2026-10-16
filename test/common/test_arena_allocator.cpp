#include "catch.hpp"
#include "duckdb/common/arena_containers/arena_vector.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/arena_allocator.hpp"

using namespace duckdb;

namespace {

struct ArenaLifetimeObject {
	ArenaLifetimeObject(ArenaAllocator &arena, vector<idx_t> &destroyed, bool &valid, idx_t id, bool fail = false)
	    : destroyed(destroyed), valid(valid), id(id), values(arena) {
		values.reserve(4096);
		values.push_back(id);
		if (fail) {
			throw InvalidInputException("Construction failed");
		}
	}

	~ArenaLifetimeObject() {
		valid = valid && values[0] == id;
		destroyed.push_back(id);
	}

	vector<idx_t> &destroyed;
	bool &valid;
	idx_t id;
	arena_vector<idx_t> values;
};

} // namespace

TEST_CASE("Arena-owned destructors run before storage is released", "[arena_allocator]") {
	vector<idx_t> destroyed;
	destroyed.reserve(16);
	bool valid = true;
	{
		ArenaAllocator arena(Allocator::DefaultAllocator(), 64);
		arena.MakeWithDestructor<ArenaLifetimeObject>(arena, destroyed, valid, 1);
		arena.MakeWithDestructor<ArenaLifetimeObject>(arena, destroyed, valid, 2);
		REQUIRE(destroyed.empty());
		SECTION("destruction") {
		}
		SECTION("reset retains reusable storage") {
			arena.Reset();
			REQUIRE(destroyed == vector<idx_t> {2, 1});
			auto head = arena.GetHead();
			REQUIRE(head);
			memset(arena.Allocate(head->maximum_size), 42, head->maximum_size);
			REQUIRE(arena.GetHead() == head);
		}
		SECTION("free all") {
			arena.FreeAll();
			REQUIRE(arena.IsEmpty());
		}
		SECTION("destroy") {
			arena.Destroy();
			REQUIRE(arena.IsEmpty());
		}
		SECTION("move transfers destruction") {
			ArenaAllocator moved(Allocator::DefaultAllocator());
			arena.Move(moved);
			arena.Destroy();
			REQUIRE(destroyed.empty());
		}
	}
	REQUIRE(valid);
	REQUIRE(destroyed == vector<idx_t> {2, 1});
}

TEST_CASE("Arena rewind only destroys objects after its saved position", "[arena_allocator]") {
	vector<idx_t> destroyed;
	destroyed.reserve(16);
	bool valid = true;
	ArenaAllocator arena(Allocator::DefaultAllocator(), 64);
	const auto empty = arena.GetPosition();
	auto first = arena.MakeWithDestructor<ArenaLifetimeObject>(arena, destroyed, valid, 1);
	const auto position = arena.GetPosition();
	arena.MakeWithDestructor<ArenaLifetimeObject>(arena, destroyed, valid, 2);
	arena.Rewind(position);
	REQUIRE(destroyed == vector<idx_t> {2});
	REQUIRE(first->values[0] == 1);
	REQUIRE(arena.GetPosition().chunk == position.chunk);
	REQUIRE(arena.GetPosition().current_position == position.current_position);
	arena.MakeWithDestructor<ArenaLifetimeObject>(arena, destroyed, valid, 3);
	arena.Rewind(empty);
	REQUIRE(arena.IsEmpty());
	REQUIRE(valid);
	REQUIRE(destroyed == vector<idx_t> {2, 3, 1});
}

TEST_CASE("Arena-owned destruction survives exceptions", "[arena_allocator]") {
	vector<idx_t> destroyed;
	destroyed.reserve(16);
	bool valid = true;
	try {
		ArenaAllocator arena(Allocator::DefaultAllocator());
		arena.MakeWithDestructor<ArenaLifetimeObject>(arena, destroyed, valid, 1);
		arena.MakeWithDestructor<ArenaLifetimeObject>(arena, destroyed, valid, 2, true);
		FAIL("Construction should have failed");
	} catch (const InvalidInputException &) {
	}
	REQUIRE(valid);
	REQUIRE(destroyed == vector<idx_t> {1});
}
