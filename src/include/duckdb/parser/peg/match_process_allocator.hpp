//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/match_process_allocator.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

//! Reusable process storage; checkpoints must be restored in stack order after destruction.
class MatchProcessAllocator {
private:
	struct Segment {
		Segment(data_ptr_t data_p, idx_t capacity_p) : data(data_p), capacity(capacity_p) {
		}

		data_ptr_t data;
		idx_t capacity;
		optional_ptr<Segment> next;
	};

public:
	struct Position {
		optional_ptr<Segment> segment;
		idx_t offset = 0;
	};

public:
	explicit MatchProcessAllocator(ArenaAllocator &arena_p) : arena(arena_p) {
	}

	Position GetPosition() const {
		return position;
	}

	void Rewind(Position position_p) {
		position = position_p;
	}

	template <class T, class... ARGS>
	arena_ptr<MatchProcess> Make(ARGS &&...args) {
		static_assert(std::is_base_of<MatchProcess, T>::value, "Expected a matcher process");
		auto storage = Allocate(sizeof(T), alignof(T));
		return arena_ptr<MatchProcess>(new (storage) T(std::forward<ARGS>(args)...));
	}

private:
	static constexpr idx_t SEGMENT_CAPACITY = 8192;

	Segment &AllocateSegment(idx_t size);
	DUCKDB_API data_ptr_t Allocate(idx_t size, idx_t alignment);

private:
	ArenaAllocator &arena;
	optional_ptr<Segment> first_segment;
	Position position;
};

} // namespace duckdb
