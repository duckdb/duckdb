#include "catch.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/operator/join/outer_join_marker.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Join match selection preserves payloads and marker reuse", "[join][selection]") {
	for (idx_t count : {idx_t(0), idx_t(1), idx_t(7), idx_t(8), idx_t(9), idx_t(63), idx_t(64), idx_t(65), idx_t(193),
	                    idx_t(STANDARD_VECTOR_SIZE)}) {
		if (count > STANDARD_VECTOR_SIZE) {
			continue;
		}
		for (bool dictionary : {false, true}) {
			DataChunk left;
			left.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
			{
				auto writer = FlatVector::Writer<int64_t>(left.data[0], count);
				for (idx_t i = 0; i < count; i++) {
					if (i % 7 == 0) {
						writer.WriteNull();
					} else {
						writer.WriteValue(NumericCast<int64_t>(i));
					}
				}
			}
			if (dictionary && count > 0) {
				SelectionVector sel(count);
				for (idx_t i = 0; i < count; i++) {
					sel.set_index(i, (i * 13) % ((count + 1) / 2));
				}
				left.Slice(sel, count);
			}
			auto input = left.data[0].Values<int64_t>();
			auto matches = make_unsafe_uniq_array<bool>(STANDARD_VECTOR_SIZE);
			OuterJoinMarker marker(true);
			marker.Initialize(STANDARD_VECTOR_SIZE);
			DataChunk result, outer_result;
			result.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
			outer_result.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT, LogicalType::BIGINT});

			for (idx_t pattern : {0, 1, 2, 1, 0}) {
				marker.Reset();
				for (idx_t i = 0; i < count; i++) {
					matches[i] = pattern == 2 || (pattern == 1 && i % 3 == 0);
					if (matches[i]) {
						marker.SetMatch(i);
					}
				}
				for (bool match : {false, true}) {
					result.Reset();
					if (match) {
						PhysicalJoin::ConstructSemiJoinResult(left, result, matches.get());
					} else {
						PhysicalJoin::ConstructAntiJoinResult(left, result, matches.get());
					}
					auto output = result.data[0].Values<int64_t>();
					idx_t selected = 0;
					for (idx_t i = 0; i < count; i++) {
						if (matches[i] != match) {
							continue;
						}
						REQUIRE(selected < result.size());
						REQUIRE(output[selected].IsValid() == input[i].IsValid());
						if (input[i].IsValid()) {
							REQUIRE(output[selected].GetValue() == input[i].GetValue());
						}
						selected++;
					}
					REQUIRE(result.size() == selected);
					if (!match) {
						outer_result.Reset();
						marker.ConstructLeftJoinResult(left, outer_result);
						REQUIRE(outer_result.size() == selected);
						auto outer = outer_result.data[0].Values<int64_t>();
						auto nulls = outer_result.data[1].Validity();
						for (idx_t i = 0; i < selected; i++) {
							REQUIRE(outer[i].IsValid() == output[i].IsValid());
							if (output[i].IsValid()) {
								REQUIRE(outer[i].GetValue() == output[i].GetValue());
							}
							REQUIRE(!nulls.IsValid(i));
						}
					}
				}
			}
		}
	}
}
