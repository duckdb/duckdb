#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

duckdb_vector create_src_vector_for_copy_selection_test(duckdb_logical_type type) {
	// Create a source vector of BIGINTs with 6 elements.
	duckdb_vector vector = duckdb_create_vector(type, 6);
	auto data = (int64_t *)duckdb_vector_get_data(vector);
	duckdb_vector_ensure_validity_writable(vector);
	auto src_validity = duckdb_vector_get_validity(vector);

	// Populate with data: {10, 20, NULL, 40, 50, 60}
	data[0] = 10;
	data[1] = 20;
	src_validity[0] = ~0x04;
	data[3] = 40;
	data[4] = 50;
	data[5] = 60;
	return vector;
}

duckdb_selection_vector create_selection_vector_for_copy_selection_test() {
	// Selects rows in the order: 5, 3, 2, 0
	idx_t selection_data[] = {5, 3, 2, 0};
	duckdb_selection_vector sel = duckdb_create_selection_vector(4);
	sel_t *sel_data = duckdb_selection_vector_get_data_ptr(sel);
	for (idx_t i = 0; i < 4; ++i) {
		sel_data[i] = selection_data[i];
	}
	return sel;
}

TEST_CASE("Test duckdb_vector_copy_sel", "[capi]") {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);

	SECTION("Test basic selection copy") {
		duckdb_vector src_vector = create_src_vector_for_copy_selection_test(type);
		duckdb_selection_vector sel_vector = create_selection_vector_for_copy_selection_test();

		duckdb_vector dst_vector = duckdb_create_vector(type, 4);
		auto dst_data = (int64_t *)duckdb_vector_get_data(dst_vector);
		duckdb_vector_ensure_validity_writable(dst_vector);
		auto dst_validity = duckdb_vector_get_validity(dst_vector);

		// Copy 4 elements from the start of the selection vector to the start of the destination.
		duckdb_vector_copy_sel(src_vector, dst_vector, sel_vector, 4, 0, 0);

		// Verify the copied data: should be {60, 40, NULL, 10}
		REQUIRE(dst_data[0] == 60);
		REQUIRE((dst_validity[0] & 0x01) == 0x01);

		REQUIRE(dst_data[1] == 40);
		REQUIRE((dst_validity[0] & 0x02) == 0x02);

		// Check that the NULL was copied correctly
		REQUIRE((~dst_validity[0] & 0x04) == 0x04);

		REQUIRE(dst_data[3] == 10);
		REQUIRE((dst_validity[0] & 0x08) == 0x08);

		duckdb_destroy_vector(&src_vector);
		duckdb_destroy_vector(&dst_vector);
		duckdb_destroy_selection_vector(sel_vector);
	}

	SECTION("Test copy with source and destination offsets") {
		duckdb_vector src_vector = create_src_vector_for_copy_selection_test(type);
		duckdb_selection_vector sel_vector = create_selection_vector_for_copy_selection_test();

		// Create a destination vector pre-filled with some data.
		duckdb_vector dst_vector = duckdb_create_vector(type, 6);
		auto dst_data = (int64_t *)duckdb_vector_get_data(dst_vector);
		duckdb_vector_ensure_validity_writable(dst_vector);
		for (int i = 0; i < 6; i++) {
			dst_data[i] = 999;
		}

		// Copy 2 elements, starting from offset 1 in `sel` (`{3, 2}`).
		// Copy them into `dst_vector` starting at offset 2.
		duckdb_vector_copy_sel(src_vector, dst_vector, sel_vector, 3, 1, 2);

		// Verify destination: should be {999, 999, 40, NULL, 999, 999}
		auto dst_validity = duckdb_vector_get_validity(dst_vector);

		// Unchanged elements
		REQUIRE(dst_data[0] == 999);
		REQUIRE(dst_data[1] == 999);
		REQUIRE(dst_data[4] == 999);
		REQUIRE(dst_data[5] == 999);

		// Copied elements
		REQUIRE(dst_data[2] == 40);
		REQUIRE((dst_validity[0] & 0x04) == 0x04);
		REQUIRE((~dst_validity[0] & 0x08) == 0x08); // The NULL value from src[2]

		duckdb_destroy_vector(&src_vector);
		duckdb_destroy_vector(&dst_vector);
		duckdb_destroy_selection_vector(sel_vector);
	}

	SECTION("Test copy with zero count") {
		duckdb_vector src_vector = create_src_vector_for_copy_selection_test(type);
		duckdb_selection_vector sel_vector = create_selection_vector_for_copy_selection_test();

		duckdb_vector dst_vector = duckdb_create_vector(type, 4);
		auto dst_data = (int64_t *)duckdb_vector_get_data(dst_vector);
		for (int i = 0; i < 4; i++) {
			dst_data[i] = 123; // Pre-fill
		}

		// copy 0 elements.
		duckdb_vector_copy_sel(src_vector, dst_vector, sel_vector, 0, 0, 0);

		for (int i = 0; i < 4; i++) {
			REQUIRE(dst_data[i] == 123);
		}

		duckdb_destroy_vector(&src_vector);
		duckdb_destroy_vector(&dst_vector);
		duckdb_destroy_selection_vector(sel_vector);
	}

	duckdb_destroy_logical_type(&type);
}
