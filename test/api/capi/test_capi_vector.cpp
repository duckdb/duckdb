#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

string get_string_from_duckdb_string_t(duckdb_string_t *input) {
	const char *ptr = duckdb_string_is_inlined(*input) ? input->value.inlined.inlined : input->value.pointer.ptr;
	return string(ptr, duckdb_string_t_length(*input));
}

duckdb_vector create_src_vector_for_copy_selection_test(duckdb_logical_type type) {
	// Create a source vector of BIGINTs with 6 elements.
	auto vector = duckdb_create_vector(type, 6);
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
	auto sel = duckdb_create_selection_vector(4);
	sel_t *sel_data = duckdb_selection_vector_get_data_ptr(sel);
	for (idx_t i = 0; i < 4; ++i) {
		sel_data[i] = selection_data[i];
	}
	return sel;
}

TEST_CASE("Test duckdb_vector_copy_sel", "[capi]") {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);

	SECTION("Test basic selection copy") {
		auto src_vector = create_src_vector_for_copy_selection_test(type);
		auto sel_vector = create_selection_vector_for_copy_selection_test();

		auto dst_vector = duckdb_create_vector(type, 4);
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
		auto src_vector = create_src_vector_for_copy_selection_test(type);
		auto sel_vector = create_selection_vector_for_copy_selection_test();

		// Create a destination vector pre-filled with some data.
		auto dst_vector = duckdb_create_vector(type, 6);
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
		auto src_vector = create_src_vector_for_copy_selection_test(type);
		auto sel_vector = create_selection_vector_for_copy_selection_test();

		auto dst_vector = duckdb_create_vector(type, 4);
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

void copy_data_chunk_using_vector_copy_sel(duckdb_data_chunk src, duckdb_data_chunk dst) {
	idx_t src_size = duckdb_data_chunk_get_size(src);

	auto incr_sel_vector = duckdb_create_selection_vector(src_size);
	sel_t *data_ptr = duckdb_selection_vector_get_data_ptr(incr_sel_vector);
	for (sel_t i = 0; i < sel_t(src_size); i++) {
		data_ptr[i] = i;
	}

	for (idx_t i = 0; i < duckdb_data_chunk_get_column_count(src); i++) {
		duckdb_vector src_vector = duckdb_data_chunk_get_vector(src, i);
		duckdb_vector dst_vector = duckdb_data_chunk_get_vector(dst, i);
		duckdb_vector_copy_sel(src_vector, dst_vector, incr_sel_vector, src_size, 0, 0);
	}
	duckdb_data_chunk_set_size(dst, src_size);
	duckdb_destroy_selection_vector(incr_sel_vector);
}

TEST_CASE("Test copying data_chunk by using duckdb_vector_copy_sel", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	SECTION("Test basic data chunk copy") {
		duckdb_logical_type types[] = {duckdb_create_logical_type(DUCKDB_TYPE_INTEGER),
		                               duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR)};

		auto src_chunk = duckdb_create_data_chunk(types, 2);
		auto dst_chunk = duckdb_create_data_chunk(types, 2);

		int32_t *int_data =
		    reinterpret_cast<int32_t *>(duckdb_vector_get_data(duckdb_data_chunk_get_vector(src_chunk, 0)));
		int_data[0] = 42;
		int_data[1] = 99;

		auto varchar_vector = duckdb_data_chunk_get_vector(src_chunk, 1);
		duckdb_vector_assign_string_element(varchar_vector, 0, "hello");
		duckdb_vector_assign_string_element(varchar_vector, 1, "world");

		duckdb_data_chunk_set_size(src_chunk, 2);

		copy_data_chunk_using_vector_copy_sel(src_chunk, dst_chunk);

		REQUIRE(duckdb_data_chunk_get_size(dst_chunk) == 2);
		REQUIRE(duckdb_data_chunk_get_column_count(dst_chunk) == 2);

		int32_t *dst_int_data = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(dst_chunk, 0));
		CHECK(dst_int_data[0] == 42);
		CHECK(dst_int_data[1] == 99);

		auto dst_vector = duckdb_data_chunk_get_vector(dst_chunk, 1);
		auto validity = duckdb_vector_get_validity(dst_vector);
		auto string_data = (duckdb_string_t *)duckdb_vector_get_data(dst_vector);

		CHECK(duckdb_validity_row_is_valid(validity, 0));
		CHECK(duckdb_validity_row_is_valid(validity, 1));

		CHECK(get_string_from_duckdb_string_t(&string_data[0]).compare("hello") == 0);
		CHECK(get_string_from_duckdb_string_t(&string_data[1]).compare("world") == 0);

		duckdb_destroy_data_chunk(&src_chunk);
		duckdb_destroy_data_chunk(&dst_chunk);
		for (size_t i = 0; i < 2; i++) {
			duckdb_destroy_logical_type(&types[i]);
		}
	}
}

void reference_data_chunk_using_vector_reference_vector(duckdb_data_chunk src, duckdb_data_chunk dst,
                                                        const idx_t *ref_indices, idx_t ref_len) {
	duckdb_data_chunk_reset(dst);
	idx_t src_size = duckdb_data_chunk_get_size(src);
	for (idx_t i = 0; i < ref_len; i++) {
		idx_t idx = ref_indices[i];
		auto src_vector = duckdb_data_chunk_get_vector(src, idx);
		auto dst_vector = duckdb_data_chunk_get_vector(dst, i);
		duckdb_vector_reference_vector(dst_vector, src_vector);
	}
	duckdb_data_chunk_set_size(dst, src_size);
}

TEST_CASE("Test referencing data chunks by using duckdb_vector_reference_vector", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	duckdb_logical_type src_types[] = {duckdb_create_logical_type(DUCKDB_TYPE_INTEGER),
	                                   duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE),
	                                   duckdb_create_logical_type(DUCKDB_TYPE_BIGINT)};
	auto src_chunk = duckdb_create_data_chunk(src_types, 3);

	auto src_int_vector = duckdb_data_chunk_get_vector(src_chunk, 0);
	auto src_double_vector = duckdb_data_chunk_get_vector(src_chunk, 1);
	auto src_bigint_vector = duckdb_data_chunk_get_vector(src_chunk, 2);

	auto src_int_data = (int32_t *)duckdb_vector_get_data(src_int_vector);
	auto src_double_data = (double *)duckdb_vector_get_data(src_double_vector);
	auto src_bigint_data = (int64_t *)duckdb_vector_get_data(src_bigint_vector);

	src_int_data[0] = 42;
	src_int_data[1] = 99;
	src_double_data[0] = 0.5;
	src_double_data[1] = 1.5;
	src_bigint_data[0] = 1000;
	src_bigint_data[1] = 2000;
	duckdb_data_chunk_set_size(src_chunk, 2);

	duckdb_logical_type dst_types[] = {duckdb_create_logical_type(DUCKDB_TYPE_BIGINT),
	                                   duckdb_create_logical_type(DUCKDB_TYPE_INTEGER)};
	auto dst_chunk = duckdb_create_data_chunk(dst_types, 2);

	idx_t ref_indices[] = {2, 0};

	reference_data_chunk_using_vector_reference_vector(src_chunk, dst_chunk, ref_indices, 2);

	REQUIRE(duckdb_data_chunk_get_column_count(dst_chunk) == 2);
	REQUIRE(duckdb_data_chunk_get_size(dst_chunk) == 2);

	auto dst_type_0 = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(dst_chunk, 0));
	auto dst_type_1 = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(dst_chunk, 1));
	REQUIRE(duckdb_get_type_id(dst_type_0) == DUCKDB_TYPE_BIGINT);
	REQUIRE(duckdb_get_type_id(dst_type_1) == DUCKDB_TYPE_INTEGER);
	duckdb_destroy_logical_type(&dst_type_0);
	duckdb_destroy_logical_type(&dst_type_1);

	// Verify that the data pointers are the same
	auto dst_bigint_vector = duckdb_data_chunk_get_vector(dst_chunk, 0);
	auto dst_int_vector = duckdb_data_chunk_get_vector(dst_chunk, 1);
	REQUIRE(duckdb_vector_get_data(dst_bigint_vector) == duckdb_vector_get_data(src_bigint_vector));
	REQUIRE(duckdb_vector_get_data(dst_int_vector) == duckdb_vector_get_data(src_int_vector));

	src_bigint_data[0] = 9999;
	auto dst_bigint_data = (int64_t *)duckdb_vector_get_data(dst_bigint_vector);
	REQUIRE(dst_bigint_data[0] == 9999);

	duckdb_destroy_data_chunk(&dst_chunk);
	for (size_t i = 0; i < 2; i++) {
		duckdb_destroy_logical_type(&dst_types[i]);
	}

	duckdb_destroy_data_chunk(&src_chunk);
	for (size_t i = 0; i < 3; i++) {
		duckdb_destroy_logical_type(&src_types[i]);
	}
}
