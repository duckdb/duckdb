#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

struct MyBufferManager {
	idx_t allocated_memory;
	idx_t pinned_buffers;
	idx_t max_memory;
};

struct MyBuffer {
	MyBufferManager *buffer_manager;
	void *allocation;
	bool pinned;
	idx_t size;
};

MyBuffer *CreateBuffer(void *allocation, idx_t size, MyBufferManager *buffer_manager) {
	MyBuffer *buffer = (MyBuffer *)malloc(sizeof(MyBuffer));
	if (!buffer) {
		free(allocation);
		throw IOException("Could not allocate %d bytes", sizeof(MyBuffer));
	}
	buffer->size = size;
	buffer->pinned = false;
	buffer->allocation = allocation;
	buffer->buffer_manager = buffer_manager;
	buffer_manager->allocated_memory += size;
	return buffer;
}

duckdb_buffer Allocate(void *data, idx_t size) {
	auto my_data = (MyBufferManager *)data;
	void *allocation = malloc(size);
	if (!allocation) {
		throw IOException("Could not allocate %d bytes", size);
	}
	return CreateBuffer(allocation, size, my_data);
}

void Destroy(duckdb_buffer buffer) {
	auto my_buffer = (MyBuffer *)buffer;
	auto buffer_manager = my_buffer->buffer_manager;

	// assert that the buffer was not pinned, otherwise it should not be allowed to be destroyed
	D_ASSERT(!my_buffer->pinned);

	free(my_buffer->allocation);
	buffer_manager->allocated_memory -= my_buffer->size;
	free(my_buffer);
}

duckdb_buffer ReAllocate(duckdb_buffer buffer, idx_t old_size, idx_t new_size) {
	auto my_buffer = (MyBuffer *)buffer;
	auto buffer_manager = my_buffer->buffer_manager;

	// assert that the buffer is not pinned, otherwise it should not be allowed to be reallocated
	D_ASSERT(!my_buffer->pinned);

	Destroy(buffer);
	return Allocate(buffer_manager, new_size);
}

void *GetAllocation(duckdb_buffer buffer) {
	auto my_buffer = (MyBuffer *)buffer;

	// assert that the buffer was pinned, you should not be allowed to retrieve an allocation from an unpinned buffer
	D_ASSERT(my_buffer->pinned);
	return my_buffer->allocation;
}

void Pin(duckdb_buffer buffer) {
	auto my_buffer = (MyBuffer *)buffer;
	auto buffer_manager = my_buffer->buffer_manager;

	// assert that the buffer was not pinned before
	D_ASSERT(!my_buffer->pinned);

	buffer_manager->pinned_buffers++;
	my_buffer->pinned = true;
}

void Unpin(duckdb_buffer buffer) {
	auto my_buffer = (MyBuffer *)buffer;
	auto buffer_manager = my_buffer->buffer_manager;

	// assert that the buffer was pinnned
	D_ASSERT(my_buffer->pinned);

	buffer_manager->pinned_buffers++;
	my_buffer->pinned = false;
}

idx_t UsedMemory(void *data) {
	auto my_data = (MyBufferManager *)data;

	return my_data->allocated_memory;
}

idx_t MaxMemory(void *data) {
	auto my_data = (MyBufferManager *)data;

	return my_data->max_memory;
}

TEST_CASE("Test C API CBufferManager", "[capi]") {
	duckdb_database db;
	duckdb_config config;

	// create the configuration object
	if (duckdb_create_config(&config) == DuckDBError) {
		REQUIRE(1 == 0);
	}
	MyBufferManager external_manager;

	// set some configuration options
	if (duckdb_add_custom_buffer_manager(config, &external_manager, Allocate, ReAllocate, Destroy, GetAllocation, Pin,
	                                     Unpin, MaxMemory, UsedMemory) != DuckDBSuccess) {
		REQUIRE(1 == 0);
	}

	// open the database using the configuration
	if (duckdb_open_ext(NULL, &db, config, NULL) == DuckDBError) {
		REQUIRE(1 == 0);
	}
	// cleanup the configuration object
	duckdb_destroy_config(&config);

	duckdb_connection connection;
	if (duckdb_connect(db, &connection) != DuckDBSuccess) {
		REQUIRE(1 == 0);
	}

	duckdb_result result;
	// run queries...
	if (duckdb_query(connection, "create table tbl as select * from range(1000000)", &result) != DuckDBSuccess) {
		REQUIRE(1 == 0);
	}
	if (duckdb_query(connection, "select * from tbl", &result) != DuckDBSuccess) {
		REQUIRE(1 == 0);
	}
	for (idx_t i = 0; i < 1000000; i++) {
		REQUIRE(duckdb_value_int64(&result, 0, i) == i);
	}

	// cleanup
	duckdb_close(&db);
}
