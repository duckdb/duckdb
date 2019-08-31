#include "storage/buffer/buffer_handle.hpp"
#include "storage/buffer_manager.hpp"

using namespace duckdb;
using namespace std;

BufferHandle::BufferHandle(BufferManager &manager,block_id_t block_id) :
		manager(manager), block_id(block_id) {

}

BufferHandle::~BufferHandle() {
	manager.Unpin(block_id);
}
