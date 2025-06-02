#include "duckdb/main/client_data.hpp"

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

class ClientFileSystem : public OpenerFileSystem {
public:
	explicit ClientFileSystem(ClientContext &context_p) : context(context_p) {
	}

	FileSystem &GetFileSystem() const override {
		auto &config = DBConfig::GetConfig(context);
		return *config.file_system;
	}

	optional_ptr<FileOpener> GetOpener() const override {
		return ClientData::Get(context).file_opener.get();
	}

private:
	ClientContext &context;
};

//! ClientBufferManager wraps the buffer manager to optionally forward the client context.
class ClientBufferManager : public BufferManager {
public:
	explicit ClientBufferManager(BufferManager &buffer_manager_p) : buffer_manager(buffer_manager_p) {
	}

public:
	shared_ptr<BlockHandle> AllocateTemporaryMemory(MemoryTag tag, idx_t block_size, bool can_destroy = true) override {
		return buffer_manager.AllocateTemporaryMemory(tag, block_size, can_destroy);
	}
	shared_ptr<BlockHandle> AllocateMemory(MemoryTag tag, BlockManager *block_manager,
	                                       bool can_destroy = true) override {
		return buffer_manager.AllocateMemory(tag, block_manager, can_destroy);
	}
	BufferHandle Allocate(MemoryTag tag, idx_t block_size, bool can_destroy = true) override {
		return buffer_manager.Allocate(tag, block_size, can_destroy);
	}
	BufferHandle Allocate(MemoryTag tag, BlockManager *block_manager, bool can_destroy = true) override {
		return buffer_manager.Allocate(tag, block_manager, can_destroy);
	}
	void ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) override {
		return buffer_manager.ReAllocate(handle, block_size);
	}
	BufferHandle Pin(shared_ptr<BlockHandle> &handle) override {
		return buffer_manager.Pin(handle);
	}
	void Prefetch(vector<shared_ptr<BlockHandle>> &handles) override {
		return buffer_manager.Prefetch(handles);
	}
	void Unpin(shared_ptr<BlockHandle> &handle) override {
		return buffer_manager.Unpin(handle);
	}

	idx_t GetUsedMemory() const override {
		return buffer_manager.GetUsedMemory();
	}
	idx_t GetMaxMemory() const override {
		return buffer_manager.GetMaxMemory();
	}
	idx_t GetUsedSwap() const override {
		return buffer_manager.GetUsedSwap();
	}
	optional_idx GetMaxSwap() const override {
		return buffer_manager.GetMaxSwap();
	}
	idx_t GetBlockAllocSize() const override {
		return buffer_manager.GetBlockAllocSize();
	}
	idx_t GetBlockSize() const override {
		return buffer_manager.GetBlockSize();
	}
	idx_t GetTemporaryBlockHeaderSize() const override {
		return buffer_manager.GetTemporaryBlockHeaderSize();
	}
	idx_t GetQueryMaxMemory() const override {
		return buffer_manager.GetQueryMaxMemory();
	}

	shared_ptr<BlockHandle> RegisterTransientMemory(const idx_t size, BlockManager &block_manager) override {
		return buffer_manager.RegisterTransientMemory(size, block_manager);
	}
	shared_ptr<BlockHandle> RegisterSmallMemory(const idx_t size) override {
		return buffer_manager.RegisterSmallMemory(size);
	}
	shared_ptr<BlockHandle> RegisterSmallMemory(MemoryTag tag, const idx_t size) override {
		return buffer_manager.RegisterSmallMemory(tag, size);
	}

	Allocator &GetBufferAllocator() override {
		return buffer_manager.GetBufferAllocator();
	}
	void ReserveMemory(idx_t size) override {
		return buffer_manager.ReserveMemory(size);
	}
	void FreeReservedMemory(idx_t size) override {
		return buffer_manager.FreeReservedMemory(size);
	}
	vector<MemoryInformation> GetMemoryUsageInfo() const override {
		return buffer_manager.GetMemoryUsageInfo();
	}
	void SetMemoryLimit(idx_t limit = (idx_t)-1) override {
		return buffer_manager.SetMemoryLimit(limit);
	}
	void SetSwapLimit(optional_idx limit = optional_idx()) override {
		return buffer_manager.SetSwapLimit(limit);
	}

	vector<TemporaryFileInformation> GetTemporaryFiles() override {
		return buffer_manager.GetTemporaryFiles();
	}
	const string &GetTemporaryDirectory() const override {
		return buffer_manager.GetTemporaryDirectory();
	}
	void SetTemporaryDirectory(const string &new_dir) override {
		return buffer_manager.SetTemporaryDirectory(new_dir);
	}
	bool HasTemporaryDirectory() const override {
		return buffer_manager.HasTemporaryDirectory();
	}

	unique_ptr<FileBuffer> ConstructManagedBuffer(idx_t size, idx_t block_header_size, unique_ptr<FileBuffer> &&source,
	                                              FileBufferType type = FileBufferType::MANAGED_BUFFER) override {
		return buffer_manager.ConstructManagedBuffer(size, block_header_size, std::move(source), type);
	}
	BufferPool &GetBufferPool() const override {
		return buffer_manager.GetBufferPool();
	}
	DatabaseInstance &GetDatabase() override {
		return buffer_manager.GetDatabase();
	}
	TemporaryMemoryManager &GetTemporaryMemoryManager() override {
		return buffer_manager.GetTemporaryMemoryManager();
	}

	void PurgeQueue(const BlockHandle &handle) override {
		return buffer_manager.PurgeQueue(handle);
	}
	void AddToEvictionQueue(shared_ptr<BlockHandle> &handle) override {
		return buffer_manager.AddToEvictionQueue(handle);
	}
	void WriteTemporaryBuffer(MemoryTag tag, block_id_t block_id, FileBuffer &buffer) override {
		return buffer_manager.WriteTemporaryBuffer(tag, block_id, buffer);
	}
	unique_ptr<FileBuffer> ReadTemporaryBuffer(MemoryTag tag, BlockHandle &block,
	                                           unique_ptr<FileBuffer> buffer) override {
		return buffer_manager.ReadTemporaryBuffer(tag, block, std::move(buffer));
	}
	void DeleteTemporaryFile(BlockHandle &block) override {
		return buffer_manager.DeleteTemporaryFile(block);
	}

private:
	BufferManager &buffer_manager;
};

ClientData::ClientData(ClientContext &context) : catalog_search_path(make_uniq<CatalogSearchPath>(context)) {
	auto &db = DatabaseInstance::GetDatabase(context);

	profiler = make_shared_ptr<QueryProfiler>(context);
	temporary_objects = make_shared_ptr<AttachedDatabase>(db, AttachedDatabaseType::TEMP_DATABASE);
	temporary_objects->oid = DatabaseManager::Get(db).NextOid();
	random_engine = make_uniq<RandomEngine>();
	file_opener = make_uniq<ClientContextFileOpener>(context);
	client_file_system = make_uniq<ClientFileSystem>(context);
	client_buffer_manager = make_uniq<ClientBufferManager>(db.GetBufferManager());

	temporary_objects->Initialize();
}

ClientData::~ClientData() {
}

ClientData &ClientData::Get(ClientContext &context) {
	return *context.client_data;
}

const ClientData &ClientData::Get(const ClientContext &context) {
	return *context.client_data;
}

RandomEngine &RandomEngine::Get(ClientContext &context) {
	return *ClientData::Get(context).random_engine;
}

} // namespace duckdb
