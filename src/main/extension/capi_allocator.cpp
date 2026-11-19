#include "duckdb/main/extension/capi_allocator.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

namespace {

struct ExtensionCounters {
	mutex lock;
	unordered_map<string, reference_set_t<atomic<int64_t>>> by_name;

	static shared_ptr<ExtensionCounters> Get() {
		static auto registry = make_shared_ptr<ExtensionCounters>();
		return registry;
	}
};

struct ScopedAllocatorData : PrivateAllocatorData {
	virtual void Register(const string &name) {
		throw InvalidInputException("Cannot register a database-scoped allocator with an extension");
	}
};

struct ExtensionAllocatorData : ScopedAllocatorData {
	atomic<int64_t> bytes {0};
	shared_ptr<ExtensionCounters> registry;
	string extension_name;

	void Register(const string &name) override {
		auto counters = ExtensionCounters::Get();
		lock_guard<mutex> guard(counters->lock);
		if (registry) {
			if (extension_name != name) {
				throw InvalidInputException("Allocator is already registered to extension '%s'", extension_name);
			}
			return;
		}
		extension_name = name;
		counters->by_name[name].insert(bytes);
		registry = std::move(counters);
	}

	~ExtensionAllocatorData() override {
		if (registry) {
			lock_guard<mutex> guard(registry->lock);
			registry->by_name.at(extension_name).erase(bytes);
		}
	}

	static data_ptr_t Allocate(PrivateAllocatorData *data, idx_t size) {
		auto ptr = Allocator::DefaultAllocate(data, size);
		if (ptr) {
			data->Cast<ExtensionAllocatorData>().bytes.fetch_add(size, std::memory_order_relaxed);
		}
		return ptr;
	}

	static void Free(PrivateAllocatorData *data, data_ptr_t ptr, idx_t size) {
		Allocator::DefaultFree(data, ptr, size);
		data->Cast<ExtensionAllocatorData>().bytes.fetch_sub(size, std::memory_order_relaxed);
	}

	static data_ptr_t Reallocate(PrivateAllocatorData *data, data_ptr_t ptr, idx_t old_size, idx_t size) {
		auto result = Allocator::DefaultReallocate(data, ptr, old_size, size);
		if (result) {
			data->Cast<ExtensionAllocatorData>().bytes.fetch_add(int64_t(size) - int64_t(old_size),
			                                                     std::memory_order_relaxed);
		}
		return result;
	}
};

struct DatabaseAllocatorData : ScopedAllocatorData {
	explicit DatabaseAllocatorData(DatabaseInstance &instance)
	    : db(instance.shared_from_this()), allocator(Allocator::Get(instance)),
	      manager(BufferManager::GetBufferManager(instance)) {
	}

	shared_ptr<DatabaseInstance> db;
	Allocator &allocator;
	BufferManager &manager;

	static data_ptr_t Allocate(PrivateAllocatorData *data, idx_t size) {
		return Reallocate(data, nullptr, 0, size);
	}

	static void Free(PrivateAllocatorData *data, data_ptr_t ptr, idx_t size) {
		auto &state = data->Cast<DatabaseAllocatorData>();
		state.allocator.FreeData(ptr, size);
		state.manager.FreeReservedMemory(size);
	}

	static data_ptr_t Reallocate(PrivateAllocatorData *data, data_ptr_t ptr, idx_t old_size, idx_t size) {
		auto &state = data->Cast<DatabaseAllocatorData>();
		if (size > old_size) {
			state.manager.ReserveMemory(size - old_size);
		}
		data_ptr_t result;
		try {
			result = ptr ? state.allocator.ReallocateData(ptr, old_size, size) : state.allocator.AllocateData(size);
		} catch (...) {
			if (size > old_size) {
				state.manager.FreeReservedMemory(size - old_size);
			}
			throw;
		}
		if (size < old_size) {
			state.manager.FreeReservedMemory(old_size - size);
		}
		return result;
	}
};

} // namespace

unique_ptr<Allocator> ExtensionAllocator::Create() {
	return make_uniq<Allocator>(ExtensionAllocatorData::Allocate, ExtensionAllocatorData::Free,
	                            ExtensionAllocatorData::Reallocate, make_uniq<ExtensionAllocatorData>());
}

unique_ptr<Allocator> ExtensionAllocator::Create(DatabaseInstance &db) {
	return make_uniq<Allocator>(DatabaseAllocatorData::Allocate, DatabaseAllocatorData::Free,
	                            DatabaseAllocatorData::Reallocate, make_uniq<DatabaseAllocatorData>(db));
}

void ExtensionAllocator::Register(Allocator &allocator, const string &extension_name) {
	allocator.GetPrivateData()->Cast<ScopedAllocatorData>().Register(extension_name);
}

optional_idx ExtensionAllocator::GetUsage(const string &extension_name) {
	auto registry = ExtensionCounters::Get();
	lock_guard<mutex> guard(registry->lock);
	auto it = registry->by_name.find(extension_name);
	if (it == registry->by_name.end()) {
		return optional_idx();
	}
	idx_t total = 0;
	for (auto &bytes : it->second) {
		total += NumericCast<idx_t>(bytes.get().load(std::memory_order_relaxed));
	}
	return total;
}

} // namespace duckdb
