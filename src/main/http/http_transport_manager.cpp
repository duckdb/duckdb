#include "duckdb/common/http_transport_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

#if !defined(_WIN32) && !defined(__EMSCRIPTEN__)
#include <sys/resource.h>
#endif

namespace duckdb {

static constexpr idx_t HTTP_TRANSPORT_MAX_CAPACITY = 256;

class HTTPTransportManagerState {
public:
	HTTPTransportManagerState(shared_ptr<HTTPUtil> http_util_p, HTTPTransportReusePolicy reuse_policy_p,
	                          idx_t provider_epoch_p)
	    : http_util(std::move(http_util_p)), reuse_policy(reuse_policy_p), provider_epoch(provider_epoch_p) {
	}

	//! Provider retained for this publication.
	const shared_ptr<HTTPUtil> http_util;
	//! Client reuse contract reported by the provider.
	const HTTPTransportReusePolicy reuse_policy;
	//! Stable manager-issued provider identity.
	const idx_t provider_epoch;
	//! Current connection generation for explicit invalidation.
	uint64_t connection_epoch = 0;
	//! Whether epoch exhaustion permanently disabled reuse.
	bool reuse_poisoned = false;
};

class HTTPTransportCapacityGuard;

class HTTPTransportCapacityGuard {
public:
	explicit HTTPTransportCapacityGuard(HTTPTransportManager &manager_p)
	    : manager(manager_p), previous(CurrentThreadGuard()) {
		CurrentThreadGuard() = this;
	}

	~HTTPTransportCapacityGuard() {
		D_ASSERT(CurrentThreadGuard().get() == this);
		CurrentThreadGuard() = previous;
	}

	void MarkCapacityOwned() {
		D_ASSERT(CurrentThreadGuard().get() == this);
		owns_capacity = true;
	}

	static bool CurrentThreadOwnsCapacity(HTTPTransportManager &manager) {
		for (auto entry = CurrentThreadGuard(); entry; entry = entry->previous) {
			if (entry->manager.get() == &manager && entry->owns_capacity) {
				return true;
			}
		}
		return false;
	}

	static void MarkCurrentCapacityOwned(HTTPTransportManager &manager) {
		D_ASSERT(CurrentThreadGuard());
		D_ASSERT(CurrentThreadGuard()->manager.get() == &manager);
		CurrentThreadGuard()->MarkCapacityOwned();
	}

private:
	static optional_ptr<HTTPTransportCapacityGuard> &CurrentThreadGuard() {
		static thread_local optional_ptr<HTTPTransportCapacityGuard> current_guard;
		return current_guard;
	}

	//! Manager whose capacity can be held by this call stack.
	optional_ptr<HTTPTransportManager> manager;
	//! Previous guard on the current thread.
	optional_ptr<HTTPTransportCapacityGuard> previous;
	//! Whether this scope currently owns counted capacity.
	bool owns_capacity = false;
};

static void ValidateReusePolicy(HTTPTransportReusePolicy policy) {
	switch (policy) {
	case HTTPTransportReusePolicy::CLIENT_FREE:
	case HTTPTransportReusePolicy::EPHEMERAL:
	case HTTPTransportReusePolicy::SESSION_LOCAL:
	case HTTPTransportReusePolicy::SHARED:
		return;
	default:
		throw InvalidInputException("HTTP provider returned an invalid transport reuse policy");
	}
}

HTTPTransportManager::Session::Session(HTTPTransportManager &manager_p, HTTPTransportManagerState &state_p,
                                       idx_t session_id_p, unique_ptr<HTTPParams> params_p)
    : manager(manager_p), state(state_p), session_id(session_id_p), params(std::move(params_p)) {
}

HTTPTransportManager::Session::Session(Session &&other) noexcept
    : manager(other.manager), state(other.state), session_id(other.session_id), params(std::move(other.params)) {
	other.manager = nullptr;
	other.state = nullptr;
	other.session_id = DConstants::INVALID_INDEX;
}

HTTPTransportManager::Session &HTTPTransportManager::Session::operator=(Session &&other) noexcept {
	if (this == &other) {
		return *this;
	}
	Reset();
	manager = other.manager;
	state = other.state;
	session_id = other.session_id;
	params = std::move(other.params);
	other.manager = nullptr;
	other.state = nullptr;
	other.session_id = DConstants::INVALID_INDEX;
	return *this;
}

HTTPTransportManager::Session::~Session() {
	Reset();
}

void HTTPTransportManager::Session::Reset() noexcept {
	if (manager) {
		manager->DestroySession(session_id);
	}
	manager = nullptr;
	state = nullptr;
	session_id = DConstants::INVALID_INDEX;
	params.reset();
}

HTTPParams &HTTPTransportManager::Session::Parameters() {
	if (!manager || !params) {
		throw InvalidInputException("Cannot access parameters from an invalid HTTP transport session");
	}
	return *params;
}

unique_ptr<HTTPResponse> HTTPTransportManager::Session::Request(BaseRequest &request) {
	if (!manager) {
		throw InvalidInputException("Cannot perform a request with an invalid HTTP transport session");
	}
	return manager->PerformRequest(*this, request);
}

void HTTPTransportManager::Session::Invalidate() noexcept {
	if (manager) {
		manager->Invalidate(state);
	}
}

bool HTTPTransportManager::ClientKey::operator==(const ClientKey &other) const {
	return provider_epoch == other.provider_epoch && connection_epoch == other.connection_epoch &&
	       session_id == other.session_id && origin_hash == other.origin_hash;
}

hash_t HTTPTransportManager::ClientKeyHash::operator()(const ClientKey &key) const {
	auto result = std::hash<idx_t> {}(key.provider_epoch);
	auto combine = [&](hash_t value) {
		result ^= value + 0x9e3779b9U + (result << 6U) + (result >> 2U);
	};
	combine(std::hash<uint64_t> {}(key.connection_epoch));
	combine(std::hash<idx_t> {}(key.session_id));
	combine(key.origin_hash);
	return result;
}

HTTPTransportManager::Lease::Lease(HTTPTransportManager &manager_p, HTTPTransportManagerState &state_p,
                                   optional_ptr<ClientBucket> bucket_p, unique_ptr<HTTPClient> client_p)
    : manager(manager_p), state(state_p), bucket(bucket_p), client(std::move(client_p)) {
}

HTTPTransportManager::Lease::Lease(Lease &&other) noexcept
    : manager(other.manager), state(other.state), bucket(other.bucket), client(std::move(other.client)),
      reusable(other.reusable) {
	other.manager = nullptr;
	other.state = nullptr;
	other.bucket = nullptr;
	other.reusable = false;
}

HTTPTransportManager::Lease::~Lease() {
	Reset();
}

void HTTPTransportManager::Lease::Reset() noexcept {
	if (manager) {
		manager->Release(*this);
	}
	manager = nullptr;
	state = nullptr;
	bucket = nullptr;
	client.reset();
	reusable = false;
}

unique_ptr<HTTPClient> &HTTPTransportManager::Lease::Client() {
	if (!manager) {
		throw InvalidInputException("Cannot use an invalid HTTP transport lease");
	}
	return client;
}

void HTTPTransportManager::Lease::InvalidateClient() noexcept {
	reusable = false;
}

unique_ptr<HTTPTransportManager> HTTPTransportManager::Create(const shared_ptr<HTTPUtil> &initial_http_util) {
	return unique_ptr<HTTPTransportManager>(new HTTPTransportManager(initial_http_util));
}

HTTPTransportManager::HTTPTransportManager(const shared_ptr<HTTPUtil> &initial_http_util) {
	if (!initial_http_util) {
		throw InvalidInputException("HTTP provider cannot be null");
	}
	auto reuse_policy = initial_http_util->GetTransportReusePolicy();
	ValidateReusePolicy(reuse_policy);
	providers.push_back(make_uniq<HTTPTransportManagerState>(initial_http_util, reuse_policy, 0));
}

HTTPTransportManager::~HTTPTransportManager() {
	Close();
	annotated_lock_guard<annotated_mutex> guard(lock);
	D_ASSERT(reserved_clients == 0);
	D_ASSERT(client_buckets.empty());
	D_ASSERT(non_empty_buckets.empty());
}

idx_t HTTPTransportManager::CalculateCapacity(idx_t system_concurrency, optional_idx file_descriptor_limit) {
	idx_t cpu_target;
	if (system_concurrency >= 128) {
		cpu_target = HTTP_TRANSPORT_MAX_CAPACITY;
	} else {
		cpu_target = system_concurrency * 2;
		cpu_target = MaxValue<idx_t>(cpu_target, 16);
	}
	if (!file_descriptor_limit.IsValid()) {
		return cpu_target;
	}
	auto fd_target = MaxValue<idx_t>(file_descriptor_limit.GetIndex() / 8, 1);
	return MinValue<idx_t>(cpu_target, fd_target);
}

optional_idx HTTPTransportManager::GetFileDescriptorLimit() {
#if !defined(_WIN32) && !defined(__EMSCRIPTEN__)
	struct rlimit limit;
	if (getrlimit(RLIMIT_NOFILE, &limit) != 0 || limit.rlim_cur == RLIM_INFINITY) {
		return optional_idx();
	}
	if (limit.rlim_cur >= NumericLimits<idx_t>::Maximum()) {
		return optional_idx(NumericLimits<idx_t>::Maximum());
	}
	return optional_idx(static_cast<idx_t>(limit.rlim_cur));
#else
	return optional_idx();
#endif
}

bool HTTPTransportManager::AdvanceConnectionEpoch(uint64_t &connection_epoch, bool &reuse_poisoned) noexcept {
	if (reuse_poisoned) {
		return true;
	}
	if (connection_epoch == NumericLimits<uint64_t>::Maximum()) {
		reuse_poisoned = true;
		return true;
	}
	connection_epoch++;
	return false;
}

void HTTPTransportManager::Initialize(idx_t system_concurrency) {
	auto new_capacity = CalculateCapacity(system_concurrency, GetFileDescriptorLimit());
	ClientBucketMap new_client_buckets;
	new_client_buckets.reserve(new_capacity);
	vector<optional_ptr<ClientBucket>> new_non_empty_buckets;
	new_non_empty_buckets.reserve(new_capacity);

	annotated_lock_guard<annotated_mutex> guard(lock);
	if (initialized) {
		throw InternalException("HTTP transport manager was initialized more than once");
	}
	capacity = new_capacity;
	client_buckets = std::move(new_client_buckets);
	non_empty_buckets = std::move(new_non_empty_buckets);
	initialized = true;
}

HTTPTransportManager::SessionReservation HTTPTransportManager::ReserveSession() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (!initialized) {
		throw InternalException("HTTP transport manager is not initialized");
	}
	if (closed) {
		throw InvalidInputException("HTTP transport manager is closed");
	}
	if (next_session_id == DConstants::INVALID_INDEX) {
		throw InternalException("HTTP transport session identifiers exhausted");
	}
	SessionReservation result;
	result.session_id = next_session_id++;
	result.state = providers[current_provider_epoch].get();
	return result;
}

HTTPTransportManager::Session HTTPTransportManager::FinishSession(SessionReservation reservation,
                                                                  unique_ptr<HTTPParams> params) {
	if (!params) {
		throw InvalidConfigurationException("HTTP provider returned null parameters");
	}
	if (&params->http_util != reservation.state->http_util.get()) {
		throw InvalidConfigurationException("HTTP provider returned parameters owned by a different provider");
	}
	params->transport_manager = this;
	params->transport_state = reservation.state;
	params->transport_session_id = reservation.session_id;
	return Session(*this, *reservation.state, reservation.session_id, std::move(params));
}

void HTTPTransportManager::ValidateDatabase(DatabaseInstance &db) const {
	if (&db.config.GetHTTPTransportManager() != this) {
		throw InvalidInputException("HTTP transport session belongs to a different database");
	}
}

HTTPTransportManager::Session HTTPTransportManager::CreateSession(DatabaseInstance &db, const string &path) {
	ValidateDatabase(db);
	auto reservation = ReserveSession();
	return FinishSession(reservation, reservation.state->http_util->InitializeParameters(db, path));
}

HTTPTransportManager::Session HTTPTransportManager::CreateSession(ClientContext &context, const string &path) {
	ValidateDatabase(DatabaseInstance::GetDatabase(context));
	auto reservation = ReserveSession();
	return FinishSession(reservation, reservation.state->http_util->InitializeParameters(context, path));
}

HTTPTransportManager::Session HTTPTransportManager::CreateSession(optional_ptr<FileOpener> opener,
                                                                  optional_ptr<FileOpenerInfo> info) {
	auto opener_db = FileOpener::TryGetDatabase(opener);
	if (opener_db) {
		ValidateDatabase(*opener_db);
	}
	auto reservation = ReserveSession();
	return FinishSession(reservation, reservation.state->http_util->InitializeParameters(opener, info));
}

void HTTPTransportManager::SetHTTPUtil(const shared_ptr<HTTPUtil> &new_http_util) {
	if (!new_http_util) {
		throw InvalidInputException("HTTP provider cannot be null");
	}
	auto reuse_policy = new_http_util->GetTransportReusePolicy();
	ValidateReusePolicy(reuse_policy);

	idx_t old_provider_epoch;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (closed) {
			throw InvalidInputException("HTTP transport manager is closed");
		}
		if (providers.size() == DConstants::INVALID_INDEX) {
			throw InternalException("HTTP provider identifiers exhausted");
		}
		old_provider_epoch = current_provider_epoch;
		auto provider_epoch = providers.size();
		providers.push_back(make_uniq<HTTPTransportManagerState>(new_http_util, reuse_policy, provider_epoch));
		current_provider_epoch = provider_epoch;
	}
	DisposeIdle(IdleFilter::PROVIDER, old_provider_epoch);
}

HTTPUtil &HTTPTransportManager::GetHTTPUtil() const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return *providers[current_provider_epoch]->http_util;
}

bool HTTPTransportManager::IsStateValidLocked(const HTTPTransportManagerState &state) const {
	return state.provider_epoch < providers.size() && providers[state.provider_epoch].get() == &state;
}

void HTTPTransportManager::ValidateStateLocked(const HTTPTransportManagerState &state) const {
	if (closed) {
		throw InvalidInputException("HTTP transport manager is closed");
	}
	if (!IsStateValidLocked(state)) {
		throw InvalidInputException("HTTP parameters contain an invalid provider snapshot");
	}
}

HTTPTransportManager::ClientBucketMap::iterator HTTPTransportManager::FindBucketLocked(const ClientKey &key,
                                                                                       const string &origin) {
	auto entry = client_buckets.find(key);
	if (entry == client_buckets.end() || entry->second.origin == origin) {
		return entry;
	}
	auto range = client_buckets.equal_range(key);
	for (entry = range.first; entry != range.second; ++entry) {
		if (entry->second.origin == origin) {
			return entry;
		}
	}
	return client_buckets.end();
}

void HTTPTransportManager::AddNonEmptyBucketLocked(ClientBucket &bucket) {
	D_ASSERT(!bucket.idle_clients.empty());
	D_ASSERT(!bucket.non_empty_index.IsValid());
	D_ASSERT(non_empty_buckets.size() < capacity);
	bucket.non_empty_index = non_empty_buckets.size();
	non_empty_buckets.push_back(bucket);
}

void HTTPTransportManager::RemoveNonEmptyBucketLocked(ClientBucket &bucket) {
	D_ASSERT(bucket.non_empty_index.IsValid());
	auto index = bucket.non_empty_index.GetIndex();
	D_ASSERT(index < non_empty_buckets.size());
	auto last_bucket = non_empty_buckets.back();
	non_empty_buckets[index] = last_bucket;
	last_bucket->non_empty_index = index;
	non_empty_buckets.pop_back();
	bucket.non_empty_index.SetInvalid();
}

unique_ptr<HTTPClient> HTTPTransportManager::TakeIdleClientLocked(ClientBucket &bucket) {
	D_ASSERT(!bucket.idle_clients.empty());
	auto result = std::move(bucket.idle_clients.back());
	bucket.idle_clients.pop_back();
	if (bucket.idle_clients.empty()) {
		RemoveNonEmptyBucketLocked(bucket);
	}
	return result;
}

HTTPTransportManager::ClientBucketMap::node_type HTTPTransportManager::ExtractBucketLocked(ClientBucket &bucket) {
	D_ASSERT(bucket.reserved_clients == 0);
	D_ASSERT(bucket.idle_clients.empty());
	D_ASSERT(!bucket.non_empty_index.IsValid());
	auto entry = FindBucketLocked(bucket.key, bucket.origin);
	D_ASSERT(entry != client_buckets.end());
	D_ASSERT(&entry->second == &bucket);
	return client_buckets.extract(entry);
}

void HTTPTransportManager::WaitForAdmissionLocked(annotated_unique_lock<annotated_mutex> &guard) {
	if (HTTPTransportCapacityGuard::CurrentThreadOwnsCapacity(*this)) {
		throw InvalidInputException("Nested HTTP client acquisition would wait for manager capacity");
	}
	waiting_threads++;
	try {
		availability.wait(guard, [&]() DUCKDB_REQUIRES(lock) {
			return closed || reserved_clients < capacity || !non_empty_buckets.empty();
		});
	} catch (...) {
		waiting_threads--;
		throw;
	}
	waiting_threads--;
}

HTTPTransportManager::Reservation
HTTPTransportManager::ReserveClientLocked(HTTPTransportManagerState &state, ClientKey key, const string &origin,
                                          annotated_unique_lock<annotated_mutex> &guard) {
	D_ASSERT(state.reuse_policy != HTTPTransportReusePolicy::CLIENT_FREE);
	while (true) {
		ValidateStateLocked(state);

		key.connection_epoch = state.connection_epoch;
		const bool cacheable = state.provider_epoch == current_provider_epoch && !state.reuse_poisoned &&
		                       (state.reuse_policy == HTTPTransportReusePolicy::SESSION_LOCAL ||
		                        state.reuse_policy == HTTPTransportReusePolicy::SHARED);
		auto exact = cacheable ? FindBucketLocked(key, origin) : client_buckets.end();
		if (exact != client_buckets.end() && !exact->second.idle_clients.empty()) {
			Reservation result;
			result.key = key;
			result.cacheable = true;
			result.bucket = exact->second;
			result.client = TakeIdleClientLocked(exact->second);
			result.kind = ReservationKind::REUSE;
			HTTPTransportCapacityGuard::MarkCurrentCapacityOwned(*this);
			return result;
		}

		D_ASSERT(reserved_clients <= capacity);
		if (reserved_clients == capacity && non_empty_buckets.empty()) {
			WaitForAdmissionLocked(guard);
			continue;
		}

		Reservation result;
		result.key = key;
		result.cacheable = cacheable;
		result.kind = ReservationKind::NEW_CLIENT;
		result.bucket_capacity = capacity;
		if (reserved_clients < capacity) {
			reserved_clients++;
		} else {
			D_ASSERT(!non_empty_buckets.empty());
			auto replacement_bucket = non_empty_buckets.back();
			result.client = TakeIdleClientLocked(*replacement_bucket);
			D_ASSERT(replacement_bucket->reserved_clients > 0);
			replacement_bucket->reserved_clients--;
			if (replacement_bucket->reserved_clients == 0) {
				result.bucket_node = ExtractBucketLocked(*replacement_bucket);
			}
		}
		if (exact != client_buckets.end()) {
			exact->second.reserved_clients++;
			result.bucket = exact->second;
		}
		HTTPTransportCapacityGuard::MarkCurrentCapacityOwned(*this);
		return result;
	}
}

void HTTPTransportManager::PrepareBucket(Reservation &reservation, const string &origin) {
	if (!reservation.cacheable || reservation.bucket) {
		return;
	}
	if (reservation.bucket_node) {
		auto &bucket = reservation.bucket_node.mapped();
		D_ASSERT(bucket.reserved_clients == 0);
		D_ASSERT(bucket.idle_clients.empty());
		D_ASSERT(!bucket.non_empty_index.IsValid());
		D_ASSERT(bucket.idle_clients.capacity() >= reservation.bucket_capacity);
		reservation.bucket_node.key() = reservation.key;
		bucket.key = reservation.key;
		bucket.origin = origin;
		bucket.reserved_clients = 1;
	} else {
		ClientBucket bucket;
		bucket.key = reservation.key;
		bucket.origin = origin;
		bucket.idle_clients.reserve(reservation.bucket_capacity);
		bucket.reserved_clients = 1;
		ClientBucketMap pending;
		pending.emplace(reservation.key, std::move(bucket));
		reservation.bucket_node = pending.extract(pending.begin());
	}

	annotated_lock_guard<annotated_mutex> guard(lock);
	auto exact = FindBucketLocked(reservation.key, origin);
	if (exact != client_buckets.end()) {
		exact->second.reserved_clients++;
		reservation.bucket = exact->second;
		return;
	}
	auto inserted = client_buckets.insert(std::move(reservation.bucket_node));
	reservation.bucket = inserted->second;
}

void HTTPTransportManager::PrepareClient(Reservation &reservation, HTTPTransportManagerState &state, HTTPParams &params,
                                         const string &origin) {
	if (reservation.kind == ReservationKind::REUSE && reservation.client) {
		if (reservation.client->CanReuse(params)) {
			reservation.client->Initialize(params);
			return;
		}
	}
	reservation.client.reset();

	HTTPClientInitializationOptions options;
	options.cache_policy = HTTPClientCachePolicy::BYPASS_CACHE;
	reservation.client = state.http_util->InitializeClientExtended(params, origin, options);
	if (!reservation.client && state.reuse_policy != HTTPTransportReusePolicy::EPHEMERAL) {
		throw InvalidConfigurationException("Reusable HTTP provider returned a null client");
	}
}

HTTPTransportManager::Lease HTTPTransportManager::Acquire(Session &session, HTTPParams &params, const string &origin) {
	auto &state = *session.state;
	D_ASSERT(state.reuse_policy != HTTPTransportReusePolicy::CLIENT_FREE);
	ClientKey key;
	key.provider_epoch = state.provider_epoch;
	key.session_id =
	    state.reuse_policy == HTTPTransportReusePolicy::SESSION_LOCAL ? session.session_id : DConstants::INVALID_INDEX;
	key.origin_hash = std::hash<string> {}(origin);

	Reservation reservation;
	{
		annotated_unique_lock<annotated_mutex> guard(lock);
		reservation = ReserveClientLocked(state, key, origin, guard);
	}
	try {
		PrepareBucket(reservation, origin);
		PrepareClient(reservation, state, params, origin);
	} catch (...) {
		DropReservation(std::move(reservation.client), reservation.bucket);
		throw;
	}
	return Lease(*this, state, reservation.bucket, std::move(reservation.client));
}

unique_ptr<HTTPResponse> HTTPTransportManager::PerformRequest(Session &session, BaseRequest &request) {
	if (request.params.transport_manager.get() != this || request.params.transport_state != session.state ||
	    request.params.transport_session_id != session.session_id) {
		throw InvalidInputException("HTTP parameters do not belong to this transport session");
	}

	auto &state = *session.state;
	if (state.reuse_policy == HTTPTransportReusePolicy::CLIENT_FREE) {
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			ValidateStateLocked(state);
		}
		unique_ptr<HTTPClient> client;
		return state.http_util->Request(request, client);
	}

	HTTPTransportCapacityGuard capacity_guard(*this);
	auto lease = Acquire(session, request.params, request.proto_host_port);
	unique_ptr<HTTPResponse> response;
	try {
		response = state.http_util->Request(request, lease.Client());
	} catch (...) {
		lease.InvalidateClient();
		throw;
	}
	if (response->HasRequestError()) {
		lease.InvalidateClient();
	}
	return response;
}

bool HTTPTransportManager::CanReturnClientLocked(const Lease &lease, bool cleanup_succeeded) const {
	if (!cleanup_succeeded || !lease.reusable || !lease.client || closed || !lease.state || !lease.bucket ||
	    !IsStateValidLocked(*lease.state)) {
		return false;
	}
	auto &state = *lease.state;
	auto &key = lease.bucket->key;
	return !state.reuse_poisoned && state.provider_epoch == current_provider_epoch &&
	       state.connection_epoch == key.connection_epoch &&
	       (state.reuse_policy == HTTPTransportReusePolicy::SESSION_LOCAL ||
	        state.reuse_policy == HTTPTransportReusePolicy::SHARED);
}

void HTTPTransportManager::Release(Lease &lease) noexcept {
	D_ASSERT(HTTPTransportCapacityGuard::CurrentThreadOwnsCapacity(*this));

	bool cleanup_succeeded = true;
	if (lease.client) {
		try {
			lease.client->Cleanup();
		} catch (...) {
			cleanup_succeeded = false;
		}
	}

	bool returned = false;
	bool notify_waiter = false;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (CanReturnClientLocked(lease, cleanup_succeeded)) {
			auto &bucket = *lease.bucket;
			D_ASSERT(bucket.idle_clients.size() < bucket.idle_clients.capacity());
			const bool was_empty = bucket.idle_clients.empty();
			bucket.idle_clients.push_back(std::move(lease.client));
			if (was_empty) {
				AddNonEmptyBucketLocked(bucket);
			}
			returned = true;
			notify_waiter = waiting_threads > 0;
		}
	}
	if (notify_waiter) {
		availability.notify_one();
	}
	if (returned) {
		return;
	}

	DropReservation(std::move(lease.client), lease.bucket);
}

void HTTPTransportManager::DropReservation(unique_ptr<HTTPClient> client, optional_ptr<ClientBucket> bucket) noexcept {
	ClientBucketMap::node_type bucket_node;
	client.reset();
	bool notify_waiter;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		D_ASSERT(reserved_clients > 0);
		reserved_clients--;
		if (bucket) {
			D_ASSERT(bucket->reserved_clients > 0);
			bucket->reserved_clients--;
			if (bucket->reserved_clients == 0) {
				bucket_node = ExtractBucketLocked(*bucket);
			}
		}
		notify_waiter = waiting_threads > 0;
	}
	if (notify_waiter) {
		availability.notify_one();
	}
}

bool HTTPTransportManager::MatchesFilter(const ClientKey &key, IdleFilter filter, idx_t first, uint64_t second) const {
	switch (filter) {
	case IdleFilter::PROVIDER:
		return key.provider_epoch == first;
	case IdleFilter::CONNECTION:
		return key.provider_epoch == first && key.connection_epoch < second;
	case IdleFilter::SESSION:
		return key.session_id == first;
	case IdleFilter::ALL:
		return true;
	}
	return false;
}

void HTTPTransportManager::DisposeIdle(IdleFilter filter, idx_t first, uint64_t second) noexcept {
	HTTPTransportCapacityGuard capacity_guard(*this);
	while (true) {
		unique_ptr<HTTPClient> client;
		optional_ptr<ClientBucket> bucket;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			auto entry = client_buckets.begin();
			for (; entry != client_buckets.end(); ++entry) {
				if (!entry->second.idle_clients.empty() && MatchesFilter(entry->first, filter, first, second)) {
					break;
				}
			}
			if (entry == client_buckets.end()) {
				return;
			}
			bucket = entry->second;
			client = TakeIdleClientLocked(*bucket);
			HTTPTransportCapacityGuard::MarkCurrentCapacityOwned(*this);
		}
		DropReservation(std::move(client), bucket);
	}
}

void HTTPTransportManager::DestroySession(idx_t session_id) noexcept {
	if (session_id != DConstants::INVALID_INDEX) {
		DisposeIdle(IdleFilter::SESSION, session_id);
	}
}

void HTTPTransportManager::Invalidate(optional_ptr<HTTPTransportManagerState> state) noexcept {
	if (!state) {
		return;
	}
	uint64_t connection_epoch = 0;
	bool poisoned = false;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!IsStateValidLocked(*state) || state->reuse_poisoned) {
			return;
		}
		poisoned = AdvanceConnectionEpoch(state->connection_epoch, state->reuse_poisoned);
		if (!poisoned) {
			connection_epoch = state->connection_epoch;
		}
	}
	if (poisoned) {
		DisposeIdle(IdleFilter::PROVIDER, state->provider_epoch);
	} else {
		DisposeIdle(IdleFilter::CONNECTION, state->provider_epoch, connection_epoch);
	}
}

void HTTPTransportManager::Close() noexcept {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		closed = true;
	}
	availability.notify_all();
	DisposeIdle(IdleFilter::ALL);
}

} // namespace duckdb
