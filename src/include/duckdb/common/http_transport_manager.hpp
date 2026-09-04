//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/http_transport_manager.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/condition_variable.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

struct DBConfig;
struct HTTPTransportManagerTestHelper;

//! Owns provider publication and bounded HTTP clients for one DatabaseInstance.
//! Sessions and managed parameters borrow the owning DatabaseInstance lifetime.
class HTTPTransportManager {
public:
	class Session {
	public:
		DUCKDB_API Session(Session &&other) noexcept;
		DUCKDB_API Session &operator=(Session &&other) noexcept;
		Session(const Session &) = delete;
		Session &operator=(const Session &) = delete;
		DUCKDB_API ~Session();

	private:
		friend class HTTPTransportManager;

		Session(HTTPTransportManager &manager, HTTPTransportManagerState &state, idx_t session_id,
		        unique_ptr<HTTPParams> params);

	public:
		DUCKDB_API HTTPParams &Parameters();
		DUCKDB_API unique_ptr<HTTPResponse> Request(BaseRequest &request);
		DUCKDB_API void Invalidate() noexcept;

	private:
		void Reset() noexcept;

	private:
		//! Manager borrowed from the owning DatabaseInstance.
		optional_ptr<HTTPTransportManager> manager;
		//! Provider publication captured when the session was initialized.
		optional_ptr<HTTPTransportManagerState> state;
		//! Unique non-recycled identity issued by the manager.
		idx_t session_id = DConstants::INVALID_INDEX;
		//! Parameters owned for the lifetime of this session.
		unique_ptr<HTTPParams> params;
	};

public:
	DUCKDB_API ~HTTPTransportManager();

private:
	explicit HTTPTransportManager(const shared_ptr<HTTPUtil> &initial_http_util);

public:
	DUCKDB_API Session CreateSession(DatabaseInstance &db, const string &path) DUCKDB_EXCLUDES(lock);
	DUCKDB_API Session CreateSession(ClientContext &context, const string &path) DUCKDB_EXCLUDES(lock);
	DUCKDB_API Session CreateSession(optional_ptr<FileOpener> opener, optional_ptr<FileOpenerInfo> info)
	    DUCKDB_EXCLUDES(lock);

private:
	struct ClientKey {
		bool operator==(const ClientKey &other) const;

		//! Provider publication that created the parameters.
		idx_t provider_epoch = DConstants::INVALID_INDEX;
		//! Connection generation captured when capacity was reserved.
		uint64_t connection_epoch = 0;
		//! Creating session for local reuse, or INVALID_INDEX otherwise.
		idx_t session_id = DConstants::INVALID_INDEX;
		//! Hash of the structural scheme, host, and port identity.
		hash_t origin_hash = 0;
	};

	struct ClientKeyHash {
		hash_t operator()(const ClientKey &key) const;
	};

	struct ClientBucket {
		//! Structural key duplicated for stable bucket lookup and removal.
		ClientKey key;
		//! Collision-checking scheme, host, and port identity.
		string origin;
		//! Reusable clients currently available for this exact origin.
		vector<unique_ptr<HTTPClient>> idle_clients;
		//! Count of creating, leased, destroying, and idle clients for this bucket.
		idx_t reserved_clients = 0;
		//! Position in non_empty_buckets while at least one client is idle.
		optional_idx non_empty_index;
	};

	using ClientBucketMap = std::unordered_multimap<ClientKey, ClientBucket, ClientKeyHash>;

	class Lease {
	public:
		Lease(Lease &&other) noexcept;
		Lease(const Lease &) = delete;
		Lease &operator=(const Lease &) = delete;
		Lease &operator=(Lease &&other) = delete;
		~Lease();

	private:
		friend class HTTPTransportManager;

		Lease(HTTPTransportManager &manager, HTTPTransportManagerState &state, optional_ptr<ClientBucket> bucket,
		      unique_ptr<HTTPClient> client);

	public:
		unique_ptr<HTTPClient> &Client();
		void InvalidateClient() noexcept;

	private:
		void Reset() noexcept;

	private:
		//! Manager borrowed from the owning DatabaseInstance.
		optional_ptr<HTTPTransportManager> manager;
		//! Provider publication captured by the creating session.
		optional_ptr<HTTPTransportManagerState> state;
		//! Stable bucket accounting for this reusable reservation.
		optional_ptr<ClientBucket> bucket;
		//! Client held exclusively by the synchronous request.
		unique_ptr<HTTPClient> client;
		//! Whether this request may return its client to the idle pool.
		bool reusable = true;
	};

	struct SessionReservation {
		//! Provider publication captured for the new session.
		optional_ptr<HTTPTransportManagerState> state;
		//! Unique non-recycled identity reserved for the new session.
		idx_t session_id = DConstants::INVALID_INDEX;
	};

	enum class ReservationKind : uint8_t { REUSE, NEW_CLIENT };
	enum class IdleFilter : uint8_t { PROVIDER, CONNECTION, SESSION, ALL };

	struct Reservation {
		//! Structural key captured for this reservation.
		ClientKey key;
		//! Stable bucket accounting for a cacheable reservation.
		optional_ptr<ClientBucket> bucket;
		//! Idle client selected for reuse or replacement.
		unique_ptr<HTTPClient> client;
		//! Extracted or preallocated bucket node for a new exact origin.
		ClientBucketMap::node_type bucket_node;
		//! Work needed before the request can use the client.
		ReservationKind kind = ReservationKind::NEW_CLIENT;
		//! Whether this reservation may create and return through a bucket.
		bool cacheable = false;
		//! Capacity captured for out-of-lock bucket vector reservation.
		idx_t bucket_capacity = 0;
	};

private:
	friend struct DBConfig;
	friend class DatabaseInstance;
	friend struct HTTPTransportManagerTestHelper;

	//! Database lifecycle helpers.
	static unique_ptr<HTTPTransportManager> Create(const shared_ptr<HTTPUtil> &initial_http_util);
	void Initialize(idx_t system_concurrency) DUCKDB_EXCLUDES(lock);
	void Close() noexcept DUCKDB_EXCLUDES(lock);
	static idx_t CalculateCapacity(idx_t system_concurrency, optional_idx file_descriptor_limit);
	static optional_idx GetFileDescriptorLimit();
	static bool AdvanceConnectionEpoch(uint64_t &connection_epoch, bool &reuse_poisoned) noexcept;
	void SetHTTPUtil(const shared_ptr<HTTPUtil> &new_http_util) DUCKDB_EXCLUDES(lock);
	HTTPUtil &GetHTTPUtil() const DUCKDB_EXCLUDES(lock);

	//! Session lifecycle helpers.
	void ValidateDatabase(DatabaseInstance &db) const;
	SessionReservation ReserveSession() DUCKDB_EXCLUDES(lock);
	Session FinishSession(SessionReservation reservation, unique_ptr<HTTPParams> params) DUCKDB_EXCLUDES(lock);
	void DestroySession(idx_t session_id) noexcept DUCKDB_EXCLUDES(lock);
	void Invalidate(optional_ptr<HTTPTransportManagerState> state) noexcept DUCKDB_EXCLUDES(lock);

	//! Request lifecycle helpers.
	unique_ptr<HTTPResponse> PerformRequest(Session &session, BaseRequest &request) DUCKDB_EXCLUDES(lock);
	Lease Acquire(Session &session, HTTPParams &params, const string &origin) DUCKDB_EXCLUDES(lock);
	Reservation ReserveClientLocked(HTTPTransportManagerState &state, ClientKey key, const string &origin,
	                                annotated_unique_lock<annotated_mutex> &guard) DUCKDB_REQUIRES(lock);
	void PrepareBucket(Reservation &reservation, const string &origin) DUCKDB_EXCLUDES(lock);
	void PrepareClient(Reservation &reservation, HTTPTransportManagerState &state, HTTPParams &params,
	                   const string &origin) DUCKDB_EXCLUDES(lock);
	void Release(Lease &lease) noexcept DUCKDB_EXCLUDES(lock);
	void DropReservation(unique_ptr<HTTPClient> client, optional_ptr<ClientBucket> bucket = nullptr) noexcept
	    DUCKDB_EXCLUDES(lock);

	//! Lock-held state helpers.
	void ValidateStateLocked(const HTTPTransportManagerState &state) const DUCKDB_REQUIRES(lock);
	bool IsStateValidLocked(const HTTPTransportManagerState &state) const DUCKDB_REQUIRES(lock);
	void WaitForAdmissionLocked(annotated_unique_lock<annotated_mutex> &guard) DUCKDB_REQUIRES(lock);
	bool CanReturnClientLocked(const Lease &lease, bool cleanup_succeeded) const DUCKDB_REQUIRES(lock);
	ClientBucketMap::iterator FindBucketLocked(const ClientKey &key, const string &origin) DUCKDB_REQUIRES(lock);
	ClientBucketMap::node_type ExtractBucketLocked(ClientBucket &bucket) DUCKDB_REQUIRES(lock);
	unique_ptr<HTTPClient> TakeIdleClientLocked(ClientBucket &bucket) DUCKDB_REQUIRES(lock);
	void AddNonEmptyBucketLocked(ClientBucket &bucket) DUCKDB_REQUIRES(lock);
	void RemoveNonEmptyBucketLocked(ClientBucket &bucket) DUCKDB_REQUIRES(lock);

	//! Idle disposal helpers.
	void DisposeIdle(IdleFilter filter, idx_t first = DConstants::INVALID_INDEX, uint64_t second = 0) noexcept
	    DUCKDB_EXCLUDES(lock);
	bool MatchesFilter(const ClientKey &key, IdleFilter filter, idx_t first, uint64_t second) const;

private:
	//! Protects all mutable manager state below.
	mutable annotated_mutex lock;
	//! Wakes waiters when capacity, an idle client, or shutdown becomes available.
	condition_variable availability;
	//! Retained provider publications indexed by provider epoch.
	vector<unique_ptr<HTTPTransportManagerState>> providers DUCKDB_GUARDED_BY(lock);
	//! Reusable client buckets grouped by fixed structural hashes.
	ClientBucketMap client_buckets DUCKDB_GUARDED_BY(lock);
	//! Buckets containing at least one idle client for O(1) eviction.
	vector<optional_ptr<ClientBucket>> non_empty_buckets DUCKDB_GUARDED_BY(lock);
	//! Provider epoch selected for new sessions.
	idx_t current_provider_epoch DUCKDB_GUARDED_BY(lock) = 0;
	//! Next non-recycled manager session identity.
	idx_t next_session_id DUCKDB_GUARDED_BY(lock) = 1;
	//! Configured maximum number of counted clients and reservations.
	idx_t capacity DUCKDB_GUARDED_BY(lock) = 0;
	//! Count of creating, leased, destroying, and idle clients.
	idx_t reserved_clients DUCKDB_GUARDED_BY(lock) = 0;
	//! Number of threads currently waiting for capacity or an idle client.
	idx_t waiting_threads DUCKDB_GUARDED_BY(lock) = 0;
	//! Whether Configure installed the capacity and bucket indexes.
	bool initialized DUCKDB_GUARDED_BY(lock) = false;
	//! Whether database teardown permanently closed admission.
	bool closed DUCKDB_GUARDED_BY(lock) = false;
};

} // namespace duckdb
