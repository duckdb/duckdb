#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb::capiv2 {

//----------------------------------------------------------------------------------------------------------------------
// Connection state
//----------------------------------------------------------------------------------------------------------------------

//! What an init callback configures. Its user data is released when the builder is, i.e. right away if the init fails.
class CV2ContextStateBuilder {
public:
	shared_ptr<CV2UserData> user_data;
	duckdb_v2_context_state_query_begin_callback_fn query_begin_cb = nullptr;
	duckdb_v2_context_state_query_end_callback_fn query_end_cb = nullptr;
	duckdb_v2_context_state_transaction_begin_callback_fn transaction_begin_cb = nullptr;
	duckdb_v2_context_state_transaction_end_callback_fn transaction_commit_cb = nullptr;
	duckdb_v2_context_state_transaction_end_callback_fn transaction_rollback_cb = nullptr;
	duckdb_v2_context_state_profiling_callback_fn profiling_cb = nullptr;
};

static auto Convert(duckdb_v2_context_state_handle state) -> CV2ContextStateBuilder * {
	return reinterpret_cast<CV2ContextStateBuilder *>(state);
}
static auto Convert(CV2ContextStateBuilder *state) -> duckdb_v2_context_state_handle {
	return reinterpret_cast<duckdb_v2_context_state_handle>(state);
}

//! The profile being written: the stream the registered states append to.
class CV2ContextStateProfile {
public:
	explicit CV2ContextStateProfile(std::ostream &ss) : ss(ss) {
	}

	std::ostream &ss;
};

static auto Convert(duckdb_v2_context_state_profile_handle profile) -> CV2ContextStateProfile * {
	return reinterpret_cast<CV2ContextStateProfile *>(profile);
}
static auto Convert(CV2ContextStateProfile *profile) -> duckdb_v2_context_state_profile_handle {
	return reinterpret_cast<duckdb_v2_context_state_profile_handle>(profile);
}

//! Connection state set up through the C API: forwards ClientContextState's hooks to the callbacks the init set.
class CV2ContextState final : public ClientContextState {
public:
	CV2ContextState(CV2Context &context, duckdb_v2_context_state_init_callback_fn init, void *init_user_data) {
		CV2ContextStateBuilder builder;
		InvokeWithErrorSlot(
		    [&](duckdb_v2_error_info_handle *err) { init(Convert(&context), init_user_data, Convert(&builder), err); });
		callbacks = std::move(builder);
	}

	void *GetUserData() const {
		return callbacks.user_data ? callbacks.user_data->GetData() : nullptr;
	}

	using ClientContextState::QueryEnd;
	using ClientContextState::TransactionRollback;

	void QueryBegin(ClientContext &context) override {
		if (!callbacks.query_begin_cb) {
			return;
		}
		CV2CallbackContext handle(context);
		InvokeWithErrorSlot(
		    [&](duckdb_v2_error_info_handle *err) { callbacks.query_begin_cb(Convert(&handle), GetUserData(), err); });
	}

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override {
		if (!callbacks.query_end_cb) {
			return;
		}
		CV2CallbackContext handle(context);
		if (error && error->HasError()) {
			auto message = error->Message();
			callbacks.query_end_cb(Convert(&handle), GetUserData(), message.data(), message.size());
		} else {
			callbacks.query_end_cb(Convert(&handle), GetUserData(), nullptr, 0);
		}
	}

	void TransactionBegin(MetaTransaction &transaction, ClientContext &context) override {
		if (!callbacks.transaction_begin_cb) {
			return;
		}
		CV2CallbackContext handle(context);
		InvokeWithErrorSlot([&](duckdb_v2_error_info_handle *err) {
			callbacks.transaction_begin_cb(Convert(&handle), GetUserData(), err);
		});
	}

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		if (callbacks.transaction_commit_cb) {
			CV2CallbackContext handle(context);
			callbacks.transaction_commit_cb(Convert(&handle), GetUserData());
		}
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context,
	                         optional_ptr<ErrorData> error) override {
		if (callbacks.transaction_rollback_cb) {
			CV2CallbackContext handle(context);
			callbacks.transaction_rollback_cb(Convert(&handle), GetUserData());
		}
	}

	void WriteProfilingInformation(std::ostream &ss) override {
		if (!callbacks.profiling_cb) {
			return;
		}
		CV2ContextStateProfile profile(ss);
		CV2ErrorInfo info;
		auto handle = Convert(&info);
		// an error is dropped: the profile is written whatever the state adds to it
		callbacks.profiling_cb(Convert(&profile), GetUserData(), &handle);
	}

private:
	CV2ContextStateBuilder callbacks;
};

//! The C API state under `key`, or null if the key has none. Throws if it is held by state of something else.
static auto FindState(ClientContext &client, const string &key) -> shared_ptr<CV2ContextState> {
	auto existing = client.GetContext().GetRegisteredState().Get<ClientContextState>(key);
	if (!existing) {
		return nullptr;
	}
	if (!dynamic_cast<CV2ContextState *>(existing.get())) {
		throw InvalidInputException("The key \"%s\" is in use by state that is not registered through the C API", key);
	}
	return shared_ptr_cast<ClientContextState, CV2ContextState>(std::move(existing));
}

static void GetOrCreateState(ClientContext &client, CV2Context &context, const string &key,
                             duckdb_v2_context_state_init_callback_fn init, void *init_user_data, void **data) {
	auto state = FindState(client, key);
	if (!state) {
		state =
		    client.GetContext().GetRegisteredState().GetOrCreate<CV2ContextState>(key, context, init, init_user_data);
	}
	if (data) {
		*data = state->GetUserData();
	}
}

static void GetState(ClientContext &client, const string &key, bool *found, void **data) {
	auto state = FindState(client, key);
	*found = state != nullptr;
	if (state && data) {
		*data = state->GetUserData();
	}
}

static void RemoveState(ClientContext &client, const string &key) {
	// refuse to remove what the C API did not register
	FindState(client, key);
	client.GetContext().GetRegisteredState().Remove(key);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_context_get_or_create_state(duckdb_v2_context_handle context, const char *key,
                                                      idx_t key_length, duckdb_v2_context_state_init_callback_fn init,
                                                      void *init_user_data, void **data,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_ARG(init);
	if (data) {
		*data = nullptr;
	}
	return WithErrorHandler(err, [&]() {
		GetOrCreateState(Convert(context)->context, *Convert(context), duckdb::string(key, key_length), init,
		                 init_user_data, data);
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_get_or_create_state(duckdb_v2_connection_handle conn, const char *key,
                                                         idx_t key_length,
                                                         duckdb_v2_context_state_init_callback_fn init,
                                                         void *init_user_data, void **data,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_ARG(init);
	if (data) {
		*data = nullptr;
	}
	return WithErrorHandler(err, [&]() {
		auto &connection = *Convert(conn);
		GetOrCreateState(*connection.context, connection.context_handle, duckdb::string(key, key_length), init,
		                 init_user_data, data);
	});
}

DUCKDB_V2_ERROR duckdb_v2_context_get_state(duckdb_v2_context_handle context, const char *key, idx_t key_length,
                                            bool *found, void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_ARG(found);
	return WithErrorHandler(
	    err, [&]() { GetState(Convert(context)->context, duckdb::string(key, key_length), found, data); });
}

DUCKDB_V2_ERROR duckdb_v2_connection_get_state(duckdb_v2_connection_handle conn, const char *key, idx_t key_length,
                                               bool *found, void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_ARG(found);
	return WithErrorHandler(err,
	                        [&]() { GetState(*Convert(conn)->context, duckdb::string(key, key_length), found, data); });
}

DUCKDB_V2_ERROR duckdb_v2_context_remove_state(duckdb_v2_context_handle context, const char *key, idx_t key_length,
                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	return WithErrorHandler(err, [&]() { RemoveState(Convert(context)->context, duckdb::string(key, key_length)); });
}

DUCKDB_V2_ERROR duckdb_v2_connection_remove_state(duckdb_v2_connection_handle conn, const char *key, idx_t key_length,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	return WithErrorHandler(err, [&]() { RemoveState(*Convert(conn)->context, duckdb::string(key, key_length)); });
}

DUCKDB_V2_ERROR duckdb_v2_context_state_set_user_data(duckdb_v2_context_state_handle state, duckdb_v2_opaque *data,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(state);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(state)->user_data = duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR
duckdb_v2_context_state_set_query_begin_callback(duckdb_v2_context_state_handle state,
                                                 duckdb_v2_context_state_query_begin_callback_fn callback,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(state);
	return WithErrorHandler(err, [&]() { Convert(state)->query_begin_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_context_state_set_query_end_callback(duckdb_v2_context_state_handle state,
                                                               duckdb_v2_context_state_query_end_callback_fn callback,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(state);
	return WithErrorHandler(err, [&]() { Convert(state)->query_end_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_context_state_set_transaction_begin_callback(duckdb_v2_context_state_handle state,
                                                       duckdb_v2_context_state_transaction_begin_callback_fn callback,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(state);
	return WithErrorHandler(err, [&]() { Convert(state)->transaction_begin_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_context_state_set_transaction_commit_callback(duckdb_v2_context_state_handle state,
                                                        duckdb_v2_context_state_transaction_end_callback_fn callback,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(state);
	return WithErrorHandler(err, [&]() { Convert(state)->transaction_commit_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_context_state_set_transaction_rollback_callback(duckdb_v2_context_state_handle state,
                                                          duckdb_v2_context_state_transaction_end_callback_fn callback,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(state);
	return WithErrorHandler(err, [&]() { Convert(state)->transaction_rollback_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_context_state_set_profiling_callback(duckdb_v2_context_state_handle state,
                                                               duckdb_v2_context_state_profiling_callback_fn callback,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(state);
	return WithErrorHandler(err, [&]() { Convert(state)->profiling_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_context_state_profile_write(duckdb_v2_context_state_profile_handle profile, const char *text,
                                                      idx_t text_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(profile);
	DUCKDB_CHECK_STR_ARG(text, text_length);
	return WithErrorHandler(err,
	                        [&]() { Convert(profile)->ss.write(text, static_cast<std::streamsize>(text_length)); });
}
