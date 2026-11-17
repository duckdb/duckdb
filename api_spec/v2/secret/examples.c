/*
 * Draft usage of the secret API (api_spec/v2/secret/secret.yaml), sketching how the extensions that use the C++
 * secret manager today would use it. Compile-checked only, never linked:
 *
 *   cc -fsyntax-only -Wall -D_GNU_SOURCE -I src/include api_spec/v2/secret/examples.c
 *
 * Error handling is elided to keep the shape visible: every call returns DUCKDB_V2_ERROR and takes an optional `err`.
 */
#define DUCKDB_V2_API_ALLOW_UNSTABLE 1
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include "duckdb_v2.h"

/* A string argument is a pointer and a length: S("abc") expands to both. */
#define S(s) (s), strlen(s)

/* A borrowed string received from the API. */
typedef struct {
	const char *ptr;
	idx_t len;
} sv;

/* ------------------------------------------------------------------------------------------------------------------
 * httpfs: `CREATE SECRET (TYPE s3, ...)` and the credentials lookup of an S3 request.
 * (create_secret_functions.cpp, s3_auth.cpp)
 * ---------------------------------------------------------------------------------------------------------------- */

static void CreateS3SecretFromConfig(duckdb_v2_secret_create_input_handle input,
                                     duckdb_v2_secret_builder_handle builder, duckdb_v2_context_handle context,
                                     duckdb_v2_error_info_handle *err) {
	/* S3SecretBuilder: copy every option into the secret (validating the ones that need it) */
	idx_t count = 0;
	duckdb_v2_secret_create_input_get_option_count(input, &count, NULL);
	for (idx_t i = 0; i < count; i++) {
		sv name;
		duckdb_v2_value_handle value = NULL;
		duckdb_v2_secret_create_input_get_option_name(input, i, &name.ptr, &name.len, NULL);
		duckdb_v2_secret_create_input_get_option_value(input, i, &value, NULL);
		duckdb_v2_secret_builder_set_value(builder, name.ptr, name.len, value, err);
		duckdb_v2_value_destroy(&value);
	}
	/* only a secret that carries an SSE-C key hides its refresh info: redaction is decided per secret */
	duckdb_v2_value_handle sse_key = NULL;
	duckdb_v2_secret_create_input_get_option(input, S("sse_c_key"), &sse_key, NULL);
	if (sse_key) {
		duckdb_v2_secret_builder_add_redacted_key(builder, S("refresh_info"), NULL);
		duckdb_v2_value_destroy(&sse_key);
	}
	/* a default scope when the user gave none (S3SecretConfig::DefaultSecretScope) */
	idx_t scopes = 0;
	duckdb_v2_secret_create_input_get_scope_count(input, &scopes, NULL);
	if (scopes == 0) {
		duckdb_v2_secret_builder_add_scope(builder, S("s3://"), NULL);
	}
}

static void RegisterS3Secret(duckdb_v2_extension_handle extension, duckdb_v2_factory_handle factory) {
	duckdb_v2_secret_type_handle type = NULL;
	duckdb_v2_secret_type_create(factory, &type, NULL);
	duckdb_v2_secret_type_set_name(type, S("s3"), NULL);
	duckdb_v2_secret_type_set_default_provider(type, S("config"), NULL);
	duckdb_v2_secret_type_add_redacted_key(type, S("secret"), NULL);
	duckdb_v2_secret_type_add_redacted_key(type, S("session_token"), NULL);
	duckdb_v2_extension_register_secret_type(extension, type, NULL);
	duckdb_v2_secret_type_destroy(&type);

	duckdb_v2_secret_function_handle function = NULL;
	duckdb_v2_secret_function_create(factory, &function, NULL);
	duckdb_v2_secret_function_set_type(function, S("s3"), NULL);
	duckdb_v2_secret_function_set_provider(function, S("config"), NULL);
	duckdb_v2_logical_type_handle varchar = NULL;
	duckdb_v2_factory_create_type_from_id(factory, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, NULL, NULL, NULL, 0, &varchar,
	                                      NULL);
	duckdb_v2_secret_function_add_named_parameter(function, S("key_id"), varchar, NULL);
	duckdb_v2_secret_function_add_named_parameter(function, S("region"), varchar, NULL);
	duckdb_v2_secret_function_set_create_callback(function, CreateS3SecretFromConfig, NULL);
	duckdb_v2_extension_register_secret_function(extension, function, NULL);
	duckdb_v2_secret_function_destroy(&function);
	duckdb_v2_logical_type_destroy(&varchar);
}

/* The credentials of a request: the best `s3` secret for the path, then value by value. */
static bool GetS3Region(duckdb_v2_context_handle ctx, const char *path, char *out, idx_t out_len) {
	duckdb_v2_secret_handle secret = NULL;
	duckdb_v2_context_lookup_secret(ctx, S(path), S("s3"), &secret, NULL, NULL);
	if (!secret) {
		return false; /* the caller falls back to the `s3_region` option: duckdb_v2_config_get_option_value */
	}
	duckdb_v2_value_handle value = NULL;
	duckdb_v2_secret_get_value(secret, S("region"), &value, NULL);
	bool found = false;
	if (value) {
		sv region;
		if (duckdb_v2_value_get_varchar(value, &region.ptr, &region.len, NULL) == DUCKDB_V2_ERROR_NONE &&
		    region.len < out_len) {
			memcpy(out, region.ptr, region.len);
			out[region.len] = 0;
			found = true;
		}
		duckdb_v2_value_destroy(&value);
	}
	duckdb_v2_secret_destroy(&secret);
	return found;
}

/* httpfs and iceberg refresh: re-create a secret from the one it replaces (GenerateRefreshSecretInfo). */
static void RefreshSecret(duckdb_v2_context_handle ctx, duckdb_v2_factory_handle factory, duckdb_v2_secret_handle old) {
	duckdb_v2_secret_create_input_handle input = NULL;
	duckdb_v2_secret_create_input_create(factory, &input, NULL);
	sv s;
	duckdb_v2_secret_get_type(old, &s.ptr, &s.len, NULL);
	duckdb_v2_secret_create_input_set_type(input, s.ptr, s.len, NULL);
	duckdb_v2_secret_get_name(old, &s.ptr, &s.len, NULL);
	duckdb_v2_secret_create_input_set_name(input, s.ptr, s.len, NULL);
	duckdb_v2_secret_get_storage(old, &s.ptr, &s.len, NULL);
	duckdb_v2_secret_create_input_set_storage(input, s.ptr, s.len, NULL);
	DUCKDB_V2_SECRET_PERSIST_TYPE persist;
	duckdb_v2_secret_get_persist_type(old, &persist, NULL);
	duckdb_v2_secret_create_input_set_persist_type(input, persist, NULL);
	idx_t scopes = 0;
	duckdb_v2_secret_get_scope_count(old, &scopes, NULL);
	for (idx_t i = 0; i < scopes; i++) {
		duckdb_v2_secret_get_scope(old, i, &s.ptr, &s.len, NULL);
		duckdb_v2_secret_create_input_add_scope(input, s.ptr, s.len, NULL);
	}
	duckdb_v2_secret_create_input_set_on_conflict(input, DUCKDB_V2_SECRET_ON_CONFLICT_REPLACE, NULL);
	/* the refreshed options (e.g. `refresh_info`) are set with duckdb_v2_secret_create_input_set_option */
	duckdb_v2_context_create_secret(ctx, input, NULL, NULL);
	duckdb_v2_secret_create_input_destroy(&input);
}

/* ------------------------------------------------------------------------------------------------------------------
 * quack: a secret by name, or the single secret of the type. (quack_secret.cpp)
 * ---------------------------------------------------------------------------------------------------------------- */

static duckdb_v2_secret_handle QuackFindSingleSecret(duckdb_v2_context_handle ctx) {
	duckdb_v2_secret_list_handle list = NULL;
	duckdb_v2_context_list_secrets(ctx, &list, NULL);
	idx_t count = 0;
	duckdb_v2_secret_list_get_count(list, &count, NULL);
	duckdb_v2_secret_handle result = NULL;
	for (idx_t i = 0; i < count; i++) {
		duckdb_v2_secret_handle secret = NULL;
		duckdb_v2_secret_list_get_secret(list, i, &secret, NULL);
		sv type;
		duckdb_v2_secret_get_type(secret, &type.ptr, &type.len, NULL);
		if (type.len != 5 || strncasecmp(type.ptr, "quack", 5) != 0) {
			duckdb_v2_secret_destroy(&secret);
			continue;
		}
		if (result) { /* ambiguous: the caller falls back to its defaults */
			duckdb_v2_secret_destroy(&secret);
			duckdb_v2_secret_destroy(&result);
			break;
		}
		result = secret;
	}
	duckdb_v2_secret_list_destroy(&list);
	return result;
}

static duckdb_v2_secret_handle QuackFindByName(duckdb_v2_context_handle ctx, const char *name) {
	duckdb_v2_secret_handle secret = NULL;
	duckdb_v2_context_get_secret_by_name(ctx, S(name), &secret, NULL);
	return secret; /* NULL: the caller reports "not found"; the type is checked with duckdb_v2_secret_get_type */
}

/* quack's CreateDefault: a persistent secret, ignoring a name that is taken */
static void QuackCreateDefault(duckdb_v2_context_handle ctx, duckdb_v2_factory_handle factory,
                               duckdb_v2_value_handle token) {
	duckdb_v2_secret_create_input_handle input = NULL;
	duckdb_v2_secret_create_input_create(factory, &input, NULL);
	duckdb_v2_secret_create_input_set_type(input, S("quack"), NULL);
	duckdb_v2_secret_create_input_set_name(input, S("__default_quack"), NULL);
	duckdb_v2_secret_create_input_set_option(input, S("token"), token, NULL);
	duckdb_v2_secret_create_input_set_persist_type(input, DUCKDB_V2_SECRET_PERSIST_TYPE_PERSISTENT, NULL);
	duckdb_v2_secret_create_input_set_on_conflict(input, DUCKDB_V2_SECRET_ON_CONFLICT_IGNORE, NULL);
	duckdb_v2_context_create_secret(ctx, input, NULL, NULL);
	duckdb_v2_secret_create_input_destroy(&input);
}

/* ------------------------------------------------------------------------------------------------------------------
 * iceberg: the best secret across several types, ranked by match score. (iceberg_catalog.cpp GetStorageSecret)
 * Vended credentials are created as transaction secrets. (iceberg_table.cpp LoadCredentials)
 * ---------------------------------------------------------------------------------------------------------------- */

static duckdb_v2_secret_handle IcebergBestStorageSecret(duckdb_v2_context_handle ctx) {
	static const char *types[] = {"s3", "aws"};
	static const char *paths[] = {"s3://", "aws://"};
	duckdb_v2_secret_handle best = NULL;
	int64_t best_score = INT64_MIN;
	for (int i = 0; i < 2; i++) {
		duckdb_v2_secret_handle secret = NULL;
		int64_t score = 0;
		duckdb_v2_context_lookup_secret(ctx, S(paths[i]), S(types[i]), &secret, &score, NULL);
		if (!secret) {
			continue;
		}
		if (best && score <= best_score) {
			duckdb_v2_secret_destroy(&secret);
			continue;
		}
		duckdb_v2_secret_destroy(&best);
		best = secret;
		best_score = score;
	}
	return best;
}

static void IcebergCreateVendedSecret(duckdb_v2_context_handle ctx, duckdb_v2_factory_handle factory,
                                      const char *table_location, duckdb_v2_value_handle key_id) {
	duckdb_v2_secret_create_input_handle input = NULL;
	duckdb_v2_secret_create_input_create(factory, &input, NULL);
	duckdb_v2_secret_create_input_set_type(input, S("s3"), NULL);
	duckdb_v2_secret_create_input_add_scope(input, S(table_location), NULL);
	duckdb_v2_secret_create_input_set_option(input, S("key_id"), key_id, NULL);
	duckdb_v2_secret_create_input_set_persist_type(input, DUCKDB_V2_SECRET_PERSIST_TYPE_TRANSACTION, NULL);
	duckdb_v2_secret_create_input_set_on_conflict(input, DUCKDB_V2_SECRET_ON_CONFLICT_REPLACE, NULL);
	duckdb_v2_context_create_secret(ctx, input, NULL, NULL);
	duckdb_v2_secret_create_input_destroy(&input);
}

/* ------------------------------------------------------------------------------------------------------------------
 * delta, lance, unity_catalog: hand the keys of the best secret for a path to a client of their own.
 * (delta_multi_file_list.cpp, lance_common.cpp)
 * ---------------------------------------------------------------------------------------------------------------- */

static void CopySecretToOptions(duckdb_v2_context_handle ctx, const char *path, const char *type,
                                void (*set_option)(void *, sv, duckdb_v2_value_handle), void *target) {
	duckdb_v2_secret_handle secret = NULL;
	duckdb_v2_context_lookup_secret(ctx, S(path), S(type), &secret, NULL, NULL);
	if (!secret) {
		return;
	}
	idx_t keys = 0;
	duckdb_v2_secret_get_key_count(secret, &keys, NULL);
	for (idx_t i = 0; i < keys; i++) {
		sv key;
		duckdb_v2_value_handle value = NULL;
		duckdb_v2_secret_get_key(secret, i, &key.ptr, &key.len, NULL);
		duckdb_v2_secret_get_value(secret, key.ptr, key.len, &value, NULL);
		set_option(target, key, value);
		duckdb_v2_value_destroy(&value);
	}
	duckdb_v2_secret_destroy(&secret);
}

/* ducklake, postgres, azure: a type with a named parameter per option and a "config" provider whose callback is the
 * option loop of CreateS3SecretFromConfig, with the sensitive keys declared on the type. */
