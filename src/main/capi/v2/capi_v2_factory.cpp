#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_connection_get_factory(duckdb_v2_connection_handle connection,
                                                 duckdb_v2_factory_handle *out_factory,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(connection);
	DUCKDB_CHECK_ARG(out_factory);
	*out_factory = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &context = *Convert(connection)->context;
		*out_factory = Convert(&GetFactorySlot(context)->connection_factory);
	});
}

DUCKDB_V2_ERROR duckdb_v2_context_get_factory(duckdb_v2_context_handle context, duckdb_v2_factory_handle *out_factory,
                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_ARG(out_factory);
	*out_factory = nullptr;
	return WithErrorHandler(err,
	                        [&]() { *out_factory = Convert(&GetFactorySlot(*Convert(context))->context_factory); });
}
