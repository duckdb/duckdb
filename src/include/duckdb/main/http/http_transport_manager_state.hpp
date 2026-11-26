//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/http/http_transport_manager_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/http/http_util.hpp"

namespace duckdb {

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

} // namespace duckdb
