#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

//! The observer owns only the flag, never the buffer whose destruction it observes.
inline shared_ptr<bool> ObserveVectorBufferDestruction(Vector &vector) {
	struct DestructionMarker : AuxiliaryDataHolder {
		explicit DestructionMarker(shared_ptr<bool> destroyed_p) : destroyed(std::move(destroyed_p)) {
		}
		~DestructionMarker() override {
			*destroyed = true;
		}
		shared_ptr<bool> destroyed;
	};
	auto destroyed = make_shared_ptr<bool>(false);
	vector.BufferMutable().AddAuxiliaryData(make_uniq<DestructionMarker>(destroyed));
	return destroyed;
}

} // namespace duckdb
