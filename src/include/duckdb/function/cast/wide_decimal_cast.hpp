//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb/function/cast/wide_decimal_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/bignum_decimal.hpp"

namespace duckdb {

//! Arithmetic and formatting buffers reused by a bound wide-decimal cast.
struct WideDecimalCastState : public FunctionLocalState {
	explicit WideDecimalCastState(optional_ptr<ClientContext> context_p) : context(context_p) {
	}

	DecimalHelper<bignum_t>::RescaleState &GetRescaleState() {
		if (!rescale) {
			rescale = make_uniq<DecimalHelper<bignum_t>::RescaleState>(context);
		}
		return *rescale;
	}

	BignumCastScratch &GetFormattingState() {
		if (!formatting) {
			formatting = make_uniq<BignumCastScratch>(context);
		}
		return *formatting;
	}

	optional_ptr<ClientContext> context;
	string unscaled_blob;
	string formatted;

private:
	unique_ptr<DecimalHelper<bignum_t>::RescaleState> rescale;
	unique_ptr<BignumCastScratch> formatting;
};

} // namespace duckdb
