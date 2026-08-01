//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tz_reference.cpp
//
// Temporary: verifies the built-in time zone implementation against ICU.
// This file (and the icu_timezone_verify function) is removed once ICU is gone.
//
//===----------------------------------------------------------------------===//

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "grego.hpp"
#include "timezone.hpp"
#include "unicode/basictz.h"
#include "unicode/timezone.h"

namespace duckdb {
namespace datetime {

namespace {

//! A single instant/local time at which the two implementations are compared
struct Probe {
	double millis;
	//! The name of the lookup that is performed, used to report where a mismatch came from
	const char *kind;
	bool local;
	LocalOption non_existing;
	LocalOption duplicated;
};

struct Mismatch {
	string zone;
	double millis;
	const char *kind;
	int32_t expected_raw;
	int32_t expected_dst;
	int32_t actual_raw;
	int32_t actual_dst;
};

struct VerifyData : public GlobalTableFunctionState {
	vector<Mismatch> mismatches;
	idx_t offset = 0;
};

void AddProbes(vector<Probe> &probes, double millis) {
	// probe the instant itself and the milliseconds around it, in both directions
	for (const auto delta : {-1000.0, -1.0, 0.0, 1.0, 1000.0}) {
		probes.push_back({millis + delta, "instant", false, LocalOption::FORMER, LocalOption::LATTER});
		probes.push_back({millis + delta, "local-former-latter", true, LocalOption::FORMER, LocalOption::LATTER});
		probes.push_back({millis + delta, "local-latter-former", true, LocalOption::LATTER, LocalOption::FORMER});
		probes.push_back({millis + delta, "local-former-former", true, LocalOption::FORMER, LocalOption::FORMER});
		probes.push_back({millis + delta, "local-latter-latter", true, LocalOption::LATTER, LocalOption::LATTER});
	}
}

void Verify(const string &name, vector<Mismatch> &mismatches) {
	auto actual = TimeZone::TryCreate(name);
	if (!actual) {
		mismatches.push_back({name, 0, "unknown zone", 0, 0, 0, 0});
		return;
	}
	duckdb::unique_ptr<icu::TimeZone> expected(
	    icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(name))));
	auto basic = dynamic_cast<icu::BasicTimeZone *>(expected.get());

	vector<Probe> probes;
	// a coarse sweep over the whole range in which zones have data
	for (int32_t year = 1600; year < 2400; year++) {
		for (int32_t day = 0; day < 365; day += 97) {
			AddProbes(probes, double(Grego::FieldsToDay(year, 0, 1) + day) * double(MILLIS_PER_DAY));
		}
	}
	// and a dense sweep around every transition, where the implementations can actually differ
	for (const auto &transition : TimeZone::GetTransitions(name)) {
		AddProbes(probes, double(transition) * double(MILLIS_PER_SECOND));
		// the surrounding hours cover the local time ranges that are skipped or duplicated
		for (int32_t hours = -25; hours <= 25; hours++) {
			AddProbes(probes, double(transition) * double(MILLIS_PER_SECOND) + hours * double(MILLIS_PER_HOUR));
		}
	}

	for (const auto &probe : probes) {
		int32_t expected_raw = 0;
		int32_t expected_dst = 0;
		UErrorCode status = U_ZERO_ERROR;
		if (!probe.local) {
			expected->getOffset(probe.millis, false, expected_raw, expected_dst, status);
		} else {
			basic->getOffsetFromLocal(
			    probe.millis, probe.non_existing == LocalOption::FORMER ? UCAL_TZ_LOCAL_FORMER : UCAL_TZ_LOCAL_LATTER,
			    probe.duplicated == LocalOption::FORMER ? UCAL_TZ_LOCAL_FORMER : UCAL_TZ_LOCAL_LATTER, expected_raw,
			    expected_dst, status);
		}
		if (U_FAILURE(status)) {
			continue;
		}

		int32_t actual_raw = 0;
		int32_t actual_dst = 0;
		if (!probe.local) {
			actual->GetOffset(probe.millis, actual_raw, actual_dst);
		} else {
			actual->GetOffsetFromLocal(probe.millis, probe.non_existing, probe.duplicated, actual_raw, actual_dst);
		}

		if (expected_raw != actual_raw || expected_dst != actual_dst) {
			mismatches.push_back({name, probe.millis, probe.kind, expected_raw, expected_dst, actual_raw, actual_dst});
			if (mismatches.size() > 100) {
				return;
			}
		}
	}
}

unique_ptr<FunctionData> VerifyBind(ClientContext &context, TableFunctionBindInput &input,
                                    vector<LogicalType> &return_types, vector<string> &names) {
	names = {"zone", "millis", "kind", "expected_raw", "expected_dst", "actual_raw", "actual_dst"};
	return_types = {LogicalType::VARCHAR, LogicalType::DOUBLE,  LogicalType::VARCHAR, LogicalType::INTEGER,
	                LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER};
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> VerifyInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<VerifyData>();
	for (const auto &name : TimeZone::GetAvailableIds()) {
		Verify(name, result->mismatches);
		if (result->mismatches.size() > 100) {
			break;
		}
	}
	return std::move(result);
}

void VerifyFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<VerifyData>();
	const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, data.mismatches.size() - data.offset);
	for (idx_t i = 0; i < count; i++) {
		const auto &mismatch = data.mismatches[data.offset + i];
		output.SetValue(0, i, Value(mismatch.zone));
		output.SetValue(1, i, Value::DOUBLE(mismatch.millis));
		output.SetValue(2, i, Value(mismatch.kind));
		output.SetValue(3, i, Value::INTEGER(mismatch.expected_raw));
		output.SetValue(4, i, Value::INTEGER(mismatch.expected_dst));
		output.SetValue(5, i, Value::INTEGER(mismatch.actual_raw));
		output.SetValue(6, i, Value::INTEGER(mismatch.actual_dst));
	}
	data.offset += count;
	output.SetCardinality(count);
}

} // namespace

void RegisterTimeZoneVerifyFunction(ExtensionLoader &loader) {
	TableFunction verify("icu_timezone_verify", {}, VerifyFunction, VerifyBind, VerifyInit);
	loader.RegisterFunction(verify);
}

} // namespace datetime
} // namespace duckdb
