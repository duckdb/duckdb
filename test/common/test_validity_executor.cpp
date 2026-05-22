#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/validity_executor.hpp"

#include <array>

using namespace duckdb;

namespace {

static constexpr std::array<idx_t, 15> MIN_RUN_LENGTHS = {{idx_t(1), idx_t(2), idx_t(3), idx_t(4), idx_t(7), idx_t(8),
                                                           idx_t(15), idx_t(16), idx_t(17), idx_t(31), idx_t(32),
                                                           idx_t(63), idx_t(64), idx_t(65), idx_t(128)}};
static constexpr idx_t TEST_VALIDITY_ENTRY_COUNT = 8;

enum class EventKind : uint8_t { VALID_RANGE, INVALID_RANGE, VALIDITY_SLICE };

struct Event {
	EventKind kind;
	idx_t offset;
	idx_t count;
	validity_t bits;
};

struct TestContext {
	const char *name;
	idx_t min_run_length;
	idx_t source_offset;
	idx_t count;
	bool no_mask;
};

static void Fail(const TestContext &context, const string &message);

struct EventList {
	static constexpr idx_t MAX_EVENTS = ValidityMask::BITS_PER_VALUE * 8;

	std::array<Event, MAX_EVENTS> events;
	idx_t count = 0;

	void Add(const TestContext &context, EventKind kind, idx_t offset, idx_t row_count, validity_t bits = 0) {
		if (count >= events.size()) {
			Fail(context, "too many callback events");
		}
		events[count++] = Event {kind, offset, row_count, bits};
	}

	const Event &operator[](idx_t index) const {
		return events[index];
	}
};

static string ContextMessage(const TestContext &context, const string &message) {
	return StringUtil::Format("%s: %s (MIN=%llu, source_offset=%llu, count=%llu, no_mask=%s)", context.name,
	                          message.c_str(), context.min_run_length, context.source_offset, context.count,
	                          context.no_mask ? "true" : "false");
}

static void Fail(const TestContext &context, const string &message) {
	FAIL(ContextMessage(context, message));
}

static void Check(const TestContext &context, bool condition, const string &message) {
	if (!condition) {
		Fail(context, message);
	}
}

static validity_t ActiveMask(idx_t count) {
	D_ASSERT(count <= ValidityMask::BITS_PER_VALUE);
	if (count == ValidityMask::BITS_PER_VALUE) {
		return ~validity_t(0);
	}
	return (validity_t(1) << count) - 1;
}

static validity_t SingleBit(idx_t offset) {
	D_ASSERT(offset < ValidityMask::BITS_PER_VALUE);
	return validity_t(1) << offset;
}

static validity_t RunMask(idx_t offset, idx_t count) {
	D_ASSERT(offset + count <= ValidityMask::BITS_PER_VALUE);
	if (count == 0) {
		return 0;
	}
	return ActiveMask(count) << offset;
}

static bool RowIsValid(validity_t bits, idx_t offset) {
	return (bits & SingleBit(offset)) != 0;
}

static void SetRow(std::array<validity_t, TEST_VALIDITY_ENTRY_COUNT> &entries, idx_t row_idx, bool valid) {
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityMask::GetEntryIndex(row_idx, entry_idx, idx_in_entry);
	if (valid) {
		entries[entry_idx] |= SingleBit(idx_in_entry);
	} else {
		entries[entry_idx] &= ~SingleBit(idx_in_entry);
	}
}

struct RawValidityMask {
	template <class ROW_VALIDITY_FUNC>
	explicit RawValidityMask(idx_t capacity, idx_t source_offset, idx_t count, ROW_VALIDITY_FUNC row_is_valid)
	    : entries(), validity(entries.data(), MaxValue<idx_t>(capacity, 1)) {
		D_ASSERT(ValidityMask::EntryCount(MaxValue<idx_t>(capacity, 1)) <= entries.size());
		entries.fill(~validity_t(0));
		for (idx_t i = 0; i < count; i++) {
			SetRow(entries, source_offset + i, row_is_valid(i));
		}
	}

	std::array<validity_t, TEST_VALIDITY_ENTRY_COUNT> entries;
	ValidityMask validity;
};

static validity_t ReadLocalBits(const ValidityMask &validity, idx_t source_offset, idx_t local_offset, idx_t count) {
	validity_t result = 0;
	for (idx_t i = 0; i < count; i++) {
		if (validity.RowIsValid(source_offset + local_offset + i)) {
			result |= SingleBit(i);
		}
	}
	return result;
}

static idx_t MaxUniformRunLength(validity_t bits, idx_t count) {
	idx_t max_run = 0;
	idx_t offset = 0;
	while (offset < count) {
		const auto valid = RowIsValid(bits, offset);
		idx_t run_count = 1;
		while (offset + run_count < count && RowIsValid(bits, offset + run_count) == valid) {
			run_count++;
		}
		max_run = MaxValue(max_run, run_count);
		offset += run_count;
	}
	return max_run;
}

static idx_t UniformRunLength(validity_t bits, idx_t count, idx_t offset) {
	D_ASSERT(offset < count);
	const auto valid = RowIsValid(bits, offset);
	idx_t run_count = 1;
	while (offset + run_count < count && RowIsValid(bits, offset + run_count) == valid) {
		run_count++;
	}
	return run_count;
}

static void CheckValidityWordSlice(const TestContext &context, const ValidityWordSlice &word) {
	const auto count = word.Count();
	const auto bits = word.Bits();
	Check(context, count > 0, "validity slice has zero rows");
	Check(context, count <= ValidityMask::BITS_PER_VALUE, "validity slice is wider than one word");

	const auto active_mask = ActiveMask(count);
	const auto all_valid = bits == active_mask;
	const auto none_valid = bits == 0;

	Check(context, (bits & ~active_mask) == 0, "validity slice has inactive high bits set");
	Check(context, word.AllValid() == all_valid, "validity slice AllValid result is incorrect");
	Check(context, word.NoneValid() == none_valid, "validity slice NoneValid result is incorrect");
	Check(context, word.HasValid() == !none_valid, "validity slice HasValid result is incorrect");
	Check(context, word.HasInvalid() == !all_valid, "validity slice HasInvalid result is incorrect");

	for (idx_t i = 0; i < count; i++) {
		Check(context, word.RowIsValid(i) == RowIsValid(bits, i), "validity slice row validity is incorrect");
		Check(context, word.RunLength(i) == UniformRunLength(bits, count, i), "validity slice run length is incorrect");
	}
}

static const char *KindName(EventKind kind) {
	switch (kind) {
	case EventKind::VALID_RANGE:
		return "valid range";
	case EventKind::INVALID_RANGE:
		return "invalid range";
	case EventKind::VALIDITY_SLICE:
		return "validity slice";
	default:
		return "unknown";
	}
}

static bool HasRangeEvent(const EventList &events, EventKind kind, idx_t offset, idx_t count) {
	for (idx_t i = 0; i < events.count; i++) {
		const auto &event = events[i];
		if (event.kind == kind && event.offset == offset && event.count == count) {
			return true;
		}
	}
	return false;
}

template <idx_t MIN_RUN_LENGTH>
static idx_t CheckExpectedRangesInSegment(const TestContext &context, const EventList &events,
                                          const ValidityMask &validity, idx_t source_offset, idx_t segment_offset,
                                          idx_t segment_count) {
	idx_t expected_ranges = 0;
	idx_t offset = 0;
	while (offset < segment_count) {
		const auto valid = validity.RowIsValid(source_offset + segment_offset + offset);
		idx_t run_count = 1;
		while (offset + run_count < segment_count &&
		       validity.RowIsValid(source_offset + segment_offset + offset + run_count) == valid) {
			run_count++;
		}
		if (run_count >= MIN_RUN_LENGTH) {
			const auto kind = valid ? EventKind::VALID_RANGE : EventKind::INVALID_RANGE;
			const auto range_offset = segment_offset + offset;
			Check(context, HasRangeEvent(events, kind, range_offset, run_count),
			      StringUtil::Format("missing %s at offset %llu with count %llu", KindName(kind), range_offset,
			                         run_count));
			expected_ranges++;
		}
		offset += run_count;
	}
	return expected_ranges;
}

template <idx_t MIN_RUN_LENGTH>
static idx_t CheckExpectedRanges(const TestContext &context, const EventList &events, const ValidityMask &validity,
                                 idx_t source_offset, idx_t count) {
	if (count == 0) {
		return 0;
	}
	if (validity.CannotHaveNull()) {
		return CheckExpectedRangesInSegment<MIN_RUN_LENGTH>(context, events, validity, source_offset, 0, count);
	}

	idx_t expected_ranges = 0;
	idx_t local_offset = 0;
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityMask::GetEntryIndex(source_offset, entry_idx, idx_in_entry);

	if (idx_in_entry != 0) {
		const auto segment_count = MinValue<idx_t>(ValidityMask::BITS_PER_VALUE - idx_in_entry, count);
		expected_ranges += CheckExpectedRangesInSegment<MIN_RUN_LENGTH>(context, events, validity, source_offset,
		                                                                local_offset, segment_count);
		local_offset += segment_count;
	}

	while (count - local_offset >= ValidityMask::BITS_PER_VALUE) {
		expected_ranges += CheckExpectedRangesInSegment<MIN_RUN_LENGTH>(context, events, validity, source_offset,
		                                                                local_offset, ValidityMask::BITS_PER_VALUE);
		local_offset += ValidityMask::BITS_PER_VALUE;
	}

	if (local_offset < count) {
		expected_ranges += CheckExpectedRangesInSegment<MIN_RUN_LENGTH>(context, events, validity, source_offset,
		                                                                local_offset, count - local_offset);
	}
	return expected_ranges;
}

template <idx_t MIN_RUN_LENGTH>
static void CheckEvents(const TestContext &context, const EventList &events, const ValidityMask &validity,
                        idx_t source_offset, idx_t count) {
	idx_t cursor = 0;
	idx_t range_count = 0;
	for (idx_t event_idx = 0; event_idx < events.count; event_idx++) {
		const auto &event = events[event_idx];
		Check(context, event.count > 0, "callback event has zero rows");
		Check(context, event.offset == cursor,
		      StringUtil::Format("callback event starts at %llu, expected %llu", event.offset, cursor));
		Check(context, event.offset + event.count <= count, "callback event extends past requested count");

		if (event.kind == EventKind::VALID_RANGE || event.kind == EventKind::INVALID_RANGE) {
			const auto expected_valid = event.kind == EventKind::VALID_RANGE;
			Check(context, event.count >= MIN_RUN_LENGTH, "range callback is shorter than MIN_RUN_LENGTH");
			for (idx_t i = 0; i < event.count; i++) {
				Check(context, validity.RowIsValid(source_offset + event.offset + i) == expected_valid,
				      "range callback covers rows with the wrong validity");
			}
			range_count++;
		} else {
			Check(context, event.count <= ValidityMask::BITS_PER_VALUE, "validity slice is wider than one word");
			Check(context, event.bits == ReadLocalBits(validity, source_offset, event.offset, event.count),
			      "validity slice bits do not match source validity");
			Check(context, (event.bits & ~ActiveMask(event.count)) == 0, "validity slice has inactive high bits set");
			Check(context, MaxUniformRunLength(event.bits, event.count) < MIN_RUN_LENGTH,
			      "validity slice contains an extractable run");
			if (!validity.CannotHaveNull()) {
				const auto source_begin = source_offset + event.offset;
				const auto source_end = source_begin + event.count - 1;
				Check(context, source_begin / ValidityMask::BITS_PER_VALUE == source_end / ValidityMask::BITS_PER_VALUE,
				      "validity slice crosses a source validity word boundary");
			}
		}

		cursor += event.count;
	}

	Check(context, cursor == count, "callback events do not cover the requested count");
	const auto expected_ranges = CheckExpectedRanges<MIN_RUN_LENGTH>(context, events, validity, source_offset, count);
	Check(context, range_count == expected_ranges,
	      StringUtil::Format("got %llu range callbacks, expected %llu", range_count, expected_ranges));
}

template <idx_t MIN_RUN_LENGTH>
static void CheckValidityExecutorCase(const TestContext &context, const ValidityMask &validity) {
	EventList events;
	auto valid_func = [&](idx_t offset, idx_t count) {
		events.Add(context, EventKind::VALID_RANGE, offset, count);
	};
	auto invalid_func = [&](idx_t offset, idx_t count) {
		events.Add(context, EventKind::INVALID_RANGE, offset, count);
	};
	auto validity_slice_func = [&](idx_t offset, ValidityWordSlice word) {
		CheckValidityWordSlice(context, word);
		events.Add(context, EventKind::VALIDITY_SLICE, offset, word.Count(), word.Bits());
	};

	ValidityExecutor::Execute<MIN_RUN_LENGTH>(validity, context.source_offset, context.count, valid_func, invalid_func,
	                                          validity_slice_func);
	CheckEvents<MIN_RUN_LENGTH>(context, events, validity, context.source_offset, context.count);
}

template <idx_t MIN_RUN_LENGTH>
static void CheckAllocatedMaskCase(const char *name, idx_t source_offset, idx_t count, validity_t bits) {
	D_ASSERT(count <= ValidityMask::BITS_PER_VALUE);
	TestContext context {name, MIN_RUN_LENGTH, source_offset, count, false};
	RawValidityMask raw_mask(source_offset + count, source_offset, count,
	                         [bits](idx_t i) { return RowIsValid(bits, i); });
	CheckValidityExecutorCase<MIN_RUN_LENGTH>(context, raw_mask.validity);
}

template <idx_t MIN_RUN_LENGTH, class ROW_VALIDITY_FUNC>
static void CheckAllocatedMaskCase(const char *name, idx_t source_offset, idx_t count, ROW_VALIDITY_FUNC row_is_valid) {
	TestContext context {name, MIN_RUN_LENGTH, source_offset, count, false};
	RawValidityMask raw_mask(source_offset + count, source_offset, count, row_is_valid);
	CheckValidityExecutorCase<MIN_RUN_LENGTH>(context, raw_mask.validity);
}

template <idx_t MIN_RUN_LENGTH>
static void CheckNoMaskCase(const char *name, idx_t source_offset, idx_t count) {
	TestContext context {name, MIN_RUN_LENGTH, source_offset, count, true};
	ValidityMask validity(MaxValue<idx_t>(source_offset + count, 1));
	CheckValidityExecutorCase<MIN_RUN_LENGTH>(context, validity);
}

template <class ROW_VALIDITY_FUNC>
static void CheckAllocatedMaskCase(idx_t min_run_length, const char *name, idx_t source_offset, idx_t count,
                                   ROW_VALIDITY_FUNC row_is_valid) {
	switch (min_run_length) {
	case 1:
		CheckAllocatedMaskCase<1>(name, source_offset, count, row_is_valid);
		break;
	case 2:
		CheckAllocatedMaskCase<2>(name, source_offset, count, row_is_valid);
		break;
	case 3:
		CheckAllocatedMaskCase<3>(name, source_offset, count, row_is_valid);
		break;
	case 4:
		CheckAllocatedMaskCase<4>(name, source_offset, count, row_is_valid);
		break;
	case 7:
		CheckAllocatedMaskCase<7>(name, source_offset, count, row_is_valid);
		break;
	case 8:
		CheckAllocatedMaskCase<8>(name, source_offset, count, row_is_valid);
		break;
	case 15:
		CheckAllocatedMaskCase<15>(name, source_offset, count, row_is_valid);
		break;
	case 16:
		CheckAllocatedMaskCase<16>(name, source_offset, count, row_is_valid);
		break;
	case 17:
		CheckAllocatedMaskCase<17>(name, source_offset, count, row_is_valid);
		break;
	case 31:
		CheckAllocatedMaskCase<31>(name, source_offset, count, row_is_valid);
		break;
	case 32:
		CheckAllocatedMaskCase<32>(name, source_offset, count, row_is_valid);
		break;
	case 63:
		CheckAllocatedMaskCase<63>(name, source_offset, count, row_is_valid);
		break;
	case 64:
		CheckAllocatedMaskCase<64>(name, source_offset, count, row_is_valid);
		break;
	case 65:
		CheckAllocatedMaskCase<65>(name, source_offset, count, row_is_valid);
		break;
	case 128:
		CheckAllocatedMaskCase<128>(name, source_offset, count, row_is_valid);
		break;
	default:
		FAIL(StringUtil::Format("unhandled MIN_RUN_LENGTH %llu", min_run_length));
	}
}

static void CheckAllocatedMaskCase(idx_t min_run_length, const char *name, idx_t source_offset, idx_t count,
                                   validity_t bits) {
	CheckAllocatedMaskCase(min_run_length, name, source_offset, count, [bits](idx_t i) { return RowIsValid(bits, i); });
}

static void CheckNoMaskCase(idx_t min_run_length, const char *name, idx_t source_offset, idx_t count) {
	switch (min_run_length) {
	case 1:
		CheckNoMaskCase<1>(name, source_offset, count);
		break;
	case 2:
		CheckNoMaskCase<2>(name, source_offset, count);
		break;
	case 3:
		CheckNoMaskCase<3>(name, source_offset, count);
		break;
	case 4:
		CheckNoMaskCase<4>(name, source_offset, count);
		break;
	case 7:
		CheckNoMaskCase<7>(name, source_offset, count);
		break;
	case 8:
		CheckNoMaskCase<8>(name, source_offset, count);
		break;
	case 15:
		CheckNoMaskCase<15>(name, source_offset, count);
		break;
	case 16:
		CheckNoMaskCase<16>(name, source_offset, count);
		break;
	case 17:
		CheckNoMaskCase<17>(name, source_offset, count);
		break;
	case 31:
		CheckNoMaskCase<31>(name, source_offset, count);
		break;
	case 32:
		CheckNoMaskCase<32>(name, source_offset, count);
		break;
	case 63:
		CheckNoMaskCase<63>(name, source_offset, count);
		break;
	case 64:
		CheckNoMaskCase<64>(name, source_offset, count);
		break;
	case 65:
		CheckNoMaskCase<65>(name, source_offset, count);
		break;
	case 128:
		CheckNoMaskCase<128>(name, source_offset, count);
		break;
	default:
		FAIL(StringUtil::Format("unhandled MIN_RUN_LENGTH %llu", min_run_length));
	}
}

static idx_t CheckAllMinValuesAllocated(const char *name, idx_t source_offset, idx_t count, validity_t bits) {
	for (const auto min_run_length : MIN_RUN_LENGTHS) {
		CheckAllocatedMaskCase(min_run_length, name, source_offset, count, bits);
	}
	return MIN_RUN_LENGTHS.size();
}

template <class ROW_VALIDITY_FUNC>
static idx_t CheckAllMinValuesAllocatedRows(const char *name, idx_t source_offset, idx_t count,
                                            ROW_VALIDITY_FUNC row_is_valid) {
	for (const auto min_run_length : MIN_RUN_LENGTHS) {
		CheckAllocatedMaskCase(min_run_length, name, source_offset, count, row_is_valid);
	}
	return MIN_RUN_LENGTHS.size();
}

static idx_t CheckAllMinValuesNoMask(const char *name, idx_t source_offset, idx_t count) {
	for (const auto min_run_length : MIN_RUN_LENGTHS) {
		CheckNoMaskCase(min_run_length, name, source_offset, count);
	}
	return MIN_RUN_LENGTHS.size();
}

} // namespace

TEST_CASE("ValidityExecutor exhaustive small masks", "[validity_executor]") {
	idx_t checked_cases = 0;
	for (const auto source_offset : {idx_t(0), idx_t(63)}) {
		for (idx_t count = 0; count <= 16; count++) {
			const auto pattern_count = validity_t(1) << count;
			for (validity_t bits = 0; bits < pattern_count; bits++) {
				checked_cases +=
				    CheckAllMinValuesAllocated("exhaustive small allocated mask", source_offset, count, bits);
			}
			checked_cases += CheckAllMinValuesNoMask("exhaustive small no-mask all-valid", source_offset, count);
		}
	}
	REQUIRE(checked_cases > 0);
}

TEST_CASE("ValidityExecutor 64-bit masks", "[validity_executor]") {
	const auto all_valid = ActiveMask(ValidityMask::BITS_PER_VALUE);
	const auto alternating = validity_t(0xAAAAAAAAAAAAAAAA);
	const auto inverse_alternating = ~alternating;

	idx_t checked_cases = 0;
	for (const auto source_offset : {idx_t(0), idx_t(1), idx_t(63), idx_t(64), idx_t(65)}) {
		checked_cases += CheckAllMinValuesAllocated("64-bit all valid allocated", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, all_valid);
		checked_cases +=
		    CheckAllMinValuesNoMask("64-bit all valid no-mask", source_offset, ValidityMask::BITS_PER_VALUE);
		checked_cases +=
		    CheckAllMinValuesAllocated("64-bit all invalid", source_offset, ValidityMask::BITS_PER_VALUE, 0);
		checked_cases +=
		    CheckAllMinValuesAllocated("64-bit alternating", source_offset, ValidityMask::BITS_PER_VALUE, alternating);
		checked_cases += CheckAllMinValuesAllocated("64-bit inverse alternating", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, inverse_alternating);
		checked_cases += CheckAllMinValuesAllocated("64-bit single invalid first", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, all_valid & ~SingleBit(0));
		checked_cases += CheckAllMinValuesAllocated("64-bit single invalid last", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, all_valid & ~SingleBit(63));
		checked_cases += CheckAllMinValuesAllocated("64-bit single valid first", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, SingleBit(0));
		checked_cases += CheckAllMinValuesAllocated("64-bit single valid last", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, SingleBit(63));
		checked_cases += CheckAllMinValuesAllocated("64-bit valid run starts first", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, RunMask(0, 8));
		checked_cases += CheckAllMinValuesAllocated("64-bit valid run ends last", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, RunMask(56, 8));
		checked_cases += CheckAllMinValuesAllocated("64-bit invalid run starts first", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, all_valid & ~RunMask(0, 8));
		checked_cases += CheckAllMinValuesAllocated("64-bit invalid run ends last", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, all_valid & ~RunMask(56, 8));
		checked_cases += CheckAllMinValuesAllocated("64-bit two valid runs", source_offset,
		                                            ValidityMask::BITS_PER_VALUE, RunMask(4, 8) | RunMask(24, 9));
		checked_cases +=
		    CheckAllMinValuesAllocated("64-bit two invalid runs", source_offset, ValidityMask::BITS_PER_VALUE,
		                               all_valid & ~(RunMask(4, 8) | RunMask(24, 9)));
	}
	REQUIRE(checked_cases > 0);
}

TEST_CASE("ValidityExecutor multi-entry masks", "[validity_executor]") {
	idx_t checked_cases = 0;

	for (const auto source_offset : {idx_t(0), idx_t(1), idx_t(63)}) {
		checked_cases += CheckAllMinValuesAllocatedRows("multi-entry all valid allocated", source_offset, 130,
		                                                [](idx_t) { return true; });
		checked_cases += CheckAllMinValuesAllocatedRows("multi-entry all invalid allocated", source_offset, 130,
		                                                [](idx_t) { return false; });
		checked_cases += CheckAllMinValuesAllocatedRows("multi-entry alternating allocated", source_offset, 130,
		                                                [](idx_t i) { return (i & 1) != 0; });
	}

	for (const auto count : {idx_t(128), idx_t(192)}) {
		checked_cases += CheckAllMinValuesAllocatedRows("multi-entry aligned all valid allocated", 0, count,
		                                                [](idx_t) { return true; });
		checked_cases += CheckAllMinValuesAllocatedRows("multi-entry aligned all invalid allocated", 0, count,
		                                                [](idx_t) { return false; });
	}

	checked_cases += CheckAllMinValuesAllocatedRows("multi-entry clustered runs allocated", 1, 130, [](idx_t i) {
		return (i < 17) || (i >= 40 && i < 104) || (i >= 121);
	});
	CheckNoMaskCase<128>("multi-entry no-mask all-valid short of MIN", 0, 96);
	checked_cases++;

	REQUIRE(checked_cases > 0);
}

TEST_CASE("ValidityExecutor minimum run lengths", "[validity_executor]") {
	idx_t checked_cases = 0;
	for (const auto min_run_length : MIN_RUN_LENGTHS) {
		for (const auto source_offset : {idx_t(0), idx_t(1), idx_t(63), idx_t(64), idx_t(65)}) {
			if (min_run_length > 1) {
				const auto short_count = MinValue<idx_t>(min_run_length - 1, ValidityMask::BITS_PER_VALUE);
				CheckAllocatedMaskCase(min_run_length, "64-bit valid run shorter than MIN", source_offset,
				                       ValidityMask::BITS_PER_VALUE, RunMask(0, short_count));
				CheckAllocatedMaskCase(min_run_length, "64-bit invalid run shorter than MIN", source_offset,
				                       ValidityMask::BITS_PER_VALUE, ActiveMask(64) & ~RunMask(0, short_count));
				checked_cases += 2;
			}

			if (min_run_length <= ValidityMask::BITS_PER_VALUE) {
				CheckAllocatedMaskCase(min_run_length, "64-bit valid run exactly MIN", source_offset,
				                       ValidityMask::BITS_PER_VALUE, RunMask(0, min_run_length));
				CheckAllocatedMaskCase(min_run_length, "64-bit invalid run exactly MIN", source_offset,
				                       ValidityMask::BITS_PER_VALUE, ActiveMask(64) & ~RunMask(0, min_run_length));
				checked_cases += 2;
			}

			if (min_run_length < ValidityMask::BITS_PER_VALUE) {
				const auto long_count = min_run_length + 1;
				CheckAllocatedMaskCase(min_run_length, "64-bit valid run longer than MIN", source_offset,
				                       ValidityMask::BITS_PER_VALUE, RunMask(0, long_count));
				CheckAllocatedMaskCase(min_run_length, "64-bit invalid run longer than MIN", source_offset,
				                       ValidityMask::BITS_PER_VALUE, ActiveMask(64) & ~RunMask(0, long_count));
				checked_cases += 2;
			}
		}
	}
	REQUIRE(checked_cases > 0);
}
