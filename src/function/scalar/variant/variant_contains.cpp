#include "duckdb/common/radix.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/types/variant_comparison.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Comparing primitive values
//===--------------------------------------------------------------------===//

//! The value that is looked for, with its type. The key a NUMBER is compared by is built once
struct VariantNeedle {
	explicit VariantNeedle(const VariantNode &node_p)
	    : node(node_p), type_id(node.GetTypeId()), category(GetVariantComparisonType(type_id)) {
	}

	const VariantNumberKey &NumberKey() {
		if (!number_key) {
			number_key = VariantGetNumberKey(type_id, node);
		}
		return *number_key;
	}

	VariantNode node;
	LogicalTypeId type_id;
	VariantComparisonType category;
	optional<VariantNumberKey> number_key;
};

//! Whether two primitive values of the same comparison category are equal
bool IsPrimitiveEqual(const VariantNode &haystack, LogicalTypeId haystack_type, VariantNeedle &needle) {
	switch (needle.category) {
	case VariantComparisonType::NULL_VALUE:
		return true;
	case VariantComparisonType::BOOLEAN:
		return haystack.GetBool() == needle.node.GetBool();
	case VariantComparisonType::UUID:
		return haystack.GetData<hugeint_t>() == needle.node.GetData<hugeint_t>();
	case VariantComparisonType::REAL:
		// we need to encode so NaN == NaN, -0.0 == 0.0, and infinity == infinity
		return Radix::EncodeDouble(VariantGetRealValue(haystack_type, haystack)) ==
		       Radix::EncodeDouble(VariantGetRealValue(needle.type_id, needle.node));
	case VariantComparisonType::NUMBER:
		return VariantGetNumberKey(haystack_type, haystack) == needle.NumberKey();
	case VariantComparisonType::TIME:
		return VariantGetTimeValue(haystack_type, haystack) == VariantGetTimeValue(needle.type_id, needle.node);
	case VariantComparisonType::TIME_TZ:
		return haystack.GetData<dtime_tz_t>() == needle.node.GetData<dtime_tz_t>();
	case VariantComparisonType::TIMESTAMP:
		return VariantGetTimestampValue(haystack_type, haystack) ==
		       VariantGetTimestampValue(needle.type_id, needle.node);
	case VariantComparisonType::TIMESTAMP_TZ:
		return VariantGetTimestampTZValue(haystack_type, haystack) ==
		       VariantGetTimestampTZValue(needle.type_id, needle.node);
	case VariantComparisonType::INTERVAL:
		return haystack.GetData<interval_t>() == needle.node.GetData<interval_t>();
	case VariantComparisonType::BLOB:
	case VariantComparisonType::BITSTRING:
	case VariantComparisonType::GEOMETRY:
	case VariantComparisonType::GEOGRAPHY:
	case VariantComparisonType::VARCHAR:
		return haystack.GetString() == needle.node.GetString();
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// Matching arrays and objects
//===--------------------------------------------------------------------===//
// A haystack array or object matches a needle one when every child of the needle is matched by a child of the
// haystack. Haystack children are not consumed, so one can match several needle children.

//! The pairs of elements to compare for two arrays: every haystack element is a candidate for a needle element
class VariantArrayMatch {
public:
	VariantArrayMatch(const VariantNode &haystack, const VariantNode &needle)
	    : haystack_elements(haystack.GetArrayChildren()), needle_elements(needle.GetArrayChildren()) {
	}

	//! Returns false when all needle elements are matched, or the current one has no candidate left
	bool NextPair(VariantNode &haystack_child, VariantNode &needle_child) const {
		if (AllMatched() || candidate_index == haystack_elements.size()) {
			return false;
		}
		haystack_child = haystack_elements[candidate_index];
		needle_child = needle_elements[needle_index];
		return true;
	}

	void PairCompared(bool matched) {
		if (!matched) {
			candidate_index++;
			return;
		}
		needle_index++;
		candidate_index = 0;
	}

	bool AllMatched() const {
		return needle_index == needle_elements.size();
	}

private:
	VariantArrayIterator haystack_elements;
	VariantArrayIterator needle_elements;
	idx_t needle_index = 0;
	idx_t candidate_index = 0;
};

//! The pairs of values to compare for two objects: only a haystack entry with the same key is a candidate for a
//! needle entry. Entries are taken by raw position, skipping the fields that are missing in the row
class VariantObjectMatch {
public:
	VariantObjectMatch(const VariantNode &haystack, const VariantNode &needle)
	    : haystack_entries(haystack, VariantIterationOrder::INTERNAL),
	      needle_entries(needle, VariantIterationOrder::INTERNAL) {
	}

	//! Returns false when all needle entries are matched, or the current one has no candidate left
	bool NextPair(VariantNode &haystack_child, VariantNode &needle_child) {
		VariantObjectEntry needle_entry;
		if (!NextNeedleEntry(needle_entry)) {
			return false;
		}
		if (!NextCandidate(needle_entry.key, haystack_child)) {
			return false;
		}
		needle_child = needle_entry.value;
		return true;
	}

	void PairCompared(bool matched) {
		if (!matched) {
			candidates_tried++;
			return;
		}
		// objects often hold their keys in the same order, so the next key is looked for after this match
		const auto haystack_count = haystack_entries.GetRawCount();
		first_candidate = CandidatePosition(haystack_count) + 1;
		if (first_candidate == haystack_count) {
			first_candidate = 0;
		}
		candidates_tried = 0;
		needle_index++;
	}

	//! Only holds once NextPair has returned false, as that skips the missing needle fields
	bool AllMatched() const {
		return needle_index == needle_entries.GetRawCount();
	}

private:
	bool NextNeedleEntry(VariantObjectEntry &needle_entry) {
		for (; needle_index < needle_entries.GetRawCount(); needle_index++) {
			needle_entry = needle_entries.RawEntry(needle_index);
			if (!needle_entry.value.IsMissing()) {
				return true;
			}
		}
		return false;
	}

	//! Finds the next haystack entry with the key
	bool NextCandidate(const string_t &key, VariantNode &haystack_child) {
		const auto haystack_count = haystack_entries.GetRawCount();
		for (; candidates_tried < haystack_count; candidates_tried++) {
			const auto haystack_entry = haystack_entries.RawEntry(CandidatePosition(haystack_count));
			if (haystack_entry.value.IsMissing() || haystack_entry.key != key) {
				continue;
			}
			haystack_child = haystack_entry.value;
			return true;
		}
		return false;
	}

	//! Candidates are tried at a growing distance from the first one, alternately after and before it and wrapping
	//! around, so that every haystack entry is tried once
	idx_t CandidatePosition(idx_t haystack_count) const {
		const auto distance = (candidates_tried + 1) / 2;
		const auto after = candidates_tried % 2 == 0;
		if (after) {
			const auto position = first_candidate + distance;
			return position < haystack_count ? position : position - haystack_count;
		}
		return first_candidate >= distance ? first_candidate - distance : first_candidate + haystack_count - distance;
	}

private:
	VariantObjectIterator haystack_entries;
	VariantObjectIterator needle_entries;
	idx_t needle_index = 0;
	//! The raw position of the first haystack entry tried for the needle entry, and how many are tried
	idx_t first_candidate = 0;
	idx_t candidates_tried = 0;
};

//! Proxy frame for two arrays or two objects whose children are being compared
class VariantMatchFrame {
public:
	VariantMatchFrame(const VariantNode &haystack, const VariantNode &needle, VariantComparisonType category) {
		if (category == VariantComparisonType::ARRAY) {
			arrays.emplace(haystack, needle);
		} else {
			objects.emplace(haystack, needle);
		}
	}

	bool NextPair(VariantNode &haystack_child, VariantNode &needle_child) {
		if (arrays) {
			return arrays->NextPair(haystack_child, needle_child);
		}
		return Objects().NextPair(haystack_child, needle_child);
	}

	//! Moves on to the next needle child if the pair matched, and to its next candidate otherwise
	void PairCompared(bool matched) {
		if (arrays) {
			arrays->PairCompared(matched);
			return;
		}
		Objects().PairCompared(matched);
	}

	bool AllMatched() const {
		return arrays ? arrays->AllMatched() : Objects().AllMatched();
	}

private:
	VariantObjectMatch &Objects() {
		if (!objects) {
			throw InternalException("VariantMatchFrame holds neither an array nor an object");
		}
		return *objects;
	}
	const VariantObjectMatch &Objects() const {
		if (!objects) {
			throw InternalException("VariantMatchFrame holds neither an array nor an object");
		}
		return *objects;
	}

private:
	optional<VariantArrayMatch> arrays;
	optional<VariantObjectMatch> objects;
};

bool IsArrayOrObject(VariantComparisonType category) {
	return category == VariantComparisonType::ARRAY || category == VariantComparisonType::OBJECT;
}

//! Whether the haystack array or object matches the needle one.
bool ChildrenMatch(const VariantNode &haystack, const VariantNode &needle, VariantComparisonType category,
                   vector<VariantMatchFrame> &frames) {
	frames.clear();
	frames.emplace_back(haystack, needle, category);

	while (true) {
		auto &frame = frames.back();
		VariantNode haystack_child;
		VariantNode needle_child_node;

		if (!frame.NextPair(haystack_child, needle_child_node)) {
			// the frame is finished, which decides the pair that it was opened for
			const auto matched = frame.AllMatched();
			frames.pop_back();
			if (frames.empty()) {
				return matched;
			}
			frames.back().PairCompared(matched);
			continue;
		}

		VariantNeedle needle_child(needle_child_node);
		const auto haystack_child_type = haystack_child.GetTypeId();
		const auto child_category = GetVariantComparisonType(haystack_child_type);

		if (child_category != needle_child.category) {
			frame.PairCompared(false);
			continue;
		}
		if (IsArrayOrObject(child_category)) {
			// the frame is not used after this, as opening another may move it
			frames.emplace_back(haystack_child, needle_child_node, child_category);
			continue;
		}

		frame.PairCompared(IsPrimitiveEqual(haystack_child, haystack_child_type, needle_child));
	}
}

//! Whether the haystack value itself matches the needle
bool MatchesNeedle(const VariantNode &haystack, LogicalTypeId haystack_type, VariantNeedle &needle,
                   vector<VariantMatchFrame> &frames) {
	const auto category = GetVariantComparisonType(haystack_type);
	if (category != needle.category) {
		return false;
	}
	if (IsArrayOrObject(category)) {
		return ChildrenMatch(haystack, needle.node, category, frames);
	}
	return IsPrimitiveEqual(haystack, haystack_type, needle);
}

//===--------------------------------------------------------------------===//
// Searching the haystack
//===--------------------------------------------------------------------===//

//! An array or object of the haystack, with the child to visit next
class VariantWalkFrame {
public:
	VariantWalkFrame(const VariantNode &container, LogicalTypeId type_id) {
		if (type_id == LogicalTypeId::LIST) {
			elements.emplace(container.GetArrayChildren());
		} else {
			entries.emplace(container, VariantIterationOrder::INTERNAL);
		}
	}

	bool NextChild(VariantNode &child) {
		if (elements) {
			return NextElement(*elements, child);
		}
		if (!entries) {
			throw InternalException("VariantWalkFrame holds neither an array nor an object");
		}
		return NextEntryValue(*entries, child);
	}

private:
	bool NextElement(const VariantArrayIterator &array, VariantNode &child) {
		if (next_index == array.size()) {
			return false;
		}
		child = array[next_index++];
		return true;
	}

	bool NextEntryValue(const VariantObjectIterator &object, VariantNode &child) {
		while (next_index < object.GetRawCount()) {
			child = object.RawEntry(next_index++).value;
			// Shredded values may have missing object keys for a given row
			if (!child.IsMissing()) {
				return true;
			}
		}
		return false;
	}

private:
	optional<VariantArrayIterator> elements;
	optional<VariantObjectIterator> entries;
	idx_t next_index = 0;
};

//! Visits the haystack value and then every value nested in it, depth first.
class VariantHaystackWalk {
public:
	VariantHaystackWalk(const VariantNode &haystack, vector<VariantWalkFrame> &frames)
	    : frames(frames), value(haystack), type_id(haystack.GetTypeId()) {
		frames.clear();
	}

	//! Moves to the next value, which is the first child of the current one if it has any
	bool Next() {
		if (type_id == LogicalTypeId::LIST || type_id == LogicalTypeId::STRUCT) {
			frames.emplace_back(value, type_id);
		}
		while (!frames.empty()) {
			if (frames.back().NextChild(value)) {
				type_id = value.GetTypeId();
				return true;
			}
			frames.pop_back();
		}
		return false;
	}

	const VariantNode &Value() const {
		return value;
	}
	LogicalTypeId Type() const {
		return type_id;
	}

private:
	vector<VariantWalkFrame> &frames;
	VariantNode value;
	LogicalTypeId type_id;
};

//! The stacks of a given needle-haystack search
struct VariantContainsState {
	vector<VariantMatchFrame> match_frames;
	vector<VariantWalkFrame> walk_frames;
};

//! Whether the haystack value or any value nested in it matches the needle
bool HaystackContains(const VariantNode &haystack, const VariantNode &needle_node, VariantContainsState &state) {
	VariantNeedle needle(needle_node);
	VariantHaystackWalk walk(haystack, state.walk_frames);

	do {
		if (MatchesNeedle(walk.Value(), walk.Type(), needle, state.match_frames)) {
			return true;
		}
	} while (walk.Next());

	return false;
}

} // namespace

static void VariantContainsFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 2);
	(void)state;

	const auto count = input.size();
	const VectorIterator<VectorVariantType> haystacks(input.data[0]);
	const VectorIterator<VectorVariantType> needles(input.data[1]);

	result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
	auto result_writer = FlatVector::Writer<bool>(result, count);

	VariantContainsState search_state;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!haystacks.RowIsValid(row_idx) || !needles.RowIsValid(row_idx)) {
			result_writer.WriteNull();
			continue;
		}
		result_writer.WriteValue(HaystackContains(haystacks[row_idx], needles[row_idx], search_state));
	}
}

ScalarFunctionSet VariantContainsFun::GetFunctions() {
	ScalarFunction function("variant_contains",
	                        {{"variant_haystack", LogicalType::VARIANT()}, {"variant_needle", LogicalType::VARIANT()}},
	                        LogicalType::BOOLEAN, VariantContainsFunction);
	return ScalarFunctionSet(function);
}

} // namespace duckdb
