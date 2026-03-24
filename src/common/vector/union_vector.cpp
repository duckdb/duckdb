#include "duckdb/common/vector/union_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

namespace duckdb {

const Vector &UnionVector::GetMember(const Vector &vector, idx_t member_index) {
	D_ASSERT(member_index < UnionType::GetMemberCount(vector.GetType()));
	auto &entries = StructVector::GetEntries(vector);
	return entries[member_index + 1]; // skip the "tag" entry
}

Vector &UnionVector::GetMember(Vector &vector, idx_t member_index) {
	D_ASSERT(member_index < UnionType::GetMemberCount(vector.GetType()));
	auto &entries = StructVector::GetEntries(vector);
	return entries[member_index + 1]; // skip the "tag" entry
}

const Vector &UnionVector::GetTags(const Vector &vector) {
	// the tag vector is always the first struct child.
	return StructVector::GetEntries(vector)[0];
}

Vector &UnionVector::GetTags(Vector &vector) {
	// the tag vector is always the first struct child.
	return StructVector::GetEntries(vector)[0];
}

void UnionVector::SetToMember(Vector &union_vector, union_tag_t tag, Vector &member_vector, idx_t count,
                              bool keep_tags_for_null) {
	D_ASSERT(union_vector.GetType().id() == LogicalTypeId::UNION);
	D_ASSERT(tag < UnionType::GetMemberCount(union_vector.GetType()));

	// Set the union member to the specified vector
	UnionVector::GetMember(union_vector, tag).Reference(member_vector);
	auto &tag_vector = UnionVector::GetTags(union_vector);

	if (member_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// if the member vector is constant, we can set the union to constant as well
		union_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<union_tag_t>(tag_vector)[0] = tag;
		if (keep_tags_for_null) {
			ConstantVector::SetNull(union_vector, false);
			ConstantVector::SetNull(tag_vector, false);
		} else {
			ConstantVector::SetNull(union_vector, ConstantVector::IsNull(member_vector));
			ConstantVector::SetNull(tag_vector, ConstantVector::IsNull(member_vector));
		}

	} else {
		// otherwise flatten and set to flatvector
		member_vector.Flatten(count);
		union_vector.SetVectorType(VectorType::FLAT_VECTOR);

		if (member_vector.validity.AllValid()) {
			// if the member vector is all valid, we can set the tag to constant
			tag_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			auto tag_data = ConstantVector::GetData<union_tag_t>(tag_vector);
			*tag_data = tag;
		} else {
			tag_vector.SetVectorType(VectorType::FLAT_VECTOR);
			if (keep_tags_for_null) {
				FlatVector::Validity(tag_vector).SetAllValid(count);
				FlatVector::Validity(union_vector).SetAllValid(count);
			} else {
				// ensure the tags have the same validity as the member
				FlatVector::Validity(union_vector) = FlatVector::Validity(member_vector);
				FlatVector::Validity(tag_vector) = FlatVector::Validity(member_vector);
			}

			auto tag_data = FlatVector::GetData<union_tag_t>(tag_vector);
			memset(tag_data, tag, count);
		}
	}

	// Set the non-selected members to constant null vectors
	for (idx_t i = 0; i < UnionType::GetMemberCount(union_vector.GetType()); i++) {
		if (i != tag) {
			auto &member = UnionVector::GetMember(union_vector, i);
			member.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(member, true);
		}
	}
}

bool UnionVector::TryGetTag(const Vector &vector, idx_t index, union_tag_t &result) {
	// the tag vector is always the first struct child.
	auto &tag_vector = StructVector::GetEntries(vector)[0];
	if (tag_vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(tag_vector);
		auto &dict_sel = DictionaryVector::SelVector(tag_vector);
		auto mapped_idx = dict_sel.get_index(index);
		if (FlatVector::IsNull(child, mapped_idx)) {
			return false;
		} else {
			result = FlatVector::GetData<union_tag_t>(child)[mapped_idx];
			return true;
		}
	}
	if (tag_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(tag_vector)) {
			return false;
		} else {
			result = ConstantVector::GetData<union_tag_t>(tag_vector)[0];
			return true;
		}
	}
	if (FlatVector::IsNull(tag_vector, index)) {
		return false;
	} else {
		result = FlatVector::GetData<union_tag_t>(tag_vector)[index];
		return true;
	}
}

//! Raw selection vector passed in (not merged with any other selection vectors)
UnionInvalidReason UnionVector::CheckUnionValidity(Vector &vector_p, idx_t count, const SelectionVector &sel_p) {
	D_ASSERT(vector_p.GetType().id() == LogicalTypeId::UNION);

	// Will contain the (possibly) merged selection vector
	const SelectionVector *sel = &sel_p;
	SelectionVector owned_sel;
	Vector *vector = &vector_p;
	if (vector->GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// In the case of a dictionary vector, unwrap the Vector, and merge the selection vectors.
		auto &child = DictionaryVector::Child(*vector);
		D_ASSERT(child.GetVectorType() != VectorType::DICTIONARY_VECTOR);
		auto &dict_sel = DictionaryVector::SelVector(*vector);
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(*sel, count);
		owned_sel.Initialize(new_buffer);
		sel = &owned_sel;
		vector = &child;
	} else if (vector->GetVectorType() == VectorType::CONSTANT_VECTOR) {
		sel = ConstantVector::ZeroSelectionVector(count, owned_sel);
	}

	auto member_count = UnionType::GetMemberCount(vector_p.GetType());
	if (member_count == 0) {
		return UnionInvalidReason::NO_MEMBERS;
	}

	UnifiedVectorFormat vector_vdata;
	vector_p.ToUnifiedFormat(count, vector_vdata);

	auto &entries = StructVector::GetEntries(vector_p);
	duckdb::vector<UnifiedVectorFormat> child_vdata(entries.size());
	for (idx_t entry_idx = 0; entry_idx < entries.size(); entry_idx++) {
		auto &child = entries[entry_idx];
		child.ToUnifiedFormat(count, child_vdata[entry_idx]);
	}

	auto &tag_vdata = child_vdata[0];

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto mapped_idx = sel->get_index(row_idx);

		if (!vector_vdata.validity.RowIsValid(mapped_idx)) {
			continue;
		}

		auto tag_idx = tag_vdata.sel->get_index(sel_p.get_index(row_idx));
		if (!tag_vdata.validity.RowIsValid(tag_idx)) {
			// we can't have NULL tags!
			return UnionInvalidReason::NULL_TAG;
		}
		auto tag = UnifiedVectorFormat::GetData<union_tag_t>(tag_vdata)[tag_idx];
		if (tag >= member_count) {
			return UnionInvalidReason::TAG_OUT_OF_RANGE;
		}

		bool found_valid = false;
		for (idx_t i = 0; i < member_count; i++) {
			auto &member_vdata = child_vdata[1 + i]; // skip the tag
			idx_t member_idx = member_vdata.sel->get_index(sel_p.get_index(row_idx));
			if (!member_vdata.validity.RowIsValid(member_idx)) {
				continue;
			}
			if (found_valid) {
				return UnionInvalidReason::VALIDITY_OVERLAP;
			}
			found_valid = true;
			if (tag != static_cast<union_tag_t>(i)) {
				return UnionInvalidReason::TAG_MISMATCH;
			}
		}
	}

	return UnionInvalidReason::VALID;
}

} // namespace duckdb
