#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb {

struct StructBoundCastData : public BoundCastData {
	StructBoundCastData(vector<BoundCastInfo> child_casts, LogicalType target_p)
	    : child_cast_info(move(child_casts)), target(move(target_p)) {
	}

	vector<BoundCastInfo> child_cast_info;
	LogicalType target;

public:
	unique_ptr<BoundCastData> Copy() const override {
		vector<BoundCastInfo> copy_info;
		for (auto &info : child_cast_info) {
			copy_info.push_back(info.Copy());
		}
		return make_unique<StructBoundCastData>(move(copy_info), target);
	}
};

unique_ptr<BoundCastData> BindStructToStructCast(BindCastInput &input, const LogicalType &source,
                                                 const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto &source_child_types = StructType::GetChildTypes(source);
	auto &result_child_types = StructType::GetChildTypes(target);
	if (source_child_types.size() != result_child_types.size()) {
		throw TypeMismatchException(source, target, "Cannot cast STRUCTs of different size");
	}
	for (idx_t i = 0; i < source_child_types.size(); i++) {
		auto child_cast =
		    input.function_set.GetCastFunction(source_child_types[i].second, result_child_types[i].second);
		child_cast_info.push_back(move(child_cast));
	}
	return make_unique<StructBoundCastData>(move(child_cast_info), target);
}

static bool StructToStructCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = (StructBoundCastData &)*parameters.cast_data;
	auto &source_child_types = StructType::GetChildTypes(source.GetType());
	auto &source_children = StructVector::GetEntries(source);
	D_ASSERT(source_children.size() == StructType::GetChildTypes(result.GetType()).size());

	auto &result_children = StructVector::GetEntries(result);
	for (idx_t c_idx = 0; c_idx < source_child_types.size(); c_idx++) {
		auto &result_child_vector = *result_children[c_idx];
		auto &source_child_vector = *source_children[c_idx];
		CastParameters child_parameters(parameters, cast_data.child_cast_info[c_idx].cast_data.get());
		if (!cast_data.child_cast_info[c_idx].function(source_child_vector, result_child_vector, count,
		                                               child_parameters)) {
			return false;
		}
	}
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, ConstantVector::IsNull(source));
	} else {
		source.Flatten(count);
		FlatVector::Validity(result) = FlatVector::Validity(source);
	}
	return true;
}

static bool StructToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;
	// first cast all child elements to varchar
	auto &cast_data = (StructBoundCastData &)*parameters.cast_data;
	Vector varchar_struct(cast_data.target);
	StructToStructCast(source, varchar_struct, count, parameters);

	// now construct the actual varchar vector
	varchar_struct.Flatten(count);
	auto &child_types = StructType::GetChildTypes(source.GetType());
	auto &children = StructVector::GetEntries(varchar_struct);
	auto &validity = FlatVector::Validity(varchar_struct);
	auto result_data = FlatVector::GetData<string_t>(result);
	// first compute the sizes of each of the strings
	vector<idx_t> string_lengths(count, 0);
	static constexpr const idx_t INITIAL_LENGTH = 2;
	static constexpr const idx_t SEP_LENGTH = 2;
	static constexpr const idx_t NULL_LENGTH = 4;
	// set up nulls
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		string_lengths[i] = INITIAL_LENGTH; // {}
	}
	// add the sizes of each of the children
	for (idx_t c = 0; c < children.size(); c++) {
		auto &child_validity = FlatVector::Validity(*children[c]);
		auto data = FlatVector::GetData<string_t>(*children[c]);
		auto last_child = c + 1 == children.size();
		auto &name = child_types[c].first;
		for (idx_t i = 0; i < count; i++) {
			if (string_lengths[i] == 0) {
				// NULL
				continue;
			}
			if (c > 0) {
				string_lengths[i] += SEP_LENGTH; // ", "
			}
			string_lengths[i] += name.size() + 4; // "'{name}': "
			string_lengths[i] += child_validity.RowIsValid(i) ? data[i].GetSize() : NULL_LENGTH;
			if (last_child) {
				// initialize the empty strings if the length is known
				result_data[i] = StringVector::EmptyString(result, string_lengths[i]);
			}
		}
	}
	// now copy over the data
	vector<idx_t> offsets(count, 1);
	for (idx_t c = 0; c < children.size(); c++) {
		auto &child_validity = FlatVector::Validity(*children[c]);
		auto data = FlatVector::GetData<string_t>(*children[c]);
		auto &name = child_types[c].first;
		auto last_child = c + 1 == children.size();
		for (idx_t i = 0; i < count; i++) {
			if (string_lengths[i] == 0) {
				// NULL
				continue;
			}
			auto dataptr = result_data[i].GetDataWriteable();
			if (c > 0) {
				memcpy(dataptr + offsets[i], ", ", SEP_LENGTH);
				offsets[i] += SEP_LENGTH;
			}
			dataptr[offsets[i]] = '\'';
			offsets[i]++;

			memcpy(dataptr + offsets[i], name.c_str(), name.size());
			offsets[i] += name.size();

			memcpy(dataptr + offsets[i], "': ", 3);
			offsets[i] += 3;

			if (child_validity.RowIsValid(i)) {
				auto len = data[i].GetSize();
				memcpy(dataptr + offsets[i], data[i].GetDataUnsafe(), len);
				offsets[i] += len;
			} else {
				memcpy(dataptr + offsets[i], "NULL", NULL_LENGTH);
				offsets[i] += NULL_LENGTH;
			}
			if (last_child) {
				// initialize the empty strings if the length is known
				dataptr[0] = '{';
				dataptr[string_lengths[i] - 1] = '}';
				result_data[i].Finalize();
			}
		}
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

BoundCastInfo DefaultCasts::StructCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::STRUCT:
		return BoundCastInfo(StructToStructCast, BindStructToStructCast(input, source, target));
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR: {
		// bind a cast in which we convert all child entries to VARCHAR entries
		auto &struct_children = StructType::GetChildTypes(source);
		child_list_t<LogicalType> varchar_children;
		for (auto &child_entry : struct_children) {
			varchar_children.push_back(make_pair(child_entry.first, LogicalType::VARCHAR));
		}
		auto varchar_type = LogicalType::STRUCT(move(varchar_children));
		return BoundCastInfo(StructToVarcharCast, BindStructToStructCast(input, source, varchar_type));
	}
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
