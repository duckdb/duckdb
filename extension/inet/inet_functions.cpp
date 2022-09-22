#include "inet_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

namespace duckdb {

bool INetFunctions::CastVarcharToINET(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto &entries = StructVector::GetEntries(result);
	auto address_data = FlatVector::GetData<hugeint_t>(*entries[0]);
	auto mask_data = FlatVector::GetData<uint16_t>(*entries[1]);
	auto ip_type = FlatVector::GetData<uint8_t>(*entries[2]);

	auto input = (string_t *)vdata.data;
	bool success = true;
	for (idx_t i = 0; i < (constant ? 1 : count); i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		IPAddress inet;
		if (!IPAddress::TryParse(input[idx], inet, parameters.error_message)) {
			FlatVector::SetNull(result, i, true);
			success = false;
			continue;
		}
		address_data[i] = inet.address;
		mask_data[i] = inet.mask;
		ip_type[i] = uint8_t(inet.type);
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return success;
}

bool INetFunctions::CastINETToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	GenericExecutor::ExecuteUnary<StructTypeTernary<hugeint_t, uint16_t, uint8_t>, PrimitiveType<string_t>>(
	    source, result, count, [&](StructTypeTernary<hugeint_t, uint16_t, uint8_t> input) {
		    IPAddress inet(input.a_val, input.b_val, IPAddressType(input.c_val));
		    auto str = inet.ToString();
		    return StringVector::AddString(result, str);
	    });
	return true;
}

void INetFunctions::Host(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteUnary<StructTypeTernary<hugeint_t, uint16_t, uint8_t>, PrimitiveType<string_t>>(
	    args.data[0], result, args.size(), [&](StructTypeTernary<hugeint_t, uint16_t, uint8_t> input) {
		    IPAddress inet(input.a_val, IPAddress::IPV4_DEFAULT_MASK, IPAddressType(input.c_val));
		    auto str = inet.ToString();
		    return StringVector::AddString(result, str);
	    });
}

void INetFunctions::Subtract(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteBinary<StructTypeTernary<hugeint_t, uint16_t, uint8_t>, PrimitiveType<int32_t>,
	                               StructTypeTernary<hugeint_t, uint16_t, uint8_t>>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](StructTypeTernary<hugeint_t, uint16_t, uint8_t> ip, PrimitiveType<int32_t> val) {
		    auto new_address = ip.a_val - val.val;
		    if (new_address < 0) {
			    throw NotImplementedException("Out of range!?");
		    }
		    StructTypeTernary<hugeint_t, uint16_t, uint8_t> result;
		    result.a_val = new_address;
		    result.b_val = ip.b_val;
		    result.c_val = ip.c_val;
		    return result;
	    });
}

} // namespace duckdb
