#include "inet_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

namespace duckdb {

// While the address field is better represented as a uhugeint_t, the original
// implementation used hugeint_t, so to maintain backward-compatibility it will
// continue to be stored as signed. However, operations on the address values
// will use the unsigned variant, so use the functions below to convert to/from
// the compatible representation.
using INET_TYPE = StructTypeTernary<uint8_t, hugeint_t, uint16_t>;

static uhugeint_t FromCompatAddr(hugeint_t compat_addr, IPAddressType addr_type) {
	uhugeint_t retval = static_cast<uhugeint_t>(compat_addr);
	// Only flip the bit for order on IPv6 addresses. It can never be set in IPv4
	if (addr_type == IPAddressType::IP_ADDRESS_V6) {
		// The top bit is flipped when storing as the signed hugeint so that sorting
		// works correctly. Flip it back here to have a proper unsigned value.
		retval.upper ^= (uint64_t(1) << 63);
	}

	return retval;
}

static hugeint_t ToCompatAddr(uhugeint_t new_addr, IPAddressType addr_type) {
	if (addr_type == IPAddressType::IP_ADDRESS_V6) {
		// Flip the top bit when storing as a signed hugeint_t so that sorting
		// works correctly.
		new_addr.upper ^= (uint64_t(1) << 63);
	}
	// Don't need to flip the bit for IPv4, and the original IPv4 only
	// implementation didn't do the flipping, so maintain compatibility.
	return static_cast<hugeint_t>(new_addr);
}

bool INetFunctions::CastVarcharToINET(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto &entries = StructVector::GetEntries(result);
	auto ip_type = FlatVector::GetData<uint8_t>(*entries[0]);
	auto address_data = FlatVector::GetData<hugeint_t>(*entries[1]);
	auto mask_data = FlatVector::GetData<uint16_t>(*entries[2]);

	auto input = UnifiedVectorFormat::GetData<string_t>(vdata);
	bool success = true;
	for (idx_t i = 0; i < (constant ? 1 : count); i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		IPAddress inet;
		if (!IPAddress::TryParse(input[idx], inet, parameters)) {
			FlatVector::SetNull(result, i, true);
			success = false;
			continue;
		}
		ip_type[i] = uint8_t(inet.type);
		address_data[i] = ToCompatAddr(inet.address, inet.type);
		mask_data[i] = inet.mask;
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return success;
}

bool INetFunctions::CastINETToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	GenericExecutor::ExecuteUnary<INET_TYPE, PrimitiveType<string_t>>(source, result, count, [&](INET_TYPE input) {
		auto addr_type = IPAddressType(input.a_val);
		auto unsigned_addr = FromCompatAddr(input.b_val, addr_type);
		IPAddress inet(addr_type, unsigned_addr, input.c_val);
		auto str = inet.ToString();
		return StringVector::AddString(result, str);
	});
	return true;
}

void INetFunctions::Host(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteUnary<INET_TYPE, PrimitiveType<string_t>>(
	    args.data[0], result, args.size(), [&](INET_TYPE input) {
		    auto inetType = IPAddressType(input.a_val);
		    auto mask =
		        inetType == IPAddressType::IP_ADDRESS_V4 ? IPAddress::IPV4_DEFAULT_MASK : IPAddress::IPV6_DEFAULT_MASK;
		    auto unsigned_addr = FromCompatAddr(input.b_val, inetType);
		    IPAddress inet(inetType, unsigned_addr, mask);
		    auto str = inet.ToString();
		    return StringVector::AddString(result, str);
	    });
}

void INetFunctions::Family(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteUnary<INET_TYPE, PrimitiveType<uint8_t>>(
	    args.data[0], result, args.size(), [&](INET_TYPE input) {
		    auto inetType = IPAddressType(input.a_val);
		    return inetType == IPAddressType::IP_ADDRESS_V4 ? 4 : 6;
	    });
}

void INetFunctions::Netmask(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteUnary<INET_TYPE, INET_TYPE>(args.data[0], result, args.size(), [&](INET_TYPE input) {
		IPAddressType type = IPAddressType(input.a_val);
		IPAddress inet(type, FromCompatAddr(input.b_val, type), input.c_val);
		IPAddress netmask = inet.Netmask();

		INET_TYPE str;
		str.a_val = uint8_t(netmask.type);
		str.b_val = ToCompatAddr(netmask.address, netmask.type);
		str.c_val = netmask.mask;
		return str;
	});
}

void INetFunctions::Network(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteUnary<INET_TYPE, INET_TYPE>(args.data[0], result, args.size(), [&](INET_TYPE input) {
		IPAddressType type = IPAddressType(input.a_val);
		IPAddress inet(type, FromCompatAddr(input.b_val, type), input.c_val);
		IPAddress network = inet.Network();

		INET_TYPE str;
		str.a_val = uint8_t(network.type);
		str.b_val = ToCompatAddr(network.address, network.type);
		str.c_val = network.mask;
		return str;
	});
}

void INetFunctions::Broadcast(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteUnary<INET_TYPE, INET_TYPE>(args.data[0], result, args.size(), [&](INET_TYPE input) {
		IPAddressType type = IPAddressType(input.a_val);
		IPAddress inet(type, FromCompatAddr(input.b_val, type), input.c_val);
		IPAddress broadcast = inet.Broadcast();

		INET_TYPE str;
		str.a_val = uint8_t(broadcast.type);
		str.b_val = ToCompatAddr(broadcast.address, broadcast.type);
		str.c_val = broadcast.mask;
		return str;
	});
}

// The signed hugeint_t value cannot extend through the full IPv6 range in one
// operation, but it is the largest native signed type available and should be
// appropriate for most realistic operations. Using the signed type will make
// the add/subtract SQL interface the most natural.
static INET_TYPE AddImplementation(INET_TYPE ip, hugeint_t val) {
	if (val == 0) {
		return ip;
	}

	INET_TYPE result;
	auto addr_type = IPAddressType(ip.a_val);
	uhugeint_t address_in = FromCompatAddr(ip.b_val, addr_type);
	uhugeint_t address_out;
	result.a_val = ip.a_val;
	result.c_val = ip.c_val;

	// Use the overflow checking operators to ensure well-defined behavior. The
	// operators must operate on the same type signedness, so convert the operand as
	// necessary, and choose between add/subtraction operations.
	if (val > 0) {
		address_out = AddOperatorOverflowCheck::Operation<uhugeint_t, uhugeint_t, uhugeint_t>(address_in, val);
	} else {
		address_out = SubtractOperatorOverflowCheck::Operation<uhugeint_t, uhugeint_t, uhugeint_t>(address_in, -val);
	}

	if (addr_type == IPAddressType::IP_ADDRESS_V4 && address_out >= (uhugeint_t(0xffffffff))) {
		throw OutOfRangeException("Cannot add %s to %s.", val, IPAddress(addr_type, address_in, ip.c_val).ToString());
	}

	result.b_val = ToCompatAddr(address_out, addr_type);

	return result;
}

// Calculate if the left INET is a subset of the right INET
static bool ContainsImplementation(INET_TYPE ip_left, INET_TYPE ip_right) {
	IPAddressType left_addr_type = IPAddressType(ip_left.a_val);
	IPAddressType right_addr_type = IPAddressType(ip_right.a_val);

	if (left_addr_type == right_addr_type) {
		uhugeint_t left_addr_ip = FromCompatAddr(ip_left.b_val, left_addr_type);
		uhugeint_t right_addr_ip = FromCompatAddr(ip_right.b_val, right_addr_type);

		IPAddress left(left_addr_type, left_addr_ip, ip_left.c_val);
		IPAddress right(right_addr_type, right_addr_ip, ip_right.c_val);

		return left.Network().address >= right.Network().address &&
		       left.Broadcast().address <= right.Broadcast().address;
	}

	return false;
}

void INetFunctions::Subtract(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteBinary<INET_TYPE, PrimitiveType<hugeint_t>, INET_TYPE>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](INET_TYPE ip, PrimitiveType<hugeint_t> val) { return AddImplementation(ip, -val.val); });
}

void INetFunctions::Add(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteBinary<INET_TYPE, PrimitiveType<hugeint_t>, INET_TYPE>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](INET_TYPE ip, PrimitiveType<hugeint_t> val) { return AddImplementation(ip, val.val); });
}

void INetFunctions::ContainsLeft(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](INET_TYPE ip_left, INET_TYPE ip_right) { return ContainsImplementation(ip_left, ip_right); });
}

void INetFunctions::ContainsRight(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](INET_TYPE ip_left, INET_TYPE ip_right) { return ContainsImplementation(ip_right, ip_left); });
}

} // namespace duckdb
