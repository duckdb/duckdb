/**
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 * (c) Daniel Lemire, http://fastpforlib.me/en/
 */
#pragma once
#include <cinttypes>
#include <string>

namespace duckdb_fastpforlib {
namespace internal {

// Unpacks 8 uint8_t values
void __fastunpack0(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastunpack1(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastunpack2(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastunpack3(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastunpack4(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastunpack5(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastunpack6(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastunpack7(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastunpack8(const uint8_t *__restrict in, uint8_t *__restrict out);

// Unpacks 16 uint16_t values
void __fastunpack0(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack1(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack2(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack3(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack4(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack5(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack6(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack7(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack8(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack9(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack10(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack11(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack12(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack13(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack14(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack15(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastunpack16(const uint16_t *__restrict in, uint16_t *__restrict out);

// Unpacks 32 uint32_t values
void __fastunpack0(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack1(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack2(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack3(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack4(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack5(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack6(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack7(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack8(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack9(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack10(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack11(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack12(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack13(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack14(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack15(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack16(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack17(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack18(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack19(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack20(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack21(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack22(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack23(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack24(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack25(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack26(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack27(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack28(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack29(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack30(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack31(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastunpack32(const uint32_t *__restrict in, uint32_t *__restrict out);

// Unpacks 32 uint64_t values
void __fastunpack0(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack1(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack2(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack3(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack4(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack5(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack6(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack7(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack8(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack9(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack10(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack11(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack12(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack13(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack14(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack15(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack16(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack17(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack18(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack19(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack20(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack21(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack22(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack23(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack24(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack25(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack26(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack27(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack28(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack29(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack30(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack31(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack32(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack33(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack34(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack35(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack36(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack37(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack38(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack39(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack40(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack41(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack42(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack43(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack44(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack45(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack46(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack47(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack48(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack49(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack50(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack51(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack52(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack53(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack54(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack55(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack56(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack57(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack58(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack59(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack60(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack61(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack62(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack63(const uint32_t *__restrict in, uint64_t *__restrict out);
void __fastunpack64(const uint32_t *__restrict in, uint64_t *__restrict out);

// Packs 8 int8_t values
void __fastpack0(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastpack1(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastpack2(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastpack3(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastpack4(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastpack5(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastpack6(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastpack7(const uint8_t *__restrict in, uint8_t *__restrict out);
void __fastpack8(const uint8_t *__restrict in, uint8_t *__restrict out);

// Packs 16 int16_t values
void __fastpack0(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack1(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack2(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack3(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack4(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack5(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack6(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack7(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack8(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack9(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack10(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack11(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack12(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack13(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack14(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack15(const uint16_t *__restrict in, uint16_t *__restrict out);
void __fastpack16(const uint16_t *__restrict in, uint16_t *__restrict out);

// Packs 32 int32_t values
void __fastpack0(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack1(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack2(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack3(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack4(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack5(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack6(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack7(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack8(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack9(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack10(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack11(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack12(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack13(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack14(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack15(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack16(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack17(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack18(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack19(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack20(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack21(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack22(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack23(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack24(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack25(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack26(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack27(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack28(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack29(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack30(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack31(const uint32_t *__restrict in, uint32_t *__restrict out);
void __fastpack32(const uint32_t *__restrict in, uint32_t *__restrict out);

// Packs 32 int64_t values
void __fastpack0(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack1(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack2(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack3(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack4(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack5(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack6(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack7(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack8(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack9(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack10(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack11(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack12(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack13(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack14(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack15(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack16(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack17(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack18(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack19(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack20(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack21(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack22(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack23(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack24(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack25(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack26(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack27(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack28(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack29(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack30(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack31(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack32(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack33(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack34(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack35(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack36(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack37(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack38(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack39(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack40(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack41(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack42(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack43(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack44(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack45(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack46(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack47(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack48(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack49(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack50(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack51(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack52(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack53(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack54(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack55(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack56(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack57(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack58(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack59(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack60(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack61(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack62(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack63(const uint64_t *__restrict in, uint32_t *__restrict out);
void __fastpack64(const uint64_t *__restrict in, uint32_t *__restrict out);
} // namespace internal
} // namespace duckdb_fastpforlib
