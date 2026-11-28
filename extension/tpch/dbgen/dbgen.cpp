#include "dbgen/dbgen.hpp"
#include "dbgen/dbgen_gunk.hpp"
#include "tpch_constants.hpp"

#define DECLARER /* EXTERN references get defined here */

#include "dbgen/dss.h"
#include "dbgen/rng64.h"
#include "dbgen/dsstypes.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <mutex>

using duckdb::cxx::Arena;
using duckdb::cxx::DataChunk;
using duckdb::cxx::date_t;
using duckdb::cxx::Factory;
using duckdb::cxx::LogicalType;
using duckdb::cxx::varchar_t;
using std::string;

namespace tpch {

template <class T, class S>
static T NumericCast(S value) {
	assert(static_cast<S>(static_cast<T>(value)) == value);
	return static_cast<T>(value);
}

template <class T>
static T MaxValue(T a, T b) {
	return a > b ? a : b;
}

template <class T>
static T MinValue(T a, T b) {
	return a < b ? a : b;
}

static constexpr idx_t INVALID_INDEX = std::numeric_limits<idx_t>::max();

//! Fills the rows of one table into a chunk borrowed for one Fill call
struct tpch_append_information {
	DataChunk *chunk = nullptr;
	idx_t column_count = 0;
	idx_t capacity = 0;
	idx_t row = 0;
	idx_t active_row = INVALID_INDEX;
	idx_t active_col = 0;
	//! raw column pointers of the chunk, so the per-value append path does no bounds or type checks
	static constexpr idx_t MAX_COLUMNS = 16;
	void *column_data[MAX_COLUMNS];
	//! the string heaps of the columns, looked up on first use
	std::unique_ptr<Arena> heaps[MAX_COLUMNS];

	//! Whether the scan generates this table
	bool IsEnabled() const {
		return chunk != nullptr;
	}

	void Bind(DataChunk &target) {
		chunk = &target;
		column_count = target.GetVectorCount();
		assert(column_count <= MAX_COLUMNS);
		capacity = target.GetCapacity();
		row = 0;
		for (idx_t col = 0; col < column_count; col++) {
			column_data[col] = target.GetVector(col).GetDataMutable();
			heaps[col].reset();
		}
	}

	//! Sets the row count of the chunk, and releases it
	void Finish() {
		assert(active_row == INVALID_INDEX);
		for (idx_t col = 0; col < column_count; col++) {
			chunk->GetVector(col).SetSize(row);
			heaps[col].reset();
		}
		chunk = nullptr;
	}

	char *AllocateString(idx_t col, idx_t length) {
		if (!heaps[col]) {
			heaps[col] = std::unique_ptr<Arena>(new Arena(chunk->GetVector(col).GetHeap()));
		}
		return reinterpret_cast<char *>(heaps[col]->Allocate(length));
	}
};

static void append_begin_row(tpch_append_information &info) {
	assert(info.IsEnabled());
	assert(info.active_row == INVALID_INDEX);
	assert(info.row < info.capacity);
	info.active_row = info.row;
	info.active_col = 0;
}

static void append_end_row(tpch_append_information &info) {
	assert(info.active_row != INVALID_INDEX);
	assert(info.active_col == info.column_count);
	info.row++;
	info.active_row = INVALID_INDEX;
}

template <class T>
static T *append_next_column_data(tpch_append_information &info) {
	assert(info.active_row != INVALID_INDEX);
	assert(info.active_col < info.column_count);
	return static_cast<T *>(info.column_data[info.active_col++]);
}

static void append_int32(tpch_append_information &info, int32_t value) {
	append_next_column_data<int32_t>(info)[info.active_row] = value;
}

static void append_int64(tpch_append_information &info, int64_t value) {
	append_next_column_data<int64_t>(info)[info.active_row] = value;
}

//! Only for strings that outlive the scan: the dbgen distributions and text pool
static void append_string_reference(tpch_append_information &info, const char *value, idx_t length) {
	append_next_column_data<varchar_t>(info)[info.active_row] = varchar_t(value, NumericCast<uint32_t>(length));
}

//! DECIMAL(15,2), stored as an int64 of cents
static void append_decimal(tpch_append_information &info, int64_t value) {
	append_next_column_data<int64_t>(info)[info.active_row] = value;
}

static void append_char(tpch_append_information &info, char value) {
	// a single character is stored inline
	append_next_column_data<varchar_t>(info)[info.active_row] = varchar_t(&value, 1);
}

static date_t raw_tpch_date(DSS_HUGE value) {
	static constexpr int32_t UNIX_EPOCH_OFFSET = 8035;
	// DBGEN dates are raw day offsets from STARTDATE; DuckDB dates are days since the Unix epoch.
	date_t result;
	result.days = NumericCast<int32_t>(value - STARTDATE + UNIX_EPOCH_OFFSET);
	return result;
}

static void append_date(tpch_append_information &info, DSS_HUGE value) {
	append_next_column_data<date_t>(info)[info.active_row] = raw_tpch_date(value);
}

//! A string of a known length built in place: write its bytes, then Finalize to store it in its row
struct PendingString {
	varchar_t *slot;
	uint32_t length;
	//! the bytes of a string too long to be inlined, in the heap of its vector
	char *heap_data;
	char inline_data[varchar_t::INLINE_LENGTH];

	char *GetDataWriteable() {
		return heap_data ? heap_data : inline_data;
	}
	void Finalize() {
		*slot = varchar_t(GetDataWriteable(), length);
	}
};

static PendingString append_empty_string(tpch_append_information &info, idx_t length) {
	auto col = info.active_col;
	auto slot = &append_next_column_data<varchar_t>(info)[info.active_row];
	char *heap_data = length > varchar_t::INLINE_LENGTH ? info.AllocateString(col, length) : nullptr;
	return PendingString {slot, NumericCast<uint32_t>(length), heap_data, {}};
}

static int PaddedNumberWidth(DSS_HUGE value, int min_width) {
	int width = 1;
	for (auto remaining = value; remaining >= 10; remaining /= 10) {
		width++;
	}
	return MaxValue(width, min_width);
}

static void WritePaddedNumber(char *target, DSS_HUGE value, int width) {
	for (auto i = width; i > 0; i--) {
		target[i - 1] = char('0' + (value % 10));
		value /= 10;
	}
}

static void AppendTaggedNumber(tpch_append_information &info, const char *tag, idx_t tag_length, DSS_HUGE value,
                               int min_width) {
	auto number_width = PaddedNumberWidth(value, min_width);
	auto result = append_empty_string(info, tag_length + number_width);
	auto target = result.GetDataWriteable();
	memcpy(target, tag, tag_length);
	WritePaddedNumber(target + tag_length, value, number_width);
	result.Finalize();
}

static void AppendPhone(tpch_append_information &info, DSS_HUGE nation_code, seed_t *seed) {
	DSS_HUGE acode, exchg, number;
	RANDOM(acode, 100, 999, seed);
	RANDOM(exchg, 100, 999, seed);
	RANDOM(number, 1000, 9999, seed);

	auto result = append_empty_string(info, PHONE_LEN);
	auto target = result.GetDataWriteable();
	WritePaddedNumber(target, 10 + (nation_code % NATIONS_MAX), 2);
	target[2] = '-';
	WritePaddedNumber(target + 3, acode, 3);
	target[6] = '-';
	WritePaddedNumber(target + 7, exchg, 3);
	target[10] = '-';
	WritePaddedNumber(target + 11, number, 4);
	result.Finalize();
}

static void AppendRandomAlphaNumeric(tpch_append_information &info, int average_length, seed_t *seed) {
	static const char ALPHA_NUM[] = "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";
	DSS_HUGE length, char_int = 0;
	RANDOM(length, int(average_length * V_STR_LOW), int(average_length * V_STR_HGH), seed);

	auto result = append_empty_string(info, NumericCast<idx_t>(length));
	auto target = result.GetDataWriteable();
	for (DSS_HUGE i = 0; i < length; i++) {
		if (i % 5 == 0) {
			RANDOM(char_int, 0, MAX_LONG, seed);
		}
		target[i] = ALPHA_NUM[char_int & 077];
		char_int >>= 6;
	}
	result.Finalize();
}

static void AppendText(tpch_append_information &info, int average_length, seed_t *seed) {
	const char *source;
	auto length = dbg_text_source(int(average_length * V_STR_LOW), int(average_length * V_STR_HGH), seed, &source);
	// Text pool memory is stable while a generator exists, which outlives the scans reading it.
	append_string_reference(info, source, NumericCast<idx_t>(length));
}

struct TPCHStringRef {
	const char *data;
	idx_t length;
};

static TPCHStringRef PickDistribution(distribution *set, seed_t *seed) {
	long index = 0;
	DSS_HUGE choice;

	RANDOM(choice, 1, set->list[set->count - 1].weight, seed);
	while (set->list[index].weight < choice) {
		index++;
	}
	auto data = set->list[index].text;
	return TPCHStringRef {data, NumericCast<idx_t>(set->list[index].length)};
}

static void AppendDistribution(tpch_append_information &info, distribution *set, seed_t *seed) {
	auto entry = PickDistribution(set, seed);
	append_string_reference(info, entry.data, entry.length);
}

static DSS_HUGE RetailPrice(DSS_HUGE part_key) {
	DSS_HUGE price = 90000;
	price += (part_key / 10) % 20001;
	price += (part_key % 1000) * 100;
	return price;
}

static DSS_HUGE PartSuppBridge(DBGenContext *ctx, DSS_HUGE part_key, DSS_HUGE supp_num) {
	DSS_HUGE total_supplier_count = ctx->tdefs[SUPP].base * ctx->scale_factor;
	return (part_key +
	        supp_num * (total_supplier_count / SUPP_PER_PART + (long)((part_key - 1) / total_supplier_count))) %
	           total_supplier_count +
	       1;
}

static void AppendPartName(tpch_append_information &info, DBGenContext *ctx) {
	permute_dist(&colors, P_NAME_SCL, &ctx->Seed[P_NAME_SD], ctx);
	idx_t length = 0;
	for (idx_t i = 0; i < P_NAME_SCL; i++) {
		length += NumericCast<idx_t>(colors.list[ctx->permute[i]].length);
	}
	length += P_NAME_SCL - 1;

	auto result = append_empty_string(info, length);
	auto target = result.GetDataWriteable();
	for (idx_t i = 0; i < P_NAME_SCL; i++) {
		auto &member = colors.list[ctx->permute[i]];
		auto member_length = NumericCast<idx_t>(member.length);
		auto member_text = member.text;
		memcpy(target, member_text, member_length);
		target += member_length;
		if (i + 1 < P_NAME_SCL) {
			*target++ = ' ';
		}
	}
	result.Finalize();
}

static void AppendSupplierComment(tpch_append_information &info, DBGenContext *ctx) {
	const char *source;
	auto length =
	    dbg_text_source(int(S_CMNT_LEN * V_STR_LOW), int(S_CMNT_LEN * V_STR_HGH), &ctx->Seed[S_CMNT_SD], &source);
	// Supplier comments can be patched with BBB text, so they need writable vector-owned storage.
	auto result = append_empty_string(info, NumericCast<idx_t>(length));
	auto target = result.GetDataWriteable();
	memcpy(target, source, NumericCast<idx_t>(length));

	DSS_HUGE bad_press, noise, offset, type;
	RANDOM(bad_press, 1, 10000, &ctx->Seed[BBB_CMNT_SD]);
	RANDOM(type, 0, 100, &ctx->Seed[BBB_TYPE_SD]);
	RANDOM(noise, 0, (length - BBB_CMNT_LEN), &ctx->Seed[BBB_JNK_SD]);
	RANDOM(offset, 0, (length - (BBB_CMNT_LEN + noise)), &ctx->Seed[BBB_OFFSET_SD]);
	if (bad_press <= S_CMNT_BBB) {
		type = (type < BBB_DEADBEATS) ? 0 : 1;
		memcpy(target + offset, BBB_BASE, BBB_BASE_LEN);
		if (type == 0) {
			memcpy(target + BBB_BASE_LEN + offset + noise, BBB_COMPLAIN, BBB_TYPE_LEN);
		} else {
			memcpy(target + BBB_BASE_LEN + offset + noise, BBB_COMMEND, BBB_TYPE_LEN);
		}
	}
	result.Finalize();
}

static void AppendManufacturer(tpch_append_information &info, DSS_HUGE manufacturer) {
	AppendTaggedNumber(info, P_MFG_TAG, sizeof(P_MFG_TAG) - 1, manufacturer, 1);
}

static void AppendBrand(tpch_append_information &info, DSS_HUGE manufacturer, DSS_HUGE brand) {
	AppendTaggedNumber(info, P_BRND_TAG, sizeof(P_BRND_TAG) - 1, manufacturer * 10 + brand, 2);
}

//! Generates an order and its lines. Orders and lineitem are generated by separate scans, so each skips the random
//! draws only the other table needs: row_stop_h moves every stream of the table to the start of the next row.
static void GenerateOrderLine(DSS_HUGE index, tpch_append_information *info, DBGenContext *ctx) {
	auto &order_info = info[ORDER];
	auto &line_info = info[LINE];
	const bool orders = order_info.IsEnabled();
	const bool lines = line_info.IsEnabled();
	static const DSS_HUGE CURRENT_DATE_RAW = STARTDATE + unjulian(CURRENTDATE);

	DSS_HUGE order_key;
	mk_sparse(index, &order_key, 0);

	DSS_HUGE cust_key = 0;
	TPCHStringRef order_priority {nullptr, 0};
	DSS_HUGE clerk_number = 0;
	const char *order_comment = nullptr;
	long order_comment_length = 0;
	if (orders) {
		if (ctx->scale_factor >= 30000) {
			dss_random64(&cust_key, O_CKEY_MIN, O_CKEY_MAX, &ctx->Seed[O_CKEY_SD]);
		} else {
			RANDOM(cust_key, O_CKEY_MIN, O_CKEY_MAX, &ctx->Seed[O_CKEY_SD]);
		}
		int delta = 1;
		while (cust_key % CUST_MORTALITY == 0) {
			cust_key += delta;
			cust_key = MIN(cust_key, O_CKEY_MAX);
			delta *= -1;
		}
	}

	DSS_HUGE order_date;
	RANDOM(order_date, O_ODATE_MIN, O_ODATE_MAX, &ctx->Seed[O_ODATE_SD]);
	if (orders) {
		order_priority = PickDistribution(&o_priority_set, &ctx->Seed[O_PRIO_SD]);
		RANDOM(clerk_number, 1, MAX((ctx->scale_factor * O_CLRK_SCL), O_CLRK_SCL), &ctx->Seed[O_CLRK_SD]);
		order_comment_length = dbg_text_source(int(O_CMNT_LEN * V_STR_LOW), int(O_CMNT_LEN * V_STR_HGH),
		                                       &ctx->Seed[O_CMNT_SD], &order_comment);
	}

	DSS_HUGE line_count;
	RANDOM(line_count, O_LCNT_MIN, O_LCNT_MAX, &ctx->Seed[O_LCNT_SD]);

	DSS_HUGE total_price = 0;
	DSS_HUGE closed_line_count = 0;
	for (DSS_HUGE line_number = 0; line_number < line_count; line_number++) {
		DSS_HUGE quantity, discount, tax, part_key, ship_date;
		RANDOM(quantity, L_QTY_MIN, L_QTY_MAX, &ctx->Seed[L_QTY_SD]);
		RANDOM(discount, L_DCNT_MIN, L_DCNT_MAX, &ctx->Seed[L_DCNT_SD]);
		RANDOM(tax, L_TAX_MIN, L_TAX_MAX, &ctx->Seed[L_TAX_SD]);
		TPCHStringRef ship_instruct {nullptr, 0};
		TPCHStringRef ship_mode {nullptr, 0};
		const char *line_comment = nullptr;
		long line_comment_length = 0;
		if (lines) {
			ship_instruct = PickDistribution(&l_instruct_set, &ctx->Seed[L_SHIP_SD]);
			ship_mode = PickDistribution(&l_smode_set, &ctx->Seed[L_SMODE_SD]);
			line_comment_length = dbg_text_source(int(L_CMNT_LEN * V_STR_LOW), int(L_CMNT_LEN * V_STR_HGH),
			                                      &ctx->Seed[L_CMNT_SD], &line_comment);
		}
		if (ctx->scale_factor >= 30000) {
			dss_random64(&part_key, L_PKEY_MIN, L_PKEY_MAX, &ctx->Seed[L_PKEY_SD]);
		} else {
			RANDOM(part_key, L_PKEY_MIN, L_PKEY_MAX, &ctx->Seed[L_PKEY_SD]);
		}
		auto retail_price = RetailPrice(part_key);
		quantity *= 100;
		auto extended_price = retail_price * quantity / 100;
		total_price += ((extended_price * (100 - discount)) / PENNIES) * (100 + tax) / PENNIES;

		RANDOM(ship_date, L_SDTE_MIN, L_SDTE_MAX, &ctx->Seed[L_SDTE_SD]);
		ship_date += order_date;
		if (ship_date <= CURRENT_DATE_RAW) {
			closed_line_count++;
		}
		if (!lines) {
			continue;
		}

		DSS_HUGE supp_num, commit_date, receipt_date;
		RANDOM(supp_num, 0, 3, &ctx->Seed[L_SKEY_SD]);
		auto supp_key = PartSuppBridge(ctx, part_key, supp_num);
		RANDOM(commit_date, L_CDTE_MIN, L_CDTE_MAX, &ctx->Seed[L_CDTE_SD]);
		commit_date += order_date;
		RANDOM(receipt_date, L_RDTE_MIN, L_RDTE_MAX, &ctx->Seed[L_RDTE_SD]);
		receipt_date += ship_date;

		char return_flag = 'N';
		if (receipt_date <= CURRENT_DATE_RAW) {
			return_flag = PickDistribution(&l_rflag_set, &ctx->Seed[L_RFLG_SD]).data[0];
		}
		char line_status = ship_date <= CURRENT_DATE_RAW ? 'F' : 'O';

		append_begin_row(line_info);
		append_int64(line_info, order_key);
		append_int64(line_info, part_key);
		append_int64(line_info, supp_key);
		append_int64(line_info, line_number + 1);
		append_decimal(line_info, quantity);
		append_decimal(line_info, extended_price);
		append_decimal(line_info, discount);
		append_decimal(line_info, tax);
		append_char(line_info, return_flag);
		append_char(line_info, line_status);
		append_date(line_info, ship_date);
		append_date(line_info, commit_date);
		append_date(line_info, receipt_date);
		append_string_reference(line_info, ship_instruct.data, ship_instruct.length);
		append_string_reference(line_info, ship_mode.data, ship_mode.length);
		append_string_reference(line_info, line_comment, NumericCast<idx_t>(line_comment_length));
		append_end_row(line_info);
	}
	if (!orders) {
		return;
	}

	char order_status = 'O';
	if (closed_line_count > 0) {
		order_status = 'P';
	}
	if (closed_line_count == line_count) {
		order_status = 'F';
	}

	append_begin_row(order_info);
	append_int64(order_info, order_key);
	append_int64(order_info, cust_key);
	append_char(order_info, order_status);
	append_decimal(order_info, total_price);
	append_date(order_info, order_date);
	append_string_reference(order_info, order_priority.data, order_priority.length);
	AppendTaggedNumber(order_info, O_CLRK_TAG, sizeof(O_CLRK_TAG) - 1, clerk_number, 9);
	append_int32(order_info, 0);
	append_string_reference(order_info, order_comment, NumericCast<idx_t>(order_comment_length));
	append_end_row(order_info);
}

static void GenerateSupplier(DSS_HUGE index, tpch_append_information *info, DBGenContext *ctx) {
	auto &append_info = info[SUPP];
	append_begin_row(append_info);
	append_int64(append_info, index);
	AppendTaggedNumber(append_info, S_NAME_TAG, sizeof(S_NAME_TAG) - 1, index, 9);
	AppendRandomAlphaNumeric(append_info, S_ADDR_LEN, &ctx->Seed[S_ADDR_SD]);
	DSS_HUGE nation_code;
	RANDOM(nation_code, 0, nations.count - 1, &ctx->Seed[S_NTRG_SD]);
	append_int32(append_info, NumericCast<int32_t>(nation_code));
	AppendPhone(append_info, nation_code, &ctx->Seed[S_PHNE_SD]);
	DSS_HUGE acctbal;
	RANDOM(acctbal, S_ABAL_MIN, S_ABAL_MAX, &ctx->Seed[S_ABAL_SD]);
	append_decimal(append_info, acctbal);
	AppendSupplierComment(append_info, ctx);
	append_end_row(append_info);
}

static void GenerateCustomer(DSS_HUGE index, tpch_append_information *info, DBGenContext *ctx) {
	auto &append_info = info[CUST];
	append_begin_row(append_info);
	append_int64(append_info, index);
	AppendTaggedNumber(append_info, C_NAME_TAG, sizeof(C_NAME_TAG) - 1, index, 9);
	AppendRandomAlphaNumeric(append_info, C_ADDR_LEN, &ctx->Seed[C_ADDR_SD]);
	DSS_HUGE nation_code;
	RANDOM(nation_code, 0, nations.count - 1, &ctx->Seed[C_NTRG_SD]);
	append_int32(append_info, NumericCast<int32_t>(nation_code));
	AppendPhone(append_info, nation_code, &ctx->Seed[C_PHNE_SD]);
	DSS_HUGE acctbal;
	RANDOM(acctbal, C_ABAL_MIN, C_ABAL_MAX, &ctx->Seed[C_ABAL_SD]);
	append_decimal(append_info, acctbal);
	AppendDistribution(append_info, &c_mseg_set, &ctx->Seed[C_MSEG_SD]);
	AppendText(append_info, C_CMNT_LEN, &ctx->Seed[C_CMNT_SD]);
	append_end_row(append_info);
}

static void GeneratePart(DSS_HUGE index, tpch_append_information &part_info, DBGenContext *ctx) {
	append_begin_row(part_info);
	append_int64(part_info, index);
	AppendPartName(part_info, ctx);
	DSS_HUGE manufacturer;
	RANDOM(manufacturer, P_MFG_MIN, P_MFG_MAX, &ctx->Seed[P_MFG_SD]);
	AppendManufacturer(part_info, manufacturer);
	DSS_HUGE brand;
	RANDOM(brand, P_BRND_MIN, P_BRND_MAX, &ctx->Seed[P_BRND_SD]);
	AppendBrand(part_info, manufacturer, brand);
	AppendDistribution(part_info, &p_types_set, &ctx->Seed[P_TYPE_SD]);
	DSS_HUGE size;
	RANDOM(size, P_SIZE_MIN, P_SIZE_MAX, &ctx->Seed[P_SIZE_SD]);
	append_int32(part_info, NumericCast<int32_t>(size));
	AppendDistribution(part_info, &p_cntr_set, &ctx->Seed[P_CNTR_SD]);
	append_decimal(part_info, RetailPrice(index));
	AppendText(part_info, P_CMNT_LEN, &ctx->Seed[P_CMNT_SD]);
	append_end_row(part_info);
}

static void GeneratePartAndPartsupp(DSS_HUGE index, tpch_append_information *info, DBGenContext *ctx) {
	auto &part_info = info[PART];
	if (part_info.IsEnabled()) {
		GeneratePart(index, part_info, ctx);
	}
	auto &partsupp_info = info[PSUPP];
	if (!partsupp_info.IsEnabled()) {
		return;
	}
	for (DSS_HUGE supp_num = 0; supp_num < SUPP_PER_PART; supp_num++) {
		append_begin_row(partsupp_info);
		append_int64(partsupp_info, index);
		append_int64(partsupp_info, PartSuppBridge(ctx, index, supp_num));
		DSS_HUGE quantity;
		RANDOM(quantity, PS_QTY_MIN, PS_QTY_MAX, &ctx->Seed[PS_QTY_SD]);
		append_int64(partsupp_info, quantity);
		DSS_HUGE supply_cost;
		RANDOM(supply_cost, PS_SCST_MIN, PS_SCST_MAX, &ctx->Seed[PS_SCST_SD]);
		append_decimal(partsupp_info, supply_cost);
		AppendText(partsupp_info, PS_CMNT_LEN, &ctx->Seed[PS_CMNT_SD]);
		append_end_row(partsupp_info);
	}
}

static void GenerateNation(DSS_HUGE index, tpch_append_information *info, DBGenContext *ctx) {
	auto &append_info = info[NATION];
	append_begin_row(append_info);
	append_int32(append_info, NumericCast<int32_t>(index - 1));
	append_string_reference(append_info, nations.list[index - 1].text,
	                        NumericCast<idx_t>(nations.list[index - 1].length));
	append_int32(append_info, NumericCast<int32_t>(nations.list[index - 1].weight));
	AppendText(append_info, N_CMNT_LEN, &ctx->Seed[N_CMNT_SD]);
	append_end_row(append_info);
}

static void GenerateRegion(DSS_HUGE index, tpch_append_information *info, DBGenContext *ctx) {
	auto &append_info = info[REGION];
	append_begin_row(append_info);
	append_int32(append_info, NumericCast<int32_t>(index - 1));
	append_string_reference(append_info, regions.list[index - 1].text,
	                        NumericCast<idx_t>(regions.list[index - 1].length));
	AppendText(append_info, R_CMNT_LEN, &ctx->Seed[R_CMNT_SD]);
	append_end_row(append_info);
}

static void gen_tbl(int tnum, DSS_HUGE count, tpch_append_information *info, DBGenContext *dbgen_ctx,
                    idx_t offset = 0) {
	for (DSS_HUGE i = offset + 1; count; count--, i++) {
		row_start(tnum, dbgen_ctx);
		switch (tnum) {
		case LINE:
		case ORDER:
		case ORDER_LINE:
			GenerateOrderLine(i, info, dbgen_ctx);
			break;
		case SUPP:
			GenerateSupplier(i, info, dbgen_ctx);
			break;
		case CUST:
			GenerateCustomer(i, info, dbgen_ctx);
			break;
		case PSUPP:
		case PART:
		case PART_PSUPP:
			GeneratePartAndPartsupp(i, info, dbgen_ctx);
			break;
		case NATION:
			GenerateNation(i, info, dbgen_ctx);
			break;
		case REGION:
			GenerateRegion(i, info, dbgen_ctx);
			break;
		}
		row_stop_h(tnum, dbgen_ctx);
	}
}

static string get_table_name(int num) {
	switch (num) {
	case PART:
		return "part";
	case PSUPP:
		return "partsupp";
	case SUPP:
		return "supplier";
	case CUST:
		return "customer";
	case ORDER:
		return "orders";
	case LINE:
		return "lineitem";
	case NATION:
		return "nation";
	case REGION:
		return "region";
	default:
		return "";
	}
}

static const char *const REGION_COLUMNS[] = {"r_regionkey", "r_name", "r_comment"};
static const char *const REGION_TYPES[] = {"INTEGER", "VARCHAR", "VARCHAR"};
static const char *const NATION_COLUMNS[] = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
static const char *const NATION_TYPES[] = {"INTEGER", "VARCHAR", "INTEGER", "VARCHAR"};
static const char *const SUPPLIER_COLUMNS[] = {"s_suppkey", "s_name",    "s_address", "s_nationkey",
                                               "s_phone",   "s_acctbal", "s_comment"};
static const char *const SUPPLIER_TYPES[] = {"BIGINT",  "VARCHAR",       "VARCHAR", "INTEGER",
                                             "VARCHAR", "DECIMAL(15,2)", "VARCHAR"};
static const char *const CUSTOMER_COLUMNS[] = {"c_custkey", "c_name",    "c_address",    "c_nationkey",
                                               "c_phone",   "c_acctbal", "c_mktsegment", "c_comment"};
static const char *const CUSTOMER_TYPES[] = {"BIGINT",  "VARCHAR",       "VARCHAR", "INTEGER",
                                             "VARCHAR", "DECIMAL(15,2)", "VARCHAR", "VARCHAR"};
static const char *const PART_COLUMNS[] = {"p_partkey", "p_name",      "p_mfgr",        "p_brand",  "p_type",
                                           "p_size",    "p_container", "p_retailprice", "p_comment"};
static const char *const PART_TYPES[] = {"BIGINT",  "VARCHAR", "VARCHAR",       "VARCHAR", "VARCHAR",
                                         "INTEGER", "VARCHAR", "DECIMAL(15,2)", "VARCHAR"};
static const char *const PARTSUPP_COLUMNS[] = {"ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost",
                                               "ps_comment"};
static const char *const PARTSUPP_TYPES[] = {"BIGINT", "BIGINT", "BIGINT", "DECIMAL(15,2)", "VARCHAR"};
static const char *const ORDERS_COLUMNS[] = {"o_orderkey",   "o_custkey",      "o_orderstatus",
                                             "o_totalprice", "o_orderdate",    "o_orderpriority",
                                             "o_clerk",      "o_shippriority", "o_comment"};
static const char *const ORDERS_TYPES[] = {"BIGINT",  "BIGINT",  "VARCHAR", "DECIMAL(15,2)", "DATE",
                                           "VARCHAR", "VARCHAR", "INTEGER", "VARCHAR"};
static const char *const LINEITEM_COLUMNS[] = {"l_orderkey",    "l_partkey",       "l_suppkey",  "l_linenumber",
                                               "l_quantity",    "l_extendedprice", "l_discount", "l_tax",
                                               "l_returnflag",  "l_linestatus",    "l_shipdate", "l_commitdate",
                                               "l_receiptdate", "l_shipinstruct",  "l_shipmode", "l_comment"};
static const char *const LINEITEM_TYPES[] = {
    "BIGINT",  "BIGINT",  "BIGINT", "BIGINT", "DECIMAL(15,2)", "DECIMAL(15,2)", "DECIMAL(15,2)", "DECIMAL(15,2)",
    "VARCHAR", "VARCHAR", "DATE",   "DATE",   "DATE",          "VARCHAR",       "VARCHAR",       "VARCHAR"};

#define TPCH_TABLE_SCHEMA(NAME, COLUMNS, TYPES)                                                                        \
	TPCHTableSchema {                                                                                                  \
		NAME, sizeof(COLUMNS) / sizeof(COLUMNS[0]), COLUMNS, TYPES                                                     \
	}

static const TPCHTableSchema TPCH_TABLE_SCHEMAS[TPCH_TABLE_COUNT] = {
    TPCH_TABLE_SCHEMA("region", REGION_COLUMNS, REGION_TYPES),
    TPCH_TABLE_SCHEMA("nation", NATION_COLUMNS, NATION_TYPES),
    TPCH_TABLE_SCHEMA("supplier", SUPPLIER_COLUMNS, SUPPLIER_TYPES),
    TPCH_TABLE_SCHEMA("customer", CUSTOMER_COLUMNS, CUSTOMER_TYPES),
    TPCH_TABLE_SCHEMA("part", PART_COLUMNS, PART_TYPES),
    TPCH_TABLE_SCHEMA("partsupp", PARTSUPP_COLUMNS, PARTSUPP_TYPES),
    TPCH_TABLE_SCHEMA("orders", ORDERS_COLUMNS, ORDERS_TYPES),
    TPCH_TABLE_SCHEMA("lineitem", LINEITEM_COLUMNS, LINEITEM_TYPES),
};

const TPCHTableSchema &GetTPCHTableSchema(idx_t index) {
	assert(index < TPCH_TABLE_COUNT);
	return TPCH_TABLE_SCHEMAS[index];
}

static void skip(int table, int children, DSS_HUGE step, DBGenContext &dbgen_ctx) {
	switch (table) {
	case CUST:
		sd_cust(children, step, &dbgen_ctx);
		break;
	case SUPP:
		sd_supp(children, step, &dbgen_ctx);
		break;
	case NATION:
		sd_nation(children, step, &dbgen_ctx);
		break;
	case REGION:
		sd_region(children, step, &dbgen_ctx);
		break;
	case ORDER_LINE:
		sd_line(children, step, &dbgen_ctx);
		sd_order(children, step, &dbgen_ctx);
		break;
	case PART_PSUPP:
		sd_part(children, step, &dbgen_ctx);
		sd_psupp(children, step, &dbgen_ctx);
		break;
	}
}

//===--------------------------------------------------------------------===//
// Work items
//===--------------------------------------------------------------------===//
//! Two row groups of the default size
static constexpr idx_t DBGEN_TARGET_CHUNK_ROWS = 2 * 122880;

struct DBGenTableRange {
	idx_t offset;
	idx_t count;
};

static idx_t GetDBGenTableRowCount(const DBGenContext &dbgen_ctx, int table_index) {
	if (table_index < NATION) {
		return static_cast<idx_t>(dbgen_ctx.tdefs[table_index].base * dbgen_ctx.scale_factor);
	}
	return static_cast<idx_t>(dbgen_ctx.tdefs[table_index].base);
}

static DBGenTableRange GetDBGenTableRange(idx_t row_count, int children, int current_step) {
	if (children <= 1 || current_step == -1) {
		return DBGenTableRange {0, row_count};
	}
	auto child_count = static_cast<idx_t>(children);
	auto step = static_cast<idx_t>(current_step);
	auto part_size = (row_count + child_count - 1) / child_count;
	auto part_offset = part_size * step;
	if (part_offset >= row_count) {
		return DBGenTableRange {part_offset, 0};
	}
	return DBGenTableRange {part_offset, MinValue<idx_t>(part_size, row_count - part_offset)};
}

//! The dbgen table that generates a table: orders and lineitem, and part and partsupp, are generated together
static int GetDBGenTable(int table_index) {
	switch (table_index) {
	case ORDER:
	case LINE:
		return ORDER_LINE;
	case PART:
	case PSUPP:
		return PART_PSUPP;
	default:
		return table_index;
	}
}

//! The most rows of the table one row of its dbgen table generates
static idx_t GetMaxRowsPerIndex(int table_index) {
	switch (table_index) {
	case LINE:
		return O_LCNT_MAX;
	case PSUPP:
		return SUPP_PER_PART;
	default:
		return 1;
	}
}

//! The average number of rows of the table one row of its dbgen table generates
static idx_t GetAverageRowsPerIndex(int table_index) {
	switch (table_index) {
	case LINE:
		return (O_LCNT_MIN + O_LCNT_MAX) / 2;
	case PSUPP:
		return SUPP_PER_PART;
	default:
		return 1;
	}
}

//! Splits a table into work items of about DBGEN_TARGET_CHUNK_ROWS rows of output each
static idx_t GetDefaultTableChildCount(const DBGenContext &dbgen_ctx, int table_index) {
	auto row_count = GetDBGenTableRowCount(dbgen_ctx, GetDBGenTable(table_index)) * GetAverageRowsPerIndex(table_index);
	if (row_count <= DBGEN_TARGET_CHUNK_ROWS) {
		return 1;
	}
	auto child_count = (row_count + DBGEN_TARGET_CHUNK_ROWS - 1) / DBGEN_TARGET_CHUNK_ROWS;
	return MinValue<idx_t>(child_count, MAX_CHILDREN);
}

//! The dbgen table index of a table of GetTPCHTableSchema
static int GetTableIndex(idx_t table) {
	auto &schema = GetTPCHTableSchema(table);
	for (int i = PART; i <= REGION; i++) {
		if (get_table_name(i) == schema.name) {
			return i;
		}
	}
	throw duckdb::cxx::InvalidInputException("Unexpected TPC-H table");
}

//===--------------------------------------------------------------------===//
// Generator
//===--------------------------------------------------------------------===//
//! The dbgen distributions and text pool are process-wide: they are shared by the generators that exist at a time
struct DBGenDistributions {
	std::mutex lock;
	idx_t users = 0;

	static DBGenDistributions &Get() {
		static DBGenDistributions distributions;
		return distributions;
	}

	void Acquire(DBGenContext &ctx) {
		std::lock_guard<std::mutex> guard(lock);
		if (users == 0) {
			load_dists(TEXT_POOL_SIZE, &ctx);
		}
		users++;
	}

	void Release() {
		std::lock_guard<std::mutex> guard(lock);
		assert(users > 0);
		if (--users == 0) {
			cleanup_dists();
		}
	}
};

struct DBGenGenerator::State {
	DBGenContext base_context;
	bool distributions_loaded = false;
	//! The work items per table of GetTPCHTableSchema
	std::vector<DBGenWorkItem> work_items[TPCH_TABLE_COUNT];

	~State() {
		if (distributions_loaded) {
			DBGenDistributions::Get().Release();
		}
	}
};

DBGenGenerator::DBGenGenerator(double flt_scale, int children, int current_step) : state(new State()) {
	// NaN compares false against every scale factor check, and reaches an out-of-range cast below
	if (std::isnan(flt_scale)) {
		throw duckdb::cxx::InvalidInputException("DBGen requires a valid scale factor.");
	}
	if (flt_scale <= 0) {
		return;
	}
	if (flt_scale > MAX_SCALE) {
		throw duckdb::cxx::InvalidInputException("DBGen does not support a scale factor exceeding " +
		                                         std::to_string(static_cast<int>(MAX_SCALE)) + ".");
	}
	bool explicit_partial_generation = children > 1 && current_step != -1;
	if (explicit_partial_generation && current_step >= children) {
		return;
	}

	auto &base_context = state->base_context;
	// all tables
	table = (1 << CUST) | (1 << SUPP) | (1 << NATION) | (1 << REGION) | (1 << PART_PSUPP) | (1 << ORDER_LINE);
	force = 0;
	insert_segments = 0;
	delete_segments = 0;
	insert_orders_segment = 0;
	insert_lineitem_segment = 0;
	delete_segment = 0;
	verbose = 0;
	set_seeds = 0;
	updates = 0;
	d_path = NULL;

	auto tdefs = base_context.tdefs;
	tdefs[PART].base = 200000;
	tdefs[PSUPP].base = 200000;
	tdefs[SUPP].base = 10000;
	tdefs[CUST].base = 150000;
	tdefs[ORDER].base = 150000 * ORDERS_PER_CUST;
	tdefs[LINE].base = 150000 * ORDERS_PER_CUST;
	tdefs[ORDER_LINE].base = 150000 * ORDERS_PER_CUST;
	tdefs[PART_PSUPP].base = 200000;
	tdefs[NATION].base = NATIONS_MAX;
	tdefs[REGION].base = NATIONS_MAX;

	if (flt_scale < MIN_SCALE) {
		auto int_scale = static_cast<int>(1000 * flt_scale);
		base_context.scale_factor = 1;
		for (int i = PART; i < REGION; i++) {
			tdefs[i].base = (DSS_HUGE)(int_scale * tdefs[i].base) / 1000;
			if (tdefs[i].base < 1) {
				tdefs[i].base = 1;
			}
		}
	} else {
		base_context.scale_factor = static_cast<long>(flt_scale);
	}

	DBGenDistributions::Get().Acquire(base_context);
	state->distributions_loaded = true;
	/* have to do this after init */
	base_context.tdefs[NATION].base = nations.count;
	base_context.tdefs[REGION].base = regions.count;

	// Keep small join dimensions on the historical batch boundaries used by plan-cost-sensitive statistics.
	idx_t default_child_count = 1;
	if (flt_scale >= 1) {
		static constexpr idx_t CHILDREN_PER_SCALE_FACTOR = 20;
		default_child_count = MinValue<idx_t>(static_cast<idx_t>(CHILDREN_PER_SCALE_FACTOR * flt_scale), MAX_CHILDREN);
	}
	for (idx_t table_idx = 0; table_idx < TPCH_TABLE_COUNT; table_idx++) {
		auto dbgen_table = GetDBGenTable(GetTableIndex(table_idx));
		idx_t table_children;
		if (explicit_partial_generation) {
			table_children = static_cast<idx_t>(children);
		} else if (dbgen_table == SUPP || dbgen_table == CUST) {
			table_children = default_child_count;
		} else {
			table_children = GetDefaultTableChildCount(base_context, GetTableIndex(table_idx));
		}
		auto row_count = GetDBGenTableRowCount(base_context, dbgen_table);
		auto first_step = explicit_partial_generation ? current_step : 0;
		auto end_step = explicit_partial_generation ? current_step + 1 : static_cast<int>(table_children);
		for (int step = first_step; step < end_step; step++) {
			auto range = GetDBGenTableRange(row_count, static_cast<int>(table_children), step);
			if (range.count == 0) {
				continue;
			}
			state->work_items[table_idx].push_back(DBGenWorkItem {static_cast<int>(table_children), step, range.count});
		}
	}
}

DBGenGenerator::~DBGenGenerator() {
}

const std::vector<DBGenWorkItem> &DBGenGenerator::WorkItems(idx_t table) const {
	assert(table < TPCH_TABLE_COUNT);
	return state->work_items[table];
}

//===--------------------------------------------------------------------===//
// Scanner
//===--------------------------------------------------------------------===//
struct DBGenScanner::State {
	DBGenContext dbgen_ctx;
	int table_index;
	int dbgen_table;
	idx_t max_rows_per_index;
	//! The next row of the dbgen table, and the rows left
	idx_t offset;
	idx_t remaining;
	tpch_append_information append_info[REGION + 1];
};

DBGenScanner::DBGenScanner(const DBGenGenerator &generator, idx_t table, const DBGenWorkItem &item)
    : state(new State()) {
	state->dbgen_ctx = generator.state->base_context;
	state->table_index = GetTableIndex(table);
	state->dbgen_table = GetDBGenTable(state->table_index);
	state->max_rows_per_index = GetMaxRowsPerIndex(state->table_index);
	auto row_count = GetDBGenTableRowCount(state->dbgen_ctx, state->dbgen_table);
	auto range = GetDBGenTableRange(row_count, item.children, item.step);
	state->offset = range.offset;
	state->remaining = range.count;
	if (item.children > 1) {
		skip(state->dbgen_table, item.children, static_cast<DSS_HUGE>(range.offset), state->dbgen_ctx);
	}
}

DBGenScanner::~DBGenScanner() {
}

idx_t DBGenScanner::Fill(DataChunk &chunk) {
	auto &info = state->append_info[state->table_index];
	info.Bind(chunk);
	idx_t generated = 0;
	while (state->remaining > 0) {
		auto fits = (info.capacity - info.row) / state->max_rows_per_index;
		auto count = MinValue<idx_t>(fits, state->remaining);
		if (count == 0) {
			break;
		}
		gen_tbl(state->dbgen_table, static_cast<DSS_HUGE>(count), state->append_info, &state->dbgen_ctx, state->offset);
		state->offset += count;
		state->remaining -= count;
		generated += count;
	}
	info.Finish();
	return generated;
}

string DBGenWrapper::GetQuery(int query) {
	if (query <= 0 || query > TPCH_QUERIES_COUNT) {
		throw duckdb::cxx::InvalidInputException("Out of range TPC-H query number " + std::to_string(query));
	}
	return TPCH_QUERIES[query - 1];
}

//! The stored Q11 carries the FRACTION substitution parameter for SF 1
static constexpr const char *TPCH_Q11_SF1_FRACTION = "0.0001000000";

string DBGenWrapper::GetQuery(int query, double sf) {
	if (std::isnan(sf) || sf <= 0 || sf > MAX_SCALE) {
		throw duckdb::cxx::InvalidInputException("TPC-H queries require a scale factor between 0 and " +
		                                         std::to_string(static_cast<int>(MAX_SCALE)));
	}
	auto result = GetQuery(query);
	if (query != 11) {
		return result;
	}
	// Q11 selects the parts whose stock value exceeds FRACTION of the total, where qgen sets FRACTION = 0.0001 / SF
	char fraction[64];
	snprintf(fraction, sizeof(fraction), "%.10f", 0.0001 / sf);
	auto pos = result.find(TPCH_Q11_SF1_FRACTION);
	assert(pos != string::npos);
	return result.replace(pos, strlen(TPCH_Q11_SF1_FRACTION), fraction);
}

string DBGenWrapper::GetAnswer(double sf, int query) {
	if (query <= 0 || query > TPCH_QUERIES_COUNT) {
		throw duckdb::cxx::InvalidInputException("Out of range TPC-H query number " + std::to_string(query));
	}
	const char *answer;
	if (sf == 0.01) {
		answer = TPCH_ANSWERS_SF0_01[query - 1];
	} else if (sf == 0.1) {
		answer = TPCH_ANSWERS_SF0_1[query - 1];
	} else if (sf == 1) {
		answer = TPCH_ANSWERS_SF1[query - 1];
	} else {
		throw duckdb::cxx::InvalidInputException("Don't have TPC-H answers for SF " + std::to_string(sf) + "!");
	}
	return answer;
}

} // namespace tpch
