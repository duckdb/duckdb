#ifndef _NF_LOCALEDATA_H
#define _NF_LOCALEDATA_H

#include <string>
#include "duckdb/common/vector.hpp"
#include <map>
#include "nf_calendar.h"
#include "nf_zformat.h"


namespace duckdb_excel {

#define MLD_ABBRVFULLNAME_CHECK_INDEX_RANGE(A, B)                                                                      \
	if (B < 0 || B >= (sal_Int32)m_locale_list[m_cur_locale_id].A.size())                                              \
	return L""
#define MLD_ABBRVFULLNAME_GET_FULL_NAME(A, B)  m_locale_list[m_cur_locale_id].A[B].full_name
#define MLD_ABBRVFULLNAME_GET_ABBRV_NAME(A, B) m_locale_list[m_cur_locale_id].A[B].abbrv_name
#define MLD_ABBRVFULLNAME_GET_SIZE(A)          m_locale_list[m_cur_locale_id].A.size()

struct SeparatorInfo {
	std::wstring date;
	std::wstring thousand;
	std::wstring decimal;
	std::wstring time;
	std::wstring time100sec;
	std::wstring list;
	std::wstring longdatedayofweek;
	std::wstring longdateday;
	std::wstring longdatemonth;
	std::wstring longdateyear;
};

struct CurrencyInfo {
	std::wstring currency_symbol; // "$"
	std::wstring bank_symbol;     // "USD"
	std::wstring currency_name;   // "US Dollar"
	int32_t decimal_places;       // 2
};

struct AbbrvFullNameInfo {
	std::wstring id;
	std::wstring abbrv_name;
	std::wstring full_name;
};

struct FormatCodeInfo {
	std::wstring fixed_format_key1;
	std::wstring scientific_format_key1;
	std::wstring percent_format_key1;
	std::wstring currency_format_key1;
	std::wstring date_format_key1;
	std::wstring date_format_key9;
	std::wstring datetime_format_key1;
};

struct LocaleInfo {
	SeparatorInfo seperators;
	// std::wstring		num_thousand_separator;	// ','
	duckdb::vector<int32_t> digit_grouping; // {3, 0} or {3, 2, 0} (India and Bhutan)
	std::wstring cur_currency_id;        // "USD"
	std::map<std::wstring, CurrencyInfo> currency;
	std::wstring time_am;
	std::wstring time_pm;
	DateFormat date_format;
	duckdb::vector<AbbrvFullNameInfo> months_of_year;
	duckdb::vector<AbbrvFullNameInfo> days_of_week;
	std::wstring reserved_words[reservedWords::COUNT];
	FormatCodeInfo format_codes;
	duckdb::vector<AbbrvFullNameInfo> eras;

public:
	std::wstring &getCurrentCurrencyId() {
		return cur_currency_id;
	}
	void setCurrentCurrencyId(std::wstring &currency_id) {
		cur_currency_id = currency_id;
	}
};

class Calendar;
class ImpSvNumberInputScan;
class ImpSvNumberformatScan;

class LocaleData {
public:
	LocaleData();
	~LocaleData();

	void LoadLocaleData();
	LocaleIndentifier GetLocaleId() {
		return m_cur_locale_id;
	}
	void SetLocaleId(LocaleIndentifier locale_id) {
		m_cur_locale_id = locale_id;
	}
	Calendar *GetCalendar() {
		return m_calendar_ptr;
	}

	NfEvalDateFormat GetEvalDateFormat() {
		return m_eval_date_format;
	}
	void SetEvalDateFormat(NfEvalDateFormat eval_date_format) {
		m_eval_date_format = eval_date_format;
	}

	std::wstring &GetDateSep() {
		return m_locale_list[m_cur_locale_id].seperators.date;
	}
	std::wstring &GetNumThousandSep() {
		return m_locale_list[m_cur_locale_id].seperators.thousand;
	}
	std::wstring &GetNumDecimalSep() {
		return m_locale_list[m_cur_locale_id].seperators.decimal;
	}
	std::wstring &getTimeSep() {
		return m_locale_list[m_cur_locale_id].seperators.time;
	}
	std::wstring &getTime100SecSep() {
		return m_locale_list[m_cur_locale_id].seperators.time100sec;
	}
	std::wstring &getLongDateMonthSep() {
		return m_locale_list[m_cur_locale_id].seperators.longdatemonth;
	}
	std::wstring &getLongDateDayOfWeekSep() {
		return m_locale_list[m_cur_locale_id].seperators.longdatedayofweek;
	}
	std::wstring &getLongDateDaySep() {
		return m_locale_list[m_cur_locale_id].seperators.longdateday;
	}

	duckdb::vector<int32_t> &getDigitGrouping() {
		return m_locale_list[m_cur_locale_id].digit_grouping;
	}

	ImpSvNumberformatScan *GetFormatScanner() {
		return m_num_format_scan_ptr;
	}

	std::wstring &getCurrSymbol() {
		return m_locale_list[m_cur_locale_id]
		    .currency[m_locale_list[m_cur_locale_id].getCurrentCurrencyId()]
		    .currency_symbol;
	}
	std::wstring &getCurrBankSymbol() {
		return m_locale_list[m_cur_locale_id]
		    .currency[m_locale_list[m_cur_locale_id].getCurrentCurrencyId()]
		    .bank_symbol;
	}

	std::wstring &getTimeAM() {
		return m_locale_list[m_cur_locale_id].time_am;
	}
	std::wstring &getTimePM() {
		return m_locale_list[m_cur_locale_id].time_pm;
	}

	DateFormat getDateFormat() {
		return m_locale_list[m_cur_locale_id].date_format;
	}
	DateFormat getLongDateFormat() {
		return m_locale_list[m_cur_locale_id].date_format;
	}
	int32_t getMonthsOfYearSize() {
		return MLD_ABBRVFULLNAME_GET_SIZE(months_of_year);
	}
	std::wstring getMonthsOfYearFullName(int16_t idx) {
		MLD_ABBRVFULLNAME_CHECK_INDEX_RANGE(months_of_year, idx);
		return MLD_ABBRVFULLNAME_GET_FULL_NAME(months_of_year, idx);
	}
	std::wstring getMonthsOfYearAbbrvName(int16_t idx) {
		MLD_ABBRVFULLNAME_CHECK_INDEX_RANGE(months_of_year, idx);
		return MLD_ABBRVFULLNAME_GET_ABBRV_NAME(months_of_year, idx);
	}
	int32_t getDayOfWeekSize() {
		return MLD_ABBRVFULLNAME_GET_SIZE(days_of_week);
	}
	std::wstring getDayOfWeekFullName(int16_t idx) {
		MLD_ABBRVFULLNAME_CHECK_INDEX_RANGE(days_of_week, idx);
		return MLD_ABBRVFULLNAME_GET_FULL_NAME(days_of_week, idx);
	}
	std::wstring getDayOfWeekAbbrvName(int16_t idx) {
		MLD_ABBRVFULLNAME_CHECK_INDEX_RANGE(days_of_week, idx);
		return MLD_ABBRVFULLNAME_GET_ABBRV_NAME(days_of_week, idx);
	}

	std::wstring getReservedWord(int16_t idx) {
		if (idx < 0 || idx >= reservedWords::COUNT)
			return L"";
		return m_locale_list[m_cur_locale_id].reserved_words[idx];
	}

	std::wstring &getFormatCodeNumberStandard() {
		return m_locale_list[m_cur_locale_id].format_codes.fixed_format_key1;
	}

	int32_t getEraSize() {
		return MLD_ABBRVFULLNAME_GET_SIZE(eras);
	}
	std::wstring getEraFullName(int16_t idx) {
		MLD_ABBRVFULLNAME_CHECK_INDEX_RANGE(eras, idx);
		return MLD_ABBRVFULLNAME_GET_FULL_NAME(eras, idx);
	}
	std::wstring getEraAbbrvName(int16_t idx) {
		MLD_ABBRVFULLNAME_CHECK_INDEX_RANGE(eras, idx);
		return MLD_ABBRVFULLNAME_GET_ABBRV_NAME(eras, idx);
	}

private:
	LocaleIndentifier m_cur_locale_id;
	LocaleInfo m_locale_list[LocaleIndentifierCount];
	NfEvalDateFormat m_eval_date_format;
	Calendar *m_calendar_ptr;

	ImpSvNumberformatScan *m_num_format_scan_ptr;
};

}   // namespace duckdb_excel

#endif // _NF_LOCALEDATA_H
