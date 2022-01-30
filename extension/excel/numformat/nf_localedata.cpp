#include "nf_calendar.h"
#include "nf_localedata.h"


namespace duckdb_excel {

LocaleData::LocaleData() {
	LoadLocaleData();

	m_cur_locale_id = LocaleId_en_US;
	m_eval_date_format = NF_EVALDATEFORMAT_INTL;

	m_calendar_ptr = new Calendar(this);
	m_num_format_scan_ptr = new ImpSvNumberformatScan(this);
}

LocaleData::~LocaleData() {
	if (m_calendar_ptr) {
		delete m_calendar_ptr;
	}
	if (m_num_format_scan_ptr) {
		delete m_num_format_scan_ptr;
	}
}

void LocaleData::LoadLocaleData() {
	// en-US
	// --------------------------------------------------------------------------------------------------------------------------------
	m_locale_list[LocaleId_en_US].seperators.date = L"/";
	m_locale_list[LocaleId_en_US].seperators.thousand = L",";
	m_locale_list[LocaleId_en_US].seperators.decimal = L".";
	m_locale_list[LocaleId_en_US].seperators.time = L":";
	m_locale_list[LocaleId_en_US].seperators.time100sec = L".";
	m_locale_list[LocaleId_en_US].seperators.list = L";";
	m_locale_list[LocaleId_en_US].seperators.longdatedayofweek = L", ";
	m_locale_list[LocaleId_en_US].seperators.longdateday = L", ";
	m_locale_list[LocaleId_en_US].seperators.longdatemonth = L" ";
	m_locale_list[LocaleId_en_US].seperators.longdateyear = L" ";

	m_locale_list[LocaleId_en_US].digit_grouping.clear();
	m_locale_list[LocaleId_en_US].digit_grouping.push_back(3);
	m_locale_list[LocaleId_en_US].digit_grouping.push_back(0);

	std::wstring str = L"USD";
	m_locale_list[LocaleId_en_US].setCurrentCurrencyId(str);
	m_locale_list[LocaleId_en_US].currency[L"USD"].currency_symbol = L"$";
	m_locale_list[LocaleId_en_US].currency[L"USD"].bank_symbol = L"USD";
	m_locale_list[LocaleId_en_US].currency[L"USD"].currency_name = L"US Dollar";
	m_locale_list[LocaleId_en_US].currency[L"USD"].decimal_places = 2;

	m_locale_list[LocaleId_en_US].time_am = L"AM";
	m_locale_list[LocaleId_en_US].time_pm = L"PM";

	m_locale_list[LocaleId_en_US].date_format = MDY;

	m_locale_list[LocaleId_en_US].months_of_year.clear();
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"jan", L"Jan", L"January"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"feb", L"Feb", L"February"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"mar", L"Mar", L"March"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"apr", L"Apr", L"April"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"may", L"May", L"May"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"jun", L"Jun", L"June"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"jul", L"Jul", L"July"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"aug", L"Aug", L"August"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"sep", L"Sep", L"September"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"oct", L"Oct", L"October"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"nov", L"Nov", L"November"});
	m_locale_list[LocaleId_en_US].months_of_year.push_back({L"dec", L"Dec", L"December"});

	m_locale_list[LocaleId_en_US].days_of_week.clear();
	m_locale_list[LocaleId_en_US].days_of_week.push_back({L"sun", L"Sun", L"Sunday"});
	m_locale_list[LocaleId_en_US].days_of_week.push_back({L"mon", L"Mon", L"Monday"});
	m_locale_list[LocaleId_en_US].days_of_week.push_back({L"tue", L"Tue", L"Tuesday"});
	m_locale_list[LocaleId_en_US].days_of_week.push_back({L"wed", L"Wed", L"Wednesday"});
	m_locale_list[LocaleId_en_US].days_of_week.push_back({L"thu", L"Thu", L"Thursday"});
	m_locale_list[LocaleId_en_US].days_of_week.push_back({L"fri", L"Fri", L"Friday"});
	m_locale_list[LocaleId_en_US].days_of_week.push_back({L"sat", L"Sat", L"Saturday"});

	m_locale_list[LocaleId_en_US].reserved_words[0] = L"true";
	m_locale_list[LocaleId_en_US].reserved_words[1] = L"false";
	m_locale_list[LocaleId_en_US].reserved_words[2] = L"1st quarter";
	m_locale_list[LocaleId_en_US].reserved_words[3] = L"2nd quarter";
	m_locale_list[LocaleId_en_US].reserved_words[4] = L"3rd quarter";
	m_locale_list[LocaleId_en_US].reserved_words[5] = L"4th quarter";
	m_locale_list[LocaleId_en_US].reserved_words[6] = L"above";
	m_locale_list[LocaleId_en_US].reserved_words[7] = L"below";
	m_locale_list[LocaleId_en_US].reserved_words[8] = L"Q1";
	m_locale_list[LocaleId_en_US].reserved_words[9] = L"Q2";
	m_locale_list[LocaleId_en_US].reserved_words[10] = L"Q3";
	m_locale_list[LocaleId_en_US].reserved_words[11] = L"Q4";

	m_locale_list[LocaleId_en_US].format_codes.fixed_format_key1 = L"General";
	m_locale_list[LocaleId_en_US].format_codes.scientific_format_key1 = L"0.00E+000";
	m_locale_list[LocaleId_en_US].format_codes.percent_format_key1 = L"0%";
	m_locale_list[LocaleId_en_US].format_codes.currency_format_key1 = L"[CURRENCY]#,##0;-[CURRENCY]#,##0";
	m_locale_list[LocaleId_en_US].format_codes.date_format_key1 = L"M/D/YY";
	m_locale_list[LocaleId_en_US].format_codes.date_format_key9 = L"NNNNMMMM DD, YYYY";
	m_locale_list[LocaleId_en_US].format_codes.datetime_format_key1 = L"MM/DD/YY HH:MM AM/PM";

	m_locale_list[LocaleId_en_US].eras.clear();
	m_locale_list[LocaleId_en_US].eras.push_back({L"BC", L"BC"});
	m_locale_list[LocaleId_en_US].eras.push_back({L"AD", L"AD"});

	// fr-FR
	// --------------------------------------------------------------------------------------------------------------------------------
	m_locale_list[LocaleId_fr_FR].seperators.date = L"/";
	m_locale_list[LocaleId_fr_FR].seperators.thousand = L" ";
	m_locale_list[LocaleId_fr_FR].seperators.decimal = L",";
	m_locale_list[LocaleId_fr_FR].seperators.time = L":";
	m_locale_list[LocaleId_fr_FR].seperators.time100sec = L",";
	m_locale_list[LocaleId_fr_FR].seperators.list = L";";
	m_locale_list[LocaleId_fr_FR].seperators.longdatedayofweek = L" ";
	m_locale_list[LocaleId_fr_FR].seperators.longdateday = L" ";
	m_locale_list[LocaleId_fr_FR].seperators.longdatemonth = L" ";
	m_locale_list[LocaleId_fr_FR].seperators.longdateyear = L" ";

	m_locale_list[LocaleId_fr_FR].digit_grouping.clear();
	m_locale_list[LocaleId_fr_FR].digit_grouping.push_back(3);
	m_locale_list[LocaleId_fr_FR].digit_grouping.push_back(0);

	str = L"EUR";
	m_locale_list[LocaleId_fr_FR].setCurrentCurrencyId(str);
	m_locale_list[LocaleId_fr_FR].currency[L"EUR"].currency_symbol = L"€";
	m_locale_list[LocaleId_fr_FR].currency[L"EUR"].bank_symbol = L"EUR";
	m_locale_list[LocaleId_fr_FR].currency[L"EUR"].currency_name = L"euro";
	m_locale_list[LocaleId_fr_FR].currency[L"EUR"].decimal_places = 2;
	m_locale_list[LocaleId_fr_FR].currency[L"FRF"].currency_symbol = L"F";
	m_locale_list[LocaleId_fr_FR].currency[L"FRF"].bank_symbol = L"FRF";
	m_locale_list[LocaleId_fr_FR].currency[L"FRF"].currency_name = L"franc français";
	m_locale_list[LocaleId_fr_FR].currency[L"FRF"].decimal_places = 2;

	m_locale_list[LocaleId_fr_FR].time_am = L"AM";
	m_locale_list[LocaleId_fr_FR].time_pm = L"PM";

	m_locale_list[LocaleId_fr_FR].date_format = DMY;

	m_locale_list[LocaleId_fr_FR].months_of_year.clear();
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"jan", L"janv.", L"janvier"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"feb", L"févr.", L"février"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"mar", L"mars", L"mars"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"apr", L"avr.", L"avril"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"may", L"mai", L"mai"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"jun", L"juin", L"juin"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"jul", L"juil.", L"juillet"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"aug", L"août", L"août"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"sep", L"sept.", L"septembre"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"oct", L"oct.", L"octobre"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"nov", L"nov.", L"novembre"});
	m_locale_list[LocaleId_fr_FR].months_of_year.push_back({L"dec", L"déc.", L"décembre"});

	m_locale_list[LocaleId_fr_FR].days_of_week.clear();
	m_locale_list[LocaleId_fr_FR].days_of_week.push_back({L"sun", L"dim.", L"dimanche"});
	m_locale_list[LocaleId_fr_FR].days_of_week.push_back({L"mon", L"lun.", L"lundi"});
	m_locale_list[LocaleId_fr_FR].days_of_week.push_back({L"tue", L"mar.", L"mardi"});
	m_locale_list[LocaleId_fr_FR].days_of_week.push_back({L"wed", L"mer.", L"mercredi"});
	m_locale_list[LocaleId_fr_FR].days_of_week.push_back({L"thu", L"jeu.", L"jeudi"});
	m_locale_list[LocaleId_fr_FR].days_of_week.push_back({L"fri", L"ven.", L"vendredi"});
	m_locale_list[LocaleId_fr_FR].days_of_week.push_back({L"sat", L"sam.", L"samedi"});

	m_locale_list[LocaleId_fr_FR].reserved_words[0] = L"vrai";
	m_locale_list[LocaleId_fr_FR].reserved_words[1] = L"faux";
	m_locale_list[LocaleId_fr_FR].reserved_words[2] = L"1er trimestre";
	m_locale_list[LocaleId_fr_FR].reserved_words[3] = L"2e trimestre";
	m_locale_list[LocaleId_fr_FR].reserved_words[4] = L"3e trimestre";
	m_locale_list[LocaleId_fr_FR].reserved_words[5] = L"4e trimestre";
	m_locale_list[LocaleId_fr_FR].reserved_words[6] = L"supra";
	m_locale_list[LocaleId_fr_FR].reserved_words[7] = L"infra";
	m_locale_list[LocaleId_fr_FR].reserved_words[8] = L"T1";
	m_locale_list[LocaleId_fr_FR].reserved_words[9] = L"T2";
	m_locale_list[LocaleId_fr_FR].reserved_words[10] = L"T3";
	m_locale_list[LocaleId_fr_FR].reserved_words[11] = L"T4";

	m_locale_list[LocaleId_fr_FR].format_codes.fixed_format_key1 = L"Standard";
	m_locale_list[LocaleId_fr_FR].format_codes.scientific_format_key1 = L"0,00E+000";
	m_locale_list[LocaleId_fr_FR].format_codes.percent_format_key1 = L"0%";
	m_locale_list[LocaleId_fr_FR].format_codes.currency_format_key1 = L"# ##0 [CURRENCY];-# ##0 [CURRENCY]";
	m_locale_list[LocaleId_fr_FR].format_codes.date_format_key1 = L"JJ/MM/AAAA";
	m_locale_list[LocaleId_fr_FR].format_codes.date_format_key9 = L"NNNNJ MMMM AAAA";
	m_locale_list[LocaleId_fr_FR].format_codes.datetime_format_key1 = L"JJ/MM/AA HH:MM";

	m_locale_list[LocaleId_fr_FR].eras.clear();
	m_locale_list[LocaleId_fr_FR].eras.push_back({L"av. J.-C.", L"av. J.-C."});
	m_locale_list[LocaleId_fr_FR].eras.push_back({L"apr. J.-C.", L"ap. J.-C."});
}

}	// namespace duckdb_excel