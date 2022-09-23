### Minimal ICU Collation & Time Zones
This project contains an easy-to-use version of the collation/timezone part of the ICU library. The entire library is contained in two files (`icu-collate.cpp` and `icu-collate.hpp`). This includes all the data necessary to make it work. If you want to use this in your own project, simply copy those two files there.

The compiled size of the project is around 6MB. The majority of this is the inlined ICU data that is required to properly support collation for all included locales. The header `Reducing Data Size` down below can help you if you want to strip out certain locales to make the included data smaller.

### Usage
Here are a small number of snippets.

#### List Supported Collation Locales
```cpp
int32_t count;
auto locales = Collator::getAvailableLocales(count);
fprintf(stdout, "Available collation locales: [");
for(int32_t i = 0; i < count; i++) {
    if (i > 0) {
        fprintf(stdout, ", ");
    }
    if (string(locales[i].getCountry()).empty()) {
        // language only
        fprintf(stdout, "%s", locales[i].getLanguage());
    } else {
        // language + country
        fprintf(stdout, "%s_%s", locales[i].getLanguage(), locales[i].getCountry());
    }
}
fprintf(stdout, "]\n");
// output:
// Available collation locales: [af, am, ar, as, az, be, bg, bn, bo, bs, bs, ca, ceb, chr, cs, cy, da, de, de_AT, dsb, dz, ee, el, en, en_US, en_US, eo, es, et, fa, fa_AF, fi, fil, fo, fr, fr_CA, ga, gl, gu, ha, haw, he, he_IL, hi, hr, hsb, hu, hy, id, id_ID, ig, is, it, ja, ka, kk, kl, km, kn, ko, kok, ku, ky, lb, lkt, ln, lo, lt, lv, mk, ml, mn, mr, ms, mt, my, nb, nb_NO, ne, nl, nn, om, or, pa, pa, pa_IN, pl, ps, pt, ro, ru, se, si, sk, sl, smn, sq, sr, sr, sr_BA, sr_ME, sr_RS, sr, sr_BA, sr_RS, sv, sw, ta, te, th, tk, to, tr, ug, uk, ur, uz, vi, wae, wo, xh, yi, yo, zh, zh, zh_CN, zh_SG, zh, zh_HK, zh_MO, zh_TW, zu]
```

#### Create a Collator and sort a vector of strings
We can use collators to perform locale-based string ordering using the `getSortKey` method. This method takes as input a unicode string, and returns a new *byte-orderable string* based on the locale. That is, we can use standard string comparisons to order the sort keys, and we will end up with a correct ordering for that locale.

In this example, we order several German names using the German locale.

```cpp
// createSortKey creates a byte-orderable string according to the collation locale
static string createSortKey(icu::Collator &collator, const string &str) {
    // run getSortKey once to get the size
    int32_t size = collator.getSortKey(UnicodeString::fromUTF8(StringPiece(str)), nullptr, 0);
    // allocate a buffer to hold the sort key
    auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[size]);
	// generate the sort key from the string
    UnicodeString unicode_str = UnicodeString::fromUTF8(StringPiece(str));
    collator.getSortKey(unicode_str, buf.get(), size);
    return string((char*) buf.get(), size);
}

vector<string> strings = {"Göbel", "Goethe", "Goldmann", "Göthe" "Götz", "Gabel"};

UErrorCode status = U_ZERO_ERROR;
// create the collator for the german locale
auto de_collator = std::unique_ptr<icu::Collator>(Collator::createInstance(Locale("de"), status));
if (U_FAILURE(status)) {
	// something went wrong
	exit(1);
}
// order the strings using the createSortKey method
std::sort(strings.begin(), strings.end(), [&](const string &a, const string &b) {
	return createSortKey(*de_collator, a) < createSortKey(*de_collator, b);
});
for(auto &str : strings) {
	fprintf(stdout, "%s\n", str.c_str());
}
// output:
// Gabel
// Göbel
// Goethe
// Goldmann
// Göthe
// Götz

// compare this result with the (incorrect) binary ordering:
std::sort(strings.begin(), strings.end());
for(auto &str : strings) {
	fprintf(stdout, "%s\n", str.c_str());
}
// Gabel
// Goethe
// Goldmann
// Göbel
// Göthe
// Götz
```

#### Listing Available Timezones
```cpp
UErrorCode status = U_ZERO_ERROR;
auto timeZoneIds = std::unique_ptr<StringEnumeration>(TimeZone::createEnumeration());
const UnicodeString *zoneId = timeZoneIds->snext(status);
if (U_FAILURE(status)) {
	// something went wrong
	exit(1);
}

int32_t count = 0;
fprintf(stdout, "Available timezones: [");
while (zoneId != NULL && status == U_ZERO_ERROR) {
	std::string zoneIdString;
	zoneId->toUTF8String(zoneIdString);
	if (count == 0) {
		fprintf(stdout, ", ", zoneIdString.c_str());
	}
	fprintf(stdout, "%s", zoneIdString.c_str());
	count++;
	zoneId = timeZoneIds->snext(status);
}
fprintf(stdout, "]\n");
// out:
// Available timezones: [ACT, AET, AGT, ART, AST, Africa/Abidjan, Africa/Accra, Africa/Addis_Ababa, Africa/Algiers, Africa/Asmara, Africa/Asmera, Africa/Bamako, Africa/Bangui, Africa/Banjul, Africa/Bissau, Africa/Blantyre, Africa/Brazzaville, Africa/Bujumbura, Africa/Cairo, Africa/Casablanca, Africa/Ceuta, Africa/Conakry, Africa/Dakar, Africa/Dar_es_Salaam, Africa/Djibouti, Africa/Douala, Africa/El_Aaiun, Africa/Freetown, Africa/Gaborone, Africa/Harare, Africa/Johannesburg, Africa/Juba, Africa/Kampala, Africa/Khartoum, Africa/Kigali, Africa/Kinshasa, Africa/Lagos, Africa/Libreville, Africa/Lome, Africa/Luanda, Africa/Lubumbashi, Africa/Lusaka, Africa/Malabo, Africa/Maputo, Africa/Maseru, Africa/Mbabane, Africa/Mogadishu, Africa/Monrovia, Africa/Nairobi, Africa/Ndjamena, Africa/Niamey, Africa/Nouakchott, Africa/Ouagadougou, Africa/Porto-Novo, Africa/Sao_Tome, Africa/Timbuktu, Africa/Tripoli, Africa/Tunis, Africa/Windhoek, America/Adak, America/Anchorage, America/Anguilla, America/Antigua, America/Araguaina, America/Argentina/Buenos_Aires, America/Argentina/Catamarca, America/Argentina/ComodRivadavia, America/Argentina/Cordoba, America/Argentina/Jujuy, America/Argentina/La_Rioja, America/Argentina/Mendoza, America/Argentina/Rio_Gallegos, America/Argentina/Salta, America/Argentina/San_Juan, America/Argentina/San_Luis, America/Argentina/Tucuman, America/Argentina/Ushuaia, America/Aruba, America/Asuncion, America/Atikokan, America/Atka, America/Bahia, America/Bahia_Banderas, America/Barbados, America/Belem, America/Belize, America/Blanc-Sablon, America/Boa_Vista, America/Bogota, America/Boise, America/Buenos_Aires, America/Cambridge_Bay, America/Campo_Grande, America/Cancun, America/Caracas, America/Catamarca, America/Cayenne, America/Cayman, America/Chicago, America/Chihuahua, America/Coral_Harbour, America/Cordoba, America/Costa_Rica, America/Creston, America/Cuiaba, America/Curacao, America/Danmarkshavn, America/Dawson, America/Dawson_Creek, America/Denver, America/Detroit, America/Dominica, America/Edmonton, America/Eirunepe, America/El_Salvador, America/Ensenada, America/Fort_Nelson, America/Fort_Wayne, America/Fortaleza, America/Glace_Bay, America/Godthab, America/Goose_Bay, America/Grand_Turk, America/Grenada, America/Guadeloupe, America/Guatemala, America/Guayaquil, America/Guyana, America/Halifax, America/Havana, America/Hermosillo, America/Indiana/Indianapolis, America/Indiana/Knox, America/Indiana/Marengo, America/Indiana/Petersburg, America/Indiana/Tell_City, America/Indiana/Vevay, America/Indiana/Vincennes, America/Indiana/Winamac, America/Indianapolis, America/Inuvik, America/Iqaluit, America/Jamaica, America/Jujuy, America/Juneau, America/Kentucky/Louisville, America/Kentucky/Monticello, America/Knox_IN, America/Kralendijk, America/La_Paz, America/Lima, America/Los_Angeles, America/Louisville, America/Lower_Princes, America/Maceio, America/Managua, America/Manaus, America/Marigot, America/Martinique, America/Matamoros, America/Mazatlan, America/Mendoza, America/Menominee, America/Merida, America/Metlakatla, America/Mexico_City, America/Miquelon, America/Moncton, America/Monterrey, America/Montevideo, America/Montreal, America/Montserrat, America/Nassau, America/New_York, America/Nipigon, America/Nome, America/Noronha, America/North_Dakota/Beulah, America/North_Dakota/Center, America/North_Dakota/New_Salem, America/Ojinaga, America/Panama, America/Pangnirtung, America/Paramaribo, America/Phoenix, America/Port-au-Prince, America/Port_of_Spain, America/Porto_Acre, America/Porto_Velho, America/Puerto_Rico, America/Punta_Arenas, America/Rainy_River, America/Rankin_Inlet, America/Recife, America/Regina, America/Resolute, America/Rio_Branco, America/Rosario, America/Santa_Isabel, America/Santarem, America/Santiago, America/Santo_Domingo, America/Sao_Paulo, America/Scoresbysund, America/Shiprock, America/Sitka, America/St_Barthelemy, America/St_Johns, America/St_Kitts, America/St_Lucia, America/St_Thomas, America/St_Vincent, America/Swift_Current, America/Tegucigalpa, America/Thule, America/Thunder_Bay, America/Tijuana, America/Toronto, America/Tortola, America/Vancouver, America/Virgin, America/Whitehorse, America/Winnipeg, America/Yakutat, America/Yellowknife, Antarctica/Casey, Antarctica/Davis, Antarctica/DumontDUrville, Antarctica/Macquarie, Antarctica/Mawson, Antarctica/McMurdo, Antarctica/Palmer, Antarctica/Rothera, Antarctica/South_Pole, Antarctica/Syowa, Antarctica/Troll, Antarctica/Vostok, Arctic/Longyearbyen, Asia/Aden, Asia/Almaty, Asia/Amman, Asia/Anadyr, Asia/Aqtau, Asia/Aqtobe, Asia/Ashgabat, Asia/Ashkhabad, Asia/Atyrau, Asia/Baghdad, Asia/Bahrain, Asia/Baku, Asia/Bangkok, Asia/Barnaul, Asia/Beirut, Asia/Bishkek, Asia/Brunei, Asia/Calcutta, Asia/Chita, Asia/Choibalsan, Asia/Chongqing, Asia/Chungking, Asia/Colombo, Asia/Dacca, Asia/Damascus, Asia/Dhaka, Asia/Dili, Asia/Dubai, Asia/Dushanbe, Asia/Famagusta, Asia/Gaza, Asia/Harbin, Asia/Hebron, Asia/Ho_Chi_Minh, Asia/Hong_Kong, Asia/Hovd, Asia/Irkutsk, Asia/Istanbul, Asia/Jakarta, Asia/Jayapura, Asia/Jerusalem, Asia/Kabul, Asia/Kamchatka, Asia/Karachi, Asia/Kashgar, Asia/Kathmandu, Asia/Katmandu, Asia/Khandyga, Asia/Kolkata, Asia/Krasnoyarsk, Asia/Kuala_Lumpur, Asia/Kuching, Asia/Kuwait, Asia/Macao, Asia/Macau, Asia/Magadan, Asia/Makassar, Asia/Manila, Asia/Muscat, Asia/Nicosia, Asia/Novokuznetsk, Asia/Novosibirsk, Asia/Omsk, Asia/Oral, Asia/Phnom_Penh, Asia/Pontianak, Asia/Pyongyang, Asia/Qatar, Asia/Qostanay, Asia/Qyzylorda, Asia/Rangoon, Asia/Riyadh, Asia/Saigon, Asia/Sakhalin, Asia/Samarkand, Asia/Seoul, Asia/Shanghai, Asia/Singapore, Asia/Srednekolymsk, Asia/Taipei, Asia/Tashkent, Asia/Tbilisi, Asia/Tehran, Asia/Tel_Aviv, Asia/Thimbu, Asia/Thimphu, Asia/Tokyo, Asia/Tomsk, Asia/Ujung_Pandang, Asia/Ulaanbaatar, Asia/Ulan_Bator, Asia/Urumqi, Asia/Ust-Nera, Asia/Vientiane, Asia/Vladivostok, Asia/Yakutsk, Asia/Yangon, Asia/Yekaterinburg, Asia/Yerevan, Atlantic/Azores, Atlantic/Bermuda, Atlantic/Canary, Atlantic/Cape_Verde, Atlantic/Faeroe, Atlantic/Faroe, Atlantic/Jan_Mayen, Atlantic/Madeira, Atlantic/Reykjavik, Atlantic/South_Georgia, Atlantic/St_Helena, Atlantic/Stanley, Australia/ACT, Australia/Adelaide, Australia/Brisbane, Australia/Broken_Hill, Australia/Canberra, Australia/Currie, Australia/Darwin, Australia/Eucla, Australia/Hobart, Australia/LHI, Australia/Lindeman, Australia/Lord_Howe, Australia/Melbourne, Australia/NSW, Australia/North, Australia/Perth, Australia/Queensland, Australia/South, Australia/Sydney, Australia/Tasmania, Australia/Victoria, Australia/West, Australia/Yancowinna, BET, BST, Brazil/Acre, Brazil/DeNoronha, Brazil/East, Brazil/West, CAT, CET, CNT, CST, CST6CDT, CTT, Canada/Atlantic, Canada/Central, Canada/East-Saskatchewan, Canada/Eastern, Canada/Mountain, Canada/Newfoundland, Canada/Pacific, Canada/Saskatchewan, Canada/Yukon, Chile/Continental, Chile/EasterIsland, Cuba, EAT, ECT, EET, EST, EST5EDT, Egypt, Eire, Etc/GMT, Etc/GMT+0, Etc/GMT+1, Etc/GMT+10, Etc/GMT+11, Etc/GMT+12, Etc/GMT+2, Etc/GMT+3, Etc/GMT+4, Etc/GMT+5, Etc/GMT+6, Etc/GMT+7, Etc/GMT+8, Etc/GMT+9, Etc/GMT-0, Etc/GMT-1, Etc/GMT-10, Etc/GMT-11, Etc/GMT-12, Etc/GMT-13, Etc/GMT-14, Etc/GMT-2, Etc/GMT-3, Etc/GMT-4, Etc/GMT-5, Etc/GMT-6, Etc/GMT-7, Etc/GMT-8, Etc/GMT-9, Etc/GMT0, Etc/Greenwich, Etc/UCT, Etc/UTC, Etc/Universal, Etc/Zulu, Europe/Amsterdam, Europe/Andorra, Europe/Astrakhan, Europe/Athens, Europe/Belfast, Europe/Belgrade, Europe/Berlin, Europe/Bratislava, Europe/Brussels, Europe/Bucharest, Europe/Budapest, Europe/Busingen, Europe/Chisinau, Europe/Copenhagen, Europe/Dublin, Europe/Gibraltar, Europe/Guernsey, Europe/Helsinki, Europe/Isle_of_Man, Europe/Istanbul, Europe/Jersey, Europe/Kaliningrad, Europe/Kiev, Europe/Kirov, Europe/Lisbon, Europe/Ljubljana, Europe/London, Europe/Luxembourg, Europe/Madrid, Europe/Malta, Europe/Mariehamn, Europe/Minsk, Europe/Monaco, Europe/Moscow, Europe/Nicosia, Europe/Oslo, Europe/Paris, Europe/Podgorica, Europe/Prague, Europe/Riga, Europe/Rome, Europe/Samara, Europe/San_Marino, Europe/Sarajevo, Europe/Saratov, Europe/Simferopol, Europe/Skopje, Europe/Sofia, Europe/Stockholm, Europe/Tallinn, Europe/Tirane, Europe/Tiraspol, Europe/Ulyanovsk, Europe/Uzhgorod, Europe/Vaduz, Europe/Vatican, Europe/Vienna, Europe/Vilnius, Europe/Volgograd, Europe/Warsaw, Europe/Zagreb, Europe/Zaporozhye, Europe/Zurich, Factory, GB, GB-Eire, GMT, GMT+0, GMT-0, GMT0, Greenwich, HST, Hongkong, IET, IST, Iceland, Indian/Antananarivo, Indian/Chagos, Indian/Christmas, Indian/Cocos, Indian/Comoro, Indian/Kerguelen, Indian/Mahe, Indian/Maldives, Indian/Mauritius, Indian/Mayotte, Indian/Reunion, Iran, Israel, JST, Jamaica, Japan, Kwajalein, Libya, MET, MIT, MST, MST7MDT, Mexico/BajaNorte, Mexico/BajaSur, Mexico/General, NET, NST, NZ, NZ-CHAT, Navajo, PLT, PNT, PRC, PRT, PST, PST8PDT, Pacific/Apia, Pacific/Auckland, Pacific/Bougainville, Pacific/Chatham, Pacific/Chuuk, Pacific/Easter, Pacific/Efate, Pacific/Enderbury, Pacific/Fakaofo, Pacific/Fiji, Pacific/Funafuti, Pacific/Galapagos, Pacific/Gambier, Pacific/Guadalcanal, Pacific/Guam, Pacific/Honolulu, Pacific/Johnston, Pacific/Kiritimati, Pacific/Kosrae, Pacific/Kwajalein, Pacific/Majuro, Pacific/Marquesas, Pacific/Midway, Pacific/Nauru, Pacific/Niue, Pacific/Norfolk, Pacific/Noumea, Pacific/Pago_Pago, Pacific/Palau, Pacific/Pitcairn, Pacific/Pohnpei, Pacific/Ponape, Pacific/Port_Moresby, Pacific/Rarotonga, Pacific/Saipan, Pacific/Samoa, Pacific/Tahiti, Pacific/Tarawa, Pacific/Tongatapu, Pacific/Truk, Pacific/Wake, Pacific/Wallis, Pacific/Yap, Poland, Portugal, ROC, ROK, SST, Singapore, SystemV/AST4, SystemV/AST4ADT, SystemV/CST6, SystemV/CST6CDT, SystemV/EST5, SystemV/EST5EDT, SystemV/HST10, SystemV/MST7, SystemV/MST7MDT, SystemV/PST8, SystemV/PST8PDT, SystemV/YST9, SystemV/YST9YDT, Turkey, UCT, US/Alaska, US/Aleutian, US/Arizona, US/Central, US/East-Indiana, US/Eastern, US/Hawaii, US/Indiana-Starke, US/Michigan, US/Mountain, US/Pacific, US/Pacific-New, US/Samoa, UTC, Universal, VST, W-SU, WET, Zulu]
```

#### Get TimeZone Offset
```cpp
UErrorCode success = U_ZERO_ERROR;
auto tz_us = CreateTimezone("America/Los_Angeles");

auto calendar = std::unique_ptr<Calendar>(Calendar::createInstance(success));
if (U_FAILURE(success)) {
	// something went wrong
	exit(1);
}

auto curDate = calendar->getNow();

// Use getOffset to get the stdOffset and dstOffset for the given time
int32_t stdOffset, dstOffset;
tz_us->getOffset(curDate, false, stdOffset, dstOffset, success);
if (U_FAILURE(success)) {
	// something went wrong
	exit(1);
}
printf("%s: %d\n","US Time Zone STD offset",stdOffset/(1000*60*60));
printf("%s: %d\n","US Time Zone DST offset",dstOffset/(1000*60*60));
printf("%s: ", "US date/time is in daylight savings time");
printf("%s\n", (calendar->inDaylightTime(success))?"Yes":"No");

// out:
// US Time Zone STD offset: -8
// US Time Zone DST offset: 1
// US date/time is in daylight savings time: Yes
```

### Reducing Data Size
The inlined data is present in `data/icudt66l.dat`. It is compiled from the ICU library as described [here](https://github.com/unicode-org/icu/blob/master/docs/userguide/icu_data/buildtool.md), with the following filters set:

```json
filters.json
{
    "featureFilters": {
        "brkitr_rules" : "exclude",
        "brkitr_dictionaries" : "exclude",
        "brkitr_tree"   : "exclude",
        "conversion_mappings"   : "exclude",
        "confusables"   : "exclude",
        "curr_supplemental" : "exclude",
        "curr_tree" : "exclude",
        "lang_tree" : "exclude",
        "normalization" : "exclude",
        "region_tree"   : "exclude",
        "rbnf_tree" : "exclude",
        "stringprep"    : "exclude",
        "zone_tree" : "exclude",
        "translit"  : "exclude",
        "unames"    : "exclude",
        "ulayout"   : "exclude",
        "unit_tree" : "exclude",
        "cnvalias" : "exclude",
        "locales_tree"  : "exclude"

    }
}
```

The following command can then be run in the ICU source directory to create a packaged data file:

```shell
ICU_DATA_FILTER_FILE=filters.json ./runConfigureICU Linux --with-data-packaging=archive
make
# result data file: data/out/icudt66l.dat
```

In the default configuration, only `misc`, `"coll_tree"` and `"coll_ucadata"` are included, which are the parts required for collation and basic time zone support. However, all locales are included. The size of the data can be significantly reduced by stripping certain locales. The linked page describes how to do that. After re-packaging the data, you can run `scripts/inline-data.py` to inline a smaller segment of the data.

