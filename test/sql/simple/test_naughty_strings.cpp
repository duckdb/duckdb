#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test naughty strings", "[naughtystrings]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT '#	Reserved Strings'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Reserved Strings"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which may be used elsewhere in code'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which may be used elsewhere in code"}));

	result = con.Query("SELECT 'undefined'");
	REQUIRE(CHECK_COLUMN(result, 0, {"undefined"}));

	result = con.Query("SELECT 'undef'");
	REQUIRE(CHECK_COLUMN(result, 0, {"undef"}));

	result = con.Query("SELECT 'null'");
	REQUIRE(CHECK_COLUMN(result, 0, {"null"}));

	result = con.Query("SELECT 'NULL'");
	REQUIRE(CHECK_COLUMN(result, 0, {"NULL"}));

	result = con.Query("SELECT '(null)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"(null)"}));

	result = con.Query("SELECT 'nil'");
	REQUIRE(CHECK_COLUMN(result, 0, {"nil"}));

	result = con.Query("SELECT 'NIL'");
	REQUIRE(CHECK_COLUMN(result, 0, {"NIL"}));

	result = con.Query("SELECT 'true'");
	REQUIRE(CHECK_COLUMN(result, 0, {"true"}));

	result = con.Query("SELECT 'false'");
	REQUIRE(CHECK_COLUMN(result, 0, {"false"}));

	result = con.Query("SELECT 'True'");
	REQUIRE(CHECK_COLUMN(result, 0, {"True"}));

	result = con.Query("SELECT 'False'");
	REQUIRE(CHECK_COLUMN(result, 0, {"False"}));

	result = con.Query("SELECT 'TRUE'");
	REQUIRE(CHECK_COLUMN(result, 0, {"TRUE"}));

	result = con.Query("SELECT 'FALSE'");
	REQUIRE(CHECK_COLUMN(result, 0, {"FALSE"}));

	result = con.Query("SELECT 'None'");
	REQUIRE(CHECK_COLUMN(result, 0, {"None"}));

	result = con.Query("SELECT 'hasOwnProperty'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hasOwnProperty"}));

	result = con.Query("SELECT 'then'");
	REQUIRE(CHECK_COLUMN(result, 0, {"then"}));

	result = con.Query("SELECT '\\'");
	REQUIRE(CHECK_COLUMN(result, 0, {"\\"}));

	result = con.Query("SELECT '\\\\'");
	REQUIRE(CHECK_COLUMN(result, 0, {"\\\\"}));

	result = con.Query("SELECT '#	Numeric Strings'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Numeric Strings"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which can be interpreted as numeric'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which can be interpreted as numeric"}));

	result = con.Query("SELECT '0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0"}));

	result = con.Query("SELECT '1'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1"}));

	result = con.Query("SELECT '1.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1.00"}));

	result = con.Query("SELECT '$1.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"$1.00"}));

	result = con.Query("SELECT '1/2'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1/2"}));

	result = con.Query("SELECT '1E2'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1E2"}));

	result = con.Query("SELECT '1E02'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1E02"}));

	result = con.Query("SELECT '1E+02'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1E+02"}));

	result = con.Query("SELECT '-1'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-1"}));

	result = con.Query("SELECT '-1.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-1.00"}));

	result = con.Query("SELECT '-$1.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-$1.00"}));

	result = con.Query("SELECT '-1/2'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-1/2"}));

	result = con.Query("SELECT '-1E2'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-1E2"}));

	result = con.Query("SELECT '-1E02'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-1E02"}));

	result = con.Query("SELECT '-1E+02'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-1E+02"}));

	result = con.Query("SELECT '1/0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1/0"}));

	result = con.Query("SELECT '0/0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0/0"}));

	result = con.Query("SELECT '-2147483648/-1'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-2147483648/-1"}));

	result = con.Query("SELECT '-9223372036854775808/-1'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-9223372036854775808/-1"}));

	result = con.Query("SELECT '-0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-0"}));

	result = con.Query("SELECT '-0.0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-0.0"}));

	result = con.Query("SELECT '+0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"+0"}));

	result = con.Query("SELECT '+0.0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"+0.0"}));

	result = con.Query("SELECT '0.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0.00"}));

	result = con.Query("SELECT '0..0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0..0"}));

	result = con.Query("SELECT '.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"."}));

	result = con.Query("SELECT '0.0.0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0.0.0"}));

	result = con.Query("SELECT '0,00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0,00"}));

	result = con.Query("SELECT '0,,0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0,,0"}));

	result = con.Query("SELECT ','");
	REQUIRE(CHECK_COLUMN(result, 0, {","}));

	result = con.Query("SELECT '0,0,0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0,0,0"}));

	result = con.Query("SELECT '0.0/0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0.0/0"}));

	result = con.Query("SELECT '1.0/0.0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1.0/0.0"}));

	result = con.Query("SELECT '0.0/0.0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0.0/0.0"}));

	result = con.Query("SELECT '1,0/0,0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1,0/0,0"}));

	result = con.Query("SELECT '0,0/0,0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0,0/0,0"}));

	result = con.Query("SELECT '--1'");
	REQUIRE(CHECK_COLUMN(result, 0, {"--1"}));

	result = con.Query("SELECT '-'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-"}));

	result = con.Query("SELECT '-.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-."}));

	result = con.Query("SELECT '-,'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-,"}));

	result = con.Query("SELECT '999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999'");
	REQUIRE(CHECK_COLUMN(result, 0, {"999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999"}));

	result = con.Query("SELECT 'NaN'");
	REQUIRE(CHECK_COLUMN(result, 0, {"NaN"}));

	result = con.Query("SELECT 'Infinity'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Infinity"}));

	result = con.Query("SELECT '-Infinity'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-Infinity"}));

	result = con.Query("SELECT 'INF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"INF"}));

	result = con.Query("SELECT '1#INF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1#INF"}));

	result = con.Query("SELECT '-1#IND'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-1#IND"}));

	result = con.Query("SELECT '1#QNAN'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1#QNAN"}));

	result = con.Query("SELECT '1#SNAN'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1#SNAN"}));

	result = con.Query("SELECT '1#IND'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1#IND"}));

	result = con.Query("SELECT '0x0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0x0"}));

	result = con.Query("SELECT '0xffffffff'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0xffffffff"}));

	result = con.Query("SELECT '0xffffffffffffffff'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0xffffffffffffffff"}));

	result = con.Query("SELECT '0xabad1dea'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0xabad1dea"}));

	result = con.Query("SELECT '123456789012345678901234567890123456789'");
	REQUIRE(CHECK_COLUMN(result, 0, {"123456789012345678901234567890123456789"}));

	result = con.Query("SELECT '1,000.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1,000.00"}));

	result = con.Query("SELECT '1 000.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1 000.00"}));

	result = con.Query("SELECT '1''000.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1'000.00"}));

	result = con.Query("SELECT '1,000,000.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1,000,000.00"}));

	result = con.Query("SELECT '1 000 000.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1 000 000.00"}));

	result = con.Query("SELECT '1''000''000.00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1'000'000.00"}));

	result = con.Query("SELECT '1.000,00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1.000,00"}));

	result = con.Query("SELECT '1 000,00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1 000,00"}));

	result = con.Query("SELECT '1''000,00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1'000,00"}));

	result = con.Query("SELECT '1.000.000,00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1.000.000,00"}));

	result = con.Query("SELECT '1 000 000,00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1 000 000,00"}));

	result = con.Query("SELECT '1''000''000,00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1'000'000,00"}));

	result = con.Query("SELECT '01000'");
	REQUIRE(CHECK_COLUMN(result, 0, {"01000"}));

	result = con.Query("SELECT '08'");
	REQUIRE(CHECK_COLUMN(result, 0, {"08"}));

	result = con.Query("SELECT '09'");
	REQUIRE(CHECK_COLUMN(result, 0, {"09"}));

	result = con.Query("SELECT '2.2250738585072011e-308'");
	REQUIRE(CHECK_COLUMN(result, 0, {"2.2250738585072011e-308"}));

	result = con.Query("SELECT '#	Special Characters'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Special Characters"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '# ASCII punctuation.  All of these characters may need to be escaped in some'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# ASCII punctuation.  All of these characters may need to be escaped in some"}));

	result = con.Query("SELECT '# contexts.  Divided into three groups based on (US-layout) keyboard position.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# contexts.  Divided into three groups based on (US-layout) keyboard position."}));

	result = con.Query("SELECT ',./;''[]\\-='");
	REQUIRE(CHECK_COLUMN(result, 0, {",./;'[]\\-="}));

	result = con.Query("SELECT '<>?:\"{}|_+'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<>?:\"{}|_+"}));

	result = con.Query("SELECT '!@#$%^&*()`~'");
	REQUIRE(CHECK_COLUMN(result, 0, {"!@#$%^&*()`~"}));

	result = con.Query("SELECT '# Non-whitespace C0 controls: U+0001 through U+0008, U+000E through U+001F,'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# Non-whitespace C0 controls: U+0001 through U+0008, U+000E through U+001F,"}));

	result = con.Query("SELECT '# and U+007F (DEL)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# and U+007F (DEL)"}));

	result = con.Query("SELECT '# Often forbidden to appear in various text-based file formats (e.g. XML),'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# Often forbidden to appear in various text-based file formats (e.g. XML),"}));

	result = con.Query("SELECT '# or reused for internal delimiters on the theory that they should never'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# or reused for internal delimiters on the theory that they should never"}));

	result = con.Query("SELECT '# appear in input.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# appear in input."}));

	result = con.Query("SELECT '# The next line may appear to be blank or mojibake in some viewers.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# The next line may appear to be blank or mojibake in some viewers."}));

	result = con.Query("SELECT ''");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));

	result = con.Query("SELECT '# Non-whitespace C1 controls: U+0080 through U+0084 and U+0086 through U+009F.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# Non-whitespace C1 controls: U+0080 through U+0084 and U+0086 through U+009F."}));

	result = con.Query("SELECT '# Commonly misinterpreted as additional graphic characters.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# Commonly misinterpreted as additional graphic characters."}));

	result = con.Query("SELECT '# The next line may appear to be blank, mojibake, or dingbats in some viewers.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# The next line may appear to be blank, mojibake, or dingbats in some viewers."}));

	result = con.Query("SELECT ''");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));

	result = con.Query("SELECT '# Whitespace: all of the characters with category Zs, Zl, or Zp (in Unicode'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# Whitespace: all of the characters with category Zs, Zl, or Zp (in Unicode"}));

	result = con.Query("SELECT '# version 8.0.0), plus U+0009 (HT), U+000B (VT), U+000C (FF), U+0085 (NEL),'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# version 8.0.0), plus U+0009 (HT), U+000B (VT), U+000C (FF), U+0085 (NEL),"}));

	result = con.Query("SELECT '# and U+200B (ZERO WIDTH SPACE), which are in the C categories but are often'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# and U+200B (ZERO WIDTH SPACE), which are in the C categories but are often"}));

	result = con.Query("SELECT '# treated as whitespace in some contexts.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# treated as whitespace in some contexts."}));

	result = con.Query("SELECT '# This file unfortunately cannot express strings containing'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# This file unfortunately cannot express strings containing"}));

	result = con.Query("SELECT '# U+0000, U+000A, or U+000D (NUL, LF, CR).'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# U+0000, U+000A, or U+000D (NUL, LF, CR)."}));

	result = con.Query("SELECT '# The next line may appear to be blank or mojibake in some viewers.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# The next line may appear to be blank or mojibake in some viewers."}));

	result = con.Query("SELECT '# The next line may be flagged for \"trailing whitespace\" in some viewers.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# The next line may be flagged for \"trailing whitespace\" in some viewers."}));

	result = con.Query("SELECT '	              ​    　'");
	REQUIRE(CHECK_COLUMN(result, 0, {"	              ​    　"}));

	result = con.Query("SELECT '# Unicode additional control characters: all of the characters with'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# Unicode additional control characters: all of the characters with"}));

	result = con.Query("SELECT '# general category Cf (in Unicode 8.0.0).'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# general category Cf (in Unicode 8.0.0)."}));

	result = con.Query("SELECT '# The next line may appear to be blank or mojibake in some viewers.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# The next line may appear to be blank or mojibake in some viewers."}));

	result = con.Query("SELECT '­؀؁؂؃؄؅؜۝܏᠎​‌‍‎‏‪‫‬‭‮⁠⁡⁢⁣⁤⁦⁧⁨⁩⁪⁫⁬⁭⁮⁯﻿￹￺￻𑂽𛲠𛲡𛲢𛲣𝅳𝅴𝅵𝅶𝅷𝅸𝅹𝅺󠀁󠀠󠀡󠀢󠀣󠀤󠀥󠀦󠀧󠀨󠀩󠀪󠀫󠀬󠀭󠀮󠀯󠀰󠀱󠀲󠀳󠀴󠀵󠀶󠀷󠀸󠀹󠀺󠀻󠀼󠀽󠀾󠀿󠁀󠁁󠁂󠁃󠁄󠁅󠁆󠁇󠁈󠁉󠁊󠁋󠁌󠁍󠁎󠁏󠁐󠁑󠁒󠁓󠁔󠁕󠁖󠁗󠁘󠁙󠁚󠁛󠁜󠁝󠁞󠁟󠁠󠁡󠁢󠁣󠁤󠁥󠁦󠁧󠁨󠁩󠁪󠁫󠁬󠁭󠁮󠁯󠁰󠁱󠁲󠁳󠁴󠁵󠁶󠁷󠁸󠁹󠁺󠁻󠁼󠁽󠁾󠁿'");
	REQUIRE(CHECK_COLUMN(result, 0, {"­؀؁؂؃؄؅؜۝܏᠎​‌‍‎‏‪‫‬‭‮⁠⁡⁢⁣⁤⁦⁧⁨⁩⁪⁫⁬⁭⁮⁯﻿￹￺￻𑂽𛲠𛲡𛲢𛲣𝅳𝅴𝅵𝅶𝅷𝅸𝅹𝅺󠀁󠀠󠀡󠀢󠀣󠀤󠀥󠀦󠀧󠀨󠀩󠀪󠀫󠀬󠀭󠀮󠀯󠀰󠀱󠀲󠀳󠀴󠀵󠀶󠀷󠀸󠀹󠀺󠀻󠀼󠀽󠀾󠀿󠁀󠁁󠁂󠁃󠁄󠁅󠁆󠁇󠁈󠁉󠁊󠁋󠁌󠁍󠁎󠁏󠁐󠁑󠁒󠁓󠁔󠁕󠁖󠁗󠁘󠁙󠁚󠁛󠁜󠁝󠁞󠁟󠁠󠁡󠁢󠁣󠁤󠁥󠁦󠁧󠁨󠁩󠁪󠁫󠁬󠁭󠁮󠁯󠁰󠁱󠁲󠁳󠁴󠁵󠁶󠁷󠁸󠁹󠁺󠁻󠁼󠁽󠁾󠁿"}));

	result = con.Query("SELECT '# \"Byte order marks\", U+FEFF and U+FFFE, each on its own line.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# \"Byte order marks\", U+FEFF and U+FFFE, each on its own line."}));

	result = con.Query("SELECT '# The next two lines may appear to be blank or mojibake in some viewers.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"# The next two lines may appear to be blank or mojibake in some viewers."}));

	result = con.Query("SELECT '﻿'");
	REQUIRE(CHECK_COLUMN(result, 0, {"﻿"}));

	result = con.Query("SELECT '￾'");
	REQUIRE(CHECK_COLUMN(result, 0, {"￾"}));

	result = con.Query("SELECT '#	Unicode Symbols'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Unicode Symbols"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain common unicode symbols (e.g. smart quotes)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain common unicode symbols (e.g. smart quotes)"}));

	result = con.Query("SELECT 'Ω≈ç√∫˜µ≤≥÷'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Ω≈ç√∫˜µ≤≥÷"}));

	result = con.Query("SELECT 'åß∂ƒ©˙∆˚¬…æ'");
	REQUIRE(CHECK_COLUMN(result, 0, {"åß∂ƒ©˙∆˚¬…æ"}));

	result = con.Query("SELECT 'œ∑´®†¥¨ˆøπ“‘'");
	REQUIRE(CHECK_COLUMN(result, 0, {"œ∑´®†¥¨ˆøπ“‘"}));

	result = con.Query("SELECT '¡™£¢∞§¶•ªº–≠'");
	REQUIRE(CHECK_COLUMN(result, 0, {"¡™£¢∞§¶•ªº–≠"}));

	result = con.Query("SELECT '¸˛Ç◊ı˜Â¯˘¿'");
	REQUIRE(CHECK_COLUMN(result, 0, {"¸˛Ç◊ı˜Â¯˘¿"}));

	result = con.Query("SELECT 'ÅÍÎÏ˝ÓÔÒÚÆ☃'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ÅÍÎÏ˝ÓÔÒÚÆ☃"}));

	result = con.Query("SELECT 'Œ„´‰ˇÁ¨ˆØ∏”’'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Œ„´‰ˇÁ¨ˆØ∏”’"}));

	result = con.Query("SELECT '`⁄€‹›ﬁﬂ‡°·‚—±'");
	REQUIRE(CHECK_COLUMN(result, 0, {"`⁄€‹›ﬁﬂ‡°·‚—±"}));

	result = con.Query("SELECT '⅛⅜⅝⅞'");
	REQUIRE(CHECK_COLUMN(result, 0, {"⅛⅜⅝⅞"}));

	result = con.Query("SELECT 'ЁЂЃЄЅІЇЈЉЊЋЌЍЎЏАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдежзийклмнопрстуфхцчшщъыьэюя'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ЁЂЃЄЅІЇЈЉЊЋЌЍЎЏАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдежзийклмнопрстуфхцчшщъыьэюя"}));

	result = con.Query("SELECT '٠١٢٣٤٥٦٧٨٩'");
	REQUIRE(CHECK_COLUMN(result, 0, {"٠١٢٣٤٥٦٧٨٩"}));

	result = con.Query("SELECT '#	Unicode Subscript/Superscript/Accents'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Unicode Subscript/Superscript/Accents"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain unicode subscripts/superscripts; can cause rendering issues'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain unicode subscripts/superscripts; can cause rendering issues"}));

	result = con.Query("SELECT '⁰⁴⁵'");
	REQUIRE(CHECK_COLUMN(result, 0, {"⁰⁴⁵"}));

	result = con.Query("SELECT '₀₁₂'");
	REQUIRE(CHECK_COLUMN(result, 0, {"₀₁₂"}));

	result = con.Query("SELECT '⁰⁴⁵₀₁₂'");
	REQUIRE(CHECK_COLUMN(result, 0, {"⁰⁴⁵₀₁₂"}));

	result = con.Query("SELECT 'ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็ ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็ ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็ ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็ ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็"}));

	result = con.Query("SELECT '#	Quotation Marks'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Quotation Marks"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain misplaced quotation marks; can cause encoding errors'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain misplaced quotation marks; can cause encoding errors"}));

	result = con.Query("SELECT ''''");
	REQUIRE(CHECK_COLUMN(result, 0, {"'"}));

	result = con.Query("SELECT '\"'");
	REQUIRE(CHECK_COLUMN(result, 0, {"\""}));

	result = con.Query("SELECT ''''''");
	REQUIRE(CHECK_COLUMN(result, 0, {"''"}));

	result = con.Query("SELECT '\"\"'");
	REQUIRE(CHECK_COLUMN(result, 0, {"\"\""}));

	result = con.Query("SELECT '''\"'''");
	REQUIRE(CHECK_COLUMN(result, 0, {"'\"'"}));

	result = con.Query("SELECT '<foo val=“bar” />'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<foo val=“bar” />"}));

	result = con.Query("SELECT '<foo val=“bar” />'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<foo val=“bar” />"}));

	result = con.Query("SELECT '<foo val=”bar“ />'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<foo val=”bar“ />"}));

	result = con.Query("SELECT '<foo val=`bar'' />'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<foo val=`bar' />"}));

	result = con.Query("SELECT '#	Two-Byte Characters'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Two-Byte Characters"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain two-byte characters: can cause rendering issues or character-length issues'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain two-byte characters: can cause rendering issues or character-length issues"}));

	result = con.Query("SELECT '田中さんにあげて下さい'");
	REQUIRE(CHECK_COLUMN(result, 0, {"田中さんにあげて下さい"}));

	result = con.Query("SELECT 'パーティーへ行かないか'");
	REQUIRE(CHECK_COLUMN(result, 0, {"パーティーへ行かないか"}));

	result = con.Query("SELECT '和製漢語'");
	REQUIRE(CHECK_COLUMN(result, 0, {"和製漢語"}));

	result = con.Query("SELECT '部落格'");
	REQUIRE(CHECK_COLUMN(result, 0, {"部落格"}));

	result = con.Query("SELECT '사회과학원 어학연구소'");
	REQUIRE(CHECK_COLUMN(result, 0, {"사회과학원 어학연구소"}));

	result = con.Query("SELECT '찦차를 타고 온 펲시맨과 쑛다리 똠방각하'");
	REQUIRE(CHECK_COLUMN(result, 0, {"찦차를 타고 온 펲시맨과 쑛다리 똠방각하"}));

	result = con.Query("SELECT '社會科學院語學研究所'");
	REQUIRE(CHECK_COLUMN(result, 0, {"社會科學院語學研究所"}));

	result = con.Query("SELECT '울란바토르'");
	REQUIRE(CHECK_COLUMN(result, 0, {"울란바토르"}));

	result = con.Query("SELECT '𠜎𠜱𠝹𠱓𠱸𠲖𠳏'");
	REQUIRE(CHECK_COLUMN(result, 0, {"𠜎𠜱𠝹𠱓𠱸𠲖𠳏"}));

	result = con.Query("SELECT '#	Special Unicode Characters Union'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Special Unicode Characters Union"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	A super string recommended by VMware Inc. Globalization Team: can effectively cause rendering issues or character-length issues to validate product globalization readiness.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	A super string recommended by VMware Inc. Globalization Team: can effectively cause rendering issues or character-length issues to validate product globalization readiness."}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	表          CJK_UNIFIED_IDEOGRAPHS (U+8868)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	表          CJK_UNIFIED_IDEOGRAPHS (U+8868)"}));

	result = con.Query("SELECT '#	ポ          KATAKANA LETTER PO (U+30DD)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	ポ          KATAKANA LETTER PO (U+30DD)"}));

	result = con.Query("SELECT '#	あ          HIRAGANA LETTER A (U+3042)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	あ          HIRAGANA LETTER A (U+3042)"}));

	result = con.Query("SELECT '#	A           LATIN CAPITAL LETTER A (U+0041)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	A           LATIN CAPITAL LETTER A (U+0041)"}));

	result = con.Query("SELECT '#	鷗          CJK_UNIFIED_IDEOGRAPHS (U+9DD7)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	鷗          CJK_UNIFIED_IDEOGRAPHS (U+9DD7)"}));

	result = con.Query("SELECT '#	Œ           LATIN SMALL LIGATURE OE (U+0153) '");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Œ           LATIN SMALL LIGATURE OE (U+0153) "}));

	result = con.Query("SELECT '#	é           LATIN SMALL LETTER E WITH ACUTE (U+00E9)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	é           LATIN SMALL LETTER E WITH ACUTE (U+00E9)"}));

	result = con.Query("SELECT '#	Ｂ           FULLWIDTH LATIN CAPITAL LETTER B (U+FF22)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Ｂ           FULLWIDTH LATIN CAPITAL LETTER B (U+FF22)"}));

	result = con.Query("SELECT '#	逍          CJK_UNIFIED_IDEOGRAPHS (U+900D)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	逍          CJK_UNIFIED_IDEOGRAPHS (U+900D)"}));

	result = con.Query("SELECT '#	Ü           LATIN SMALL LETTER U WITH DIAERESIS (U+00FC)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Ü           LATIN SMALL LETTER U WITH DIAERESIS (U+00FC)"}));

	result = con.Query("SELECT '#	ß           LATIN SMALL LETTER SHARP S (U+00DF)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	ß           LATIN SMALL LETTER SHARP S (U+00DF)"}));

	result = con.Query("SELECT '#	ª           FEMININE ORDINAL INDICATOR (U+00AA)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	ª           FEMININE ORDINAL INDICATOR (U+00AA)"}));

	result = con.Query("SELECT '#	ą           LATIN SMALL LETTER A WITH OGONEK (U+0105)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	ą           LATIN SMALL LETTER A WITH OGONEK (U+0105)"}));

	result = con.Query("SELECT '#	ñ           LATIN SMALL LETTER N WITH TILDE (U+00F1)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	ñ           LATIN SMALL LETTER N WITH TILDE (U+00F1)"}));

	result = con.Query("SELECT '#	丂          CJK_UNIFIED_IDEOGRAPHS (U+4E02)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	丂          CJK_UNIFIED_IDEOGRAPHS (U+4E02)"}));

	result = con.Query("SELECT '#	㐀          CJK Ideograph Extension A, First (U+3400)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	㐀          CJK Ideograph Extension A, First (U+3400)"}));

	result = con.Query("SELECT '#	𠀀          CJK Ideograph Extension B, First (U+20000)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	𠀀          CJK Ideograph Extension B, First (U+20000)"}));

	result = con.Query("SELECT '表ポあA鷗ŒéＢ逍Üßªąñ丂㐀𠀀'");
	REQUIRE(CHECK_COLUMN(result, 0, {"表ポあA鷗ŒéＢ逍Üßªąñ丂㐀𠀀"}));

	result = con.Query("SELECT '#	Changing length when lowercased'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Changing length when lowercased"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Characters which increase in length (2 to 3 bytes) when lowercased'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Characters which increase in length (2 to 3 bytes) when lowercased"}));

	result = con.Query("SELECT '#	Credit: https://twitter.com/jifa/status/625776454479970304'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Credit: https://twitter.com/jifa/status/625776454479970304"}));

	result = con.Query("SELECT 'Ⱥ'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Ⱥ"}));

	result = con.Query("SELECT 'Ⱦ'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Ⱦ"}));

	result = con.Query("SELECT '#	Japanese Emoticons'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Japanese Emoticons"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which consists of Japanese-style emoticons which are popular on the web'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which consists of Japanese-style emoticons which are popular on the web"}));

	result = con.Query("SELECT 'ヽ༼ຈل͜ຈ༽ﾉ ヽ༼ຈل͜ຈ༽ﾉ'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ヽ༼ຈل͜ຈ༽ﾉ ヽ༼ຈل͜ຈ༽ﾉ"}));

	result = con.Query("SELECT '(｡◕ ∀ ◕｡)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"(｡◕ ∀ ◕｡)"}));

	result = con.Query("SELECT '｀ｨ(´∀｀∩'");
	REQUIRE(CHECK_COLUMN(result, 0, {"｀ｨ(´∀｀∩"}));

	result = con.Query("SELECT '__ﾛ(,_,*)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"__ﾛ(,_,*)"}));

	result = con.Query("SELECT '・(￣∀￣)・:*:'");
	REQUIRE(CHECK_COLUMN(result, 0, {"・(￣∀￣)・:*:"}));

	result = con.Query("SELECT 'ﾟ･✿ヾ╲(｡◕‿◕｡)╱✿･ﾟ'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ﾟ･✿ヾ╲(｡◕‿◕｡)╱✿･ﾟ"}));

	result = con.Query("SELECT ',。・:*:・゜’( ☻ ω ☻ )。・:*:・゜’'");
	REQUIRE(CHECK_COLUMN(result, 0, {",。・:*:・゜’( ☻ ω ☻ )。・:*:・゜’"}));

	result = con.Query("SELECT '(╯°□°）╯︵ ┻━┻)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"(╯°□°）╯︵ ┻━┻)"}));

	result = con.Query("SELECT '(ﾉಥ益ಥ）ﾉ﻿ ┻━┻'");
	REQUIRE(CHECK_COLUMN(result, 0, {"(ﾉಥ益ಥ）ﾉ﻿ ┻━┻"}));

	result = con.Query("SELECT '┬─┬ノ( º _ ºノ)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"┬─┬ノ( º _ ºノ)"}));

	result = con.Query("SELECT '( ͡° ͜ʖ ͡°)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"( ͡° ͜ʖ ͡°)"}));

	result = con.Query("SELECT '¯\\_(ツ)_/¯'");
	REQUIRE(CHECK_COLUMN(result, 0, {"¯\\_(ツ)_/¯"}));

	result = con.Query("SELECT '#	Emoji'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Emoji"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain Emoji; should be the same behavior as two-byte characters, but not always'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain Emoji; should be the same behavior as two-byte characters, but not always"}));

	result = con.Query("SELECT '😍'");
	REQUIRE(CHECK_COLUMN(result, 0, {"😍"}));

	result = con.Query("SELECT '👩🏽'");
	REQUIRE(CHECK_COLUMN(result, 0, {"👩🏽"}));

	result = con.Query("SELECT '👾 🙇 💁 🙅 🙆 🙋 🙎 🙍'");
	REQUIRE(CHECK_COLUMN(result, 0, {"👾 🙇 💁 🙅 🙆 🙋 🙎 🙍"}));

	result = con.Query("SELECT '🐵 🙈 🙉 🙊'");
	REQUIRE(CHECK_COLUMN(result, 0, {"🐵 🙈 🙉 🙊"}));

	result = con.Query("SELECT '❤️ 💔 💌 💕 💞 💓 💗 💖 💘 💝 💟 💜 💛 💚 💙'");
	REQUIRE(CHECK_COLUMN(result, 0, {"❤️ 💔 💌 💕 💞 💓 💗 💖 💘 💝 💟 💜 💛 💚 💙"}));

	result = con.Query("SELECT '✋🏿 💪🏿 👐🏿 🙌🏿 👏🏿 🙏🏿'");
	REQUIRE(CHECK_COLUMN(result, 0, {"✋🏿 💪🏿 👐🏿 🙌🏿 👏🏿 🙏🏿"}));

	result = con.Query("SELECT '🚾 🆒 🆓 🆕 🆖 🆗 🆙 🏧'");
	REQUIRE(CHECK_COLUMN(result, 0, {"🚾 🆒 🆓 🆕 🆖 🆗 🆙 🏧"}));

	result = con.Query("SELECT '0️⃣ 1️⃣ 2️⃣ 3️⃣ 4️⃣ 5️⃣ 6️⃣ 7️⃣ 8️⃣ 9️⃣ 🔟'");
	REQUIRE(CHECK_COLUMN(result, 0, {"0️⃣ 1️⃣ 2️⃣ 3️⃣ 4️⃣ 5️⃣ 6️⃣ 7️⃣ 8️⃣ 9️⃣ 🔟"}));

	result = con.Query("SELECT '#       Regional Indicator Symbols'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#       Regional Indicator Symbols"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#       Regional Indicator Symbols can be displayed differently across'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#       Regional Indicator Symbols can be displayed differently across"}));

	result = con.Query("SELECT '#       fonts, and have a number of special behaviors'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#       fonts, and have a number of special behaviors"}));

	result = con.Query("SELECT '🇺🇸🇷🇺🇸 🇦🇫🇦🇲🇸'");
	REQUIRE(CHECK_COLUMN(result, 0, {"🇺🇸🇷🇺🇸 🇦🇫🇦🇲🇸"}));

	result = con.Query("SELECT '🇺🇸🇷🇺🇸🇦🇫🇦🇲'");
	REQUIRE(CHECK_COLUMN(result, 0, {"🇺🇸🇷🇺🇸🇦🇫🇦🇲"}));

	result = con.Query("SELECT '🇺🇸🇷🇺🇸🇦'");
	REQUIRE(CHECK_COLUMN(result, 0, {"🇺🇸🇷🇺🇸🇦"}));

	result = con.Query("SELECT '#	Unicode Numbers'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Unicode Numbers"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain unicode numbers; if the code is localized, it should see the input as numeric'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain unicode numbers; if the code is localized, it should see the input as numeric"}));

	result = con.Query("SELECT '１２３'");
	REQUIRE(CHECK_COLUMN(result, 0, {"１２３"}));

	result = con.Query("SELECT '١٢٣'");
	REQUIRE(CHECK_COLUMN(result, 0, {"١٢٣"}));

	result = con.Query("SELECT '#	Right-To-Left Strings'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Right-To-Left Strings"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain text that should be rendered RTL if possible (e.g. Arabic, Hebrew)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain text that should be rendered RTL if possible (e.g. Arabic, Hebrew)"}));

	result = con.Query("SELECT 'ثم نفس سقطت وبالتحديد،, جزيرتي باستخدام أن دنو. إذ هنا؟ الستار وتنصيب كان. أهّل ايطاليا، بريطانيا-فرنسا قد أخذ. سليمان، إتفاقية بين ما, يذكر الحدود أي بعد, معاملة بولندا، الإطلاق عل إيو.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ثم نفس سقطت وبالتحديد،, جزيرتي باستخدام أن دنو. إذ هنا؟ الستار وتنصيب كان. أهّل ايطاليا، بريطانيا-فرنسا قد أخذ. سليمان، إتفاقية بين ما, يذكر الحدود أي بعد, معاملة بولندا، الإطلاق عل إيو."}));

	result = con.Query("SELECT 'בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ'");
	REQUIRE(CHECK_COLUMN(result, 0, {"בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ"}));

	result = con.Query("SELECT 'הָיְתָהtestالصفحات التّحول'");
	REQUIRE(CHECK_COLUMN(result, 0, {"הָיְתָהtestالصفحات التّحول"}));

	result = con.Query("SELECT '﷽'");
	REQUIRE(CHECK_COLUMN(result, 0, {"﷽"}));

	result = con.Query("SELECT 'ﷺ'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ﷺ"}));

	result = con.Query("SELECT 'مُنَاقَشَةُ سُبُلِ اِسْتِخْدَامِ اللُّغَةِ فِي النُّظُمِ الْقَائِمَةِ وَفِيم يَخُصَّ التَّطْبِيقَاتُ الْحاسُوبِيَّةُ، '");
	REQUIRE(CHECK_COLUMN(result, 0, {"مُنَاقَشَةُ سُبُلِ اِسْتِخْدَامِ اللُّغَةِ فِي النُّظُمِ الْقَائِمَةِ وَفِيم يَخُصَّ التَّطْبِيقَاتُ الْحاسُوبِيَّةُ، "}));

	result = con.Query("SELECT '#	Trick Unicode'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Trick Unicode"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain unicode with unusual properties (e.g. Right-to-left override) (c.f. http://www.unicode.org/charts/PDF/U2000.pdf)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain unicode with unusual properties (e.g. Right-to-left override) (c.f. http://www.unicode.org/charts/PDF/U2000.pdf)"}));

	result = con.Query("SELECT '‪‪test‪'");
	REQUIRE(CHECK_COLUMN(result, 0, {"‪‪test‪"}));

	result = con.Query("SELECT '‫test‫'");
	REQUIRE(CHECK_COLUMN(result, 0, {"‫test‫"}));

	result = con.Query("SELECT ' test '");
	REQUIRE(CHECK_COLUMN(result, 0, {" test "}));

	result = con.Query("SELECT 'test⁠test‫'");
	REQUIRE(CHECK_COLUMN(result, 0, {"test⁠test‫"}));

	result = con.Query("SELECT '⁦test⁧'");
	REQUIRE(CHECK_COLUMN(result, 0, {"⁦test⁧"}));

	result = con.Query("SELECT '#	Zalgo Text'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Zalgo Text"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain \"corrupted\" text. The corruption will not appear in non-HTML text, however. (via http://www.eeemo.net)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain \"corrupted\" text. The corruption will not appear in non-HTML text, however. (via http://www.eeemo.net)"}));

	result = con.Query("SELECT 'Ṱ̺̺̕o͞ ̷i̲̬͇̪͙n̝̗͕v̟̜̘̦͟o̶̙̰̠kè͚̮̺̪̹̱̤ ̖t̝͕̳̣̻̪͞h̼͓̲̦̳̘̲e͇̣̰̦̬͎ ̢̼̻̱̘h͚͎͙̜̣̲ͅi̦̲̣̰̤v̻͍e̺̭̳̪̰-m̢iͅn̖̺̞̲̯̰d̵̼̟͙̩̼̘̳ ̞̥̱̳̭r̛̗̘e͙p͠r̼̞̻̭̗e̺̠̣͟s̘͇̳͍̝͉e͉̥̯̞̲͚̬͜ǹ̬͎͎̟̖͇̤t͍̬̤͓̼̭͘ͅi̪̱n͠g̴͉ ͏͉ͅc̬̟h͡a̫̻̯͘o̫̟̖͍̙̝͉s̗̦̲.̨̹͈̣'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Ṱ̺̺̕o͞ ̷i̲̬͇̪͙n̝̗͕v̟̜̘̦͟o̶̙̰̠kè͚̮̺̪̹̱̤ ̖t̝͕̳̣̻̪͞h̼͓̲̦̳̘̲e͇̣̰̦̬͎ ̢̼̻̱̘h͚͎͙̜̣̲ͅi̦̲̣̰̤v̻͍e̺̭̳̪̰-m̢iͅn̖̺̞̲̯̰d̵̼̟͙̩̼̘̳ ̞̥̱̳̭r̛̗̘e͙p͠r̼̞̻̭̗e̺̠̣͟s̘͇̳͍̝͉e͉̥̯̞̲͚̬͜ǹ̬͎͎̟̖͇̤t͍̬̤͓̼̭͘ͅi̪̱n͠g̴͉ ͏͉ͅc̬̟h͡a̫̻̯͘o̫̟̖͍̙̝͉s̗̦̲.̨̹͈̣"}));

	result = con.Query("SELECT '̡͓̞ͅI̗̘̦͝n͇͇͙v̮̫ok̲̫̙͈i̖͙̭̹̠̞n̡̻̮̣̺g̲͈͙̭͙̬͎ ̰t͔̦h̞̲e̢̤ ͍̬̲͖f̴̘͕̣è͖ẹ̥̩l͖͔͚i͓͚̦͠n͖͍̗͓̳̮g͍ ̨o͚̪͡f̘̣̬ ̖̘͖̟͙̮c҉͔̫͖͓͇͖ͅh̵̤̣͚͔á̗̼͕ͅo̼̣̥s̱͈̺̖̦̻͢.̛̖̞̠̫̰'");
	REQUIRE(CHECK_COLUMN(result, 0, {"̡͓̞ͅI̗̘̦͝n͇͇͙v̮̫ok̲̫̙͈i̖͙̭̹̠̞n̡̻̮̣̺g̲͈͙̭͙̬͎ ̰t͔̦h̞̲e̢̤ ͍̬̲͖f̴̘͕̣è͖ẹ̥̩l͖͔͚i͓͚̦͠n͖͍̗͓̳̮g͍ ̨o͚̪͡f̘̣̬ ̖̘͖̟͙̮c҉͔̫͖͓͇͖ͅh̵̤̣͚͔á̗̼͕ͅo̼̣̥s̱͈̺̖̦̻͢.̛̖̞̠̫̰"}));

	result = con.Query("SELECT '̗̺͖̹̯͓Ṯ̤͍̥͇͈h̲́e͏͓̼̗̙̼̣͔ ͇̜̱̠͓͍ͅN͕͠e̗̱z̘̝̜̺͙p̤̺̹͍̯͚e̠̻̠͜r̨̤͍̺̖͔̖̖d̠̟̭̬̝͟i̦͖̩͓͔̤a̠̗̬͉̙n͚͜ ̻̞̰͚ͅh̵͉i̳̞v̢͇ḙ͎͟-҉̭̩̼͔m̤̭̫i͕͇̝̦n̗͙ḍ̟ ̯̲͕͞ǫ̟̯̰̲͙̻̝f ̪̰̰̗̖̭̘͘c̦͍̲̞͍̩̙ḥ͚a̮͎̟̙͜ơ̩̹͎s̤.̝̝ ҉Z̡̖̜͖̰̣͉̜a͖̰͙̬͡l̲̫̳͍̩g̡̟̼̱͚̞̬ͅo̗͜.̟'");
	REQUIRE(CHECK_COLUMN(result, 0, {"̗̺͖̹̯͓Ṯ̤͍̥͇͈h̲́e͏͓̼̗̙̼̣͔ ͇̜̱̠͓͍ͅN͕͠e̗̱z̘̝̜̺͙p̤̺̹͍̯͚e̠̻̠͜r̨̤͍̺̖͔̖̖d̠̟̭̬̝͟i̦͖̩͓͔̤a̠̗̬͉̙n͚͜ ̻̞̰͚ͅh̵͉i̳̞v̢͇ḙ͎͟-҉̭̩̼͔m̤̭̫i͕͇̝̦n̗͙ḍ̟ ̯̲͕͞ǫ̟̯̰̲͙̻̝f ̪̰̰̗̖̭̘͘c̦͍̲̞͍̩̙ḥ͚a̮͎̟̙͜ơ̩̹͎s̤.̝̝ ҉Z̡̖̜͖̰̣͉̜a͖̰͙̬͡l̲̫̳͍̩g̡̟̼̱͚̞̬ͅo̗͜.̟"}));

	result = con.Query("SELECT '̦H̬̤̗̤͝e͜ ̜̥̝̻͍̟́w̕h̖̯͓o̝͙̖͎̱̮ ҉̺̙̞̟͈W̷̼̭a̺̪͍į͈͕̭͙̯̜t̶̼̮s̘͙͖̕ ̠̫̠B̻͍͙͉̳ͅe̵h̵̬͇̫͙i̹͓̳̳̮͎̫̕n͟d̴̪̜̖ ̰͉̩͇͙̲͞ͅT͖̼͓̪͢h͏͓̮̻e̬̝̟ͅ ̤̹̝W͙̞̝͔͇͝ͅa͏͓͔̹̼̣l̴͔̰̤̟͔ḽ̫.͕'");
	REQUIRE(CHECK_COLUMN(result, 0, {"̦H̬̤̗̤͝e͜ ̜̥̝̻͍̟́w̕h̖̯͓o̝͙̖͎̱̮ ҉̺̙̞̟͈W̷̼̭a̺̪͍į͈͕̭͙̯̜t̶̼̮s̘͙͖̕ ̠̫̠B̻͍͙͉̳ͅe̵h̵̬͇̫͙i̹͓̳̳̮͎̫̕n͟d̴̪̜̖ ̰͉̩͇͙̲͞ͅT͖̼͓̪͢h͏͓̮̻e̬̝̟ͅ ̤̹̝W͙̞̝͔͇͝ͅa͏͓͔̹̼̣l̴͔̰̤̟͔ḽ̫.͕"}));

	result = con.Query("SELECT 'Z̮̞̠͙͔ͅḀ̗̞͈̻̗Ḷ͙͎̯̹̞͓G̻O̭̗̮'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Z̮̞̠͙͔ͅḀ̗̞͈̻̗Ḷ͙͎̯̹̞͓G̻O̭̗̮"}));

	result = con.Query("SELECT '#	Unicode Upsidedown'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Unicode Upsidedown"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain unicode with an \"upsidedown\" effect (via http://www.upsidedowntext.com)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain unicode with an \"upsidedown\" effect (via http://www.upsidedowntext.com)"}));

	result = con.Query("SELECT '˙ɐnbᴉlɐ ɐuƃɐɯ ǝɹolop ʇǝ ǝɹoqɐl ʇn ʇunpᴉpᴉɔuᴉ ɹodɯǝʇ poɯsnᴉǝ op pǝs ''ʇᴉlǝ ƃuᴉɔsᴉdᴉpɐ ɹnʇǝʇɔǝsuoɔ ''ʇǝɯɐ ʇᴉs ɹolop ɯnsdᴉ ɯǝɹo˥'");
	REQUIRE(CHECK_COLUMN(result, 0, {"˙ɐnbᴉlɐ ɐuƃɐɯ ǝɹolop ʇǝ ǝɹoqɐl ʇn ʇunpᴉpᴉɔuᴉ ɹodɯǝʇ poɯsnᴉǝ op pǝs 'ʇᴉlǝ ƃuᴉɔsᴉdᴉpɐ ɹnʇǝʇɔǝsuoɔ 'ʇǝɯɐ ʇᴉs ɹolop ɯnsdᴉ ɯǝɹo˥"}));

	result = con.Query("SELECT '00˙Ɩ$-'");
	REQUIRE(CHECK_COLUMN(result, 0, {"00˙Ɩ$-"}));

	result = con.Query("SELECT '#	Unicode font'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Unicode font"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which contain bold/italic/etc. versions of normal characters'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which contain bold/italic/etc. versions of normal characters"}));

	result = con.Query("SELECT 'Ｔｈｅ ｑｕｉｃｋ ｂｒｏｗｎ ｆｏｘ ｊｕｍｐｓ ｏｖｅｒ ｔｈｅ ｌａｚｙ ｄｏｇ'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Ｔｈｅ ｑｕｉｃｋ ｂｒｏｗｎ ｆｏｘ ｊｕｍｐｓ ｏｖｅｒ ｔｈｅ ｌａｚｙ ｄｏｇ"}));

	result = con.Query("SELECT '𝐓𝐡𝐞 𝐪𝐮𝐢𝐜𝐤 𝐛𝐫𝐨𝐰𝐧 𝐟𝐨𝐱 𝐣𝐮𝐦𝐩𝐬 𝐨𝐯𝐞𝐫 𝐭𝐡𝐞 𝐥𝐚𝐳𝐲 𝐝𝐨𝐠'");
	REQUIRE(CHECK_COLUMN(result, 0, {"𝐓𝐡𝐞 𝐪𝐮𝐢𝐜𝐤 𝐛𝐫𝐨𝐰𝐧 𝐟𝐨𝐱 𝐣𝐮𝐦𝐩𝐬 𝐨𝐯𝐞𝐫 𝐭𝐡𝐞 𝐥𝐚𝐳𝐲 𝐝𝐨𝐠"}));

	result = con.Query("SELECT '𝕿𝖍𝖊 𝖖𝖚𝖎𝖈𝖐 𝖇𝖗𝖔𝖜𝖓 𝖋𝖔𝖝 𝖏𝖚𝖒𝖕𝖘 𝖔𝖛𝖊𝖗 𝖙𝖍𝖊 𝖑𝖆𝖟𝖞 𝖉𝖔𝖌'");
	REQUIRE(CHECK_COLUMN(result, 0, {"𝕿𝖍𝖊 𝖖𝖚𝖎𝖈𝖐 𝖇𝖗𝖔𝖜𝖓 𝖋𝖔𝖝 𝖏𝖚𝖒𝖕𝖘 𝖔𝖛𝖊𝖗 𝖙𝖍𝖊 𝖑𝖆𝖟𝖞 𝖉𝖔𝖌"}));

	result = con.Query("SELECT '𝑻𝒉𝒆 𝒒𝒖𝒊𝒄𝒌 𝒃𝒓𝒐𝒘𝒏 𝒇𝒐𝒙 𝒋𝒖𝒎𝒑𝒔 𝒐𝒗𝒆𝒓 𝒕𝒉𝒆 𝒍𝒂𝒛𝒚 𝒅𝒐𝒈'");
	REQUIRE(CHECK_COLUMN(result, 0, {"𝑻𝒉𝒆 𝒒𝒖𝒊𝒄𝒌 𝒃𝒓𝒐𝒘𝒏 𝒇𝒐𝒙 𝒋𝒖𝒎𝒑𝒔 𝒐𝒗𝒆𝒓 𝒕𝒉𝒆 𝒍𝒂𝒛𝒚 𝒅𝒐𝒈"}));

	result = con.Query("SELECT '𝓣𝓱𝓮 𝓺𝓾𝓲𝓬𝓴 𝓫𝓻𝓸𝔀𝓷 𝓯𝓸𝔁 𝓳𝓾𝓶𝓹𝓼 𝓸𝓿𝓮𝓻 𝓽𝓱𝓮 𝓵𝓪𝔃𝔂 𝓭𝓸𝓰'");
	REQUIRE(CHECK_COLUMN(result, 0, {"𝓣𝓱𝓮 𝓺𝓾𝓲𝓬𝓴 𝓫𝓻𝓸𝔀𝓷 𝓯𝓸𝔁 𝓳𝓾𝓶𝓹𝓼 𝓸𝓿𝓮𝓻 𝓽𝓱𝓮 𝓵𝓪𝔃𝔂 𝓭𝓸𝓰"}));

	result = con.Query("SELECT '𝕋𝕙𝕖 𝕢𝕦𝕚𝕔𝕜 𝕓𝕣𝕠𝕨𝕟 𝕗𝕠𝕩 𝕛𝕦𝕞𝕡𝕤 𝕠𝕧𝕖𝕣 𝕥𝕙𝕖 𝕝𝕒𝕫𝕪 𝕕𝕠𝕘'");
	REQUIRE(CHECK_COLUMN(result, 0, {"𝕋𝕙𝕖 𝕢𝕦𝕚𝕔𝕜 𝕓𝕣𝕠𝕨𝕟 𝕗𝕠𝕩 𝕛𝕦𝕞𝕡𝕤 𝕠𝕧𝕖𝕣 𝕥𝕙𝕖 𝕝𝕒𝕫𝕪 𝕕𝕠𝕘"}));

	result = con.Query("SELECT '𝚃𝚑𝚎 𝚚𝚞𝚒𝚌𝚔 𝚋𝚛𝚘𝚠𝚗 𝚏𝚘𝚡 𝚓𝚞𝚖𝚙𝚜 𝚘𝚟𝚎𝚛 𝚝𝚑𝚎 𝚕𝚊𝚣𝚢 𝚍𝚘𝚐'");
	REQUIRE(CHECK_COLUMN(result, 0, {"𝚃𝚑𝚎 𝚚𝚞𝚒𝚌𝚔 𝚋𝚛𝚘𝚠𝚗 𝚏𝚘𝚡 𝚓𝚞𝚖𝚙𝚜 𝚘𝚟𝚎𝚛 𝚝𝚑𝚎 𝚕𝚊𝚣𝚢 𝚍𝚘𝚐"}));

	result = con.Query("SELECT '⒯⒣⒠ ⒬⒰⒤⒞⒦ ⒝⒭⒪⒲⒩ ⒡⒪⒳ ⒥⒰⒨⒫⒮ ⒪⒱⒠⒭ ⒯⒣⒠ ⒧⒜⒵⒴ ⒟⒪⒢'");
	REQUIRE(CHECK_COLUMN(result, 0, {"⒯⒣⒠ ⒬⒰⒤⒞⒦ ⒝⒭⒪⒲⒩ ⒡⒪⒳ ⒥⒰⒨⒫⒮ ⒪⒱⒠⒭ ⒯⒣⒠ ⒧⒜⒵⒴ ⒟⒪⒢"}));

	result = con.Query("SELECT '#	Script Injection'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Script Injection"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which attempt to invoke a benign script injection; shows vulnerability to XSS'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which attempt to invoke a benign script injection; shows vulnerability to XSS"}));

	result = con.Query("SELECT '<script>alert(123)</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<script>alert(123)</script>"}));

	result = con.Query("SELECT '&lt;script&gt;alert(&#39;123&#39;);&lt;/script&gt;'");
	REQUIRE(CHECK_COLUMN(result, 0, {"&lt;script&gt;alert(&#39;123&#39;);&lt;/script&gt;"}));

	result = con.Query("SELECT '<img src=x onerror=alert(123) />'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x onerror=alert(123) />"}));

	result = con.Query("SELECT '<svg><script>123<1>alert(123)</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<svg><script>123<1>alert(123)</script>"}));

	result = con.Query("SELECT '\"><script>alert(123)</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"\"><script>alert(123)</script>"}));

	result = con.Query("SELECT '''><script>alert(123)</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"'><script>alert(123)</script>"}));

	result = con.Query("SELECT '><script>alert(123)</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"><script>alert(123)</script>"}));

	result = con.Query("SELECT '</script><script>alert(123)</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"</script><script>alert(123)</script>"}));

	result = con.Query("SELECT '< / script >< script >alert(123)< / script >'");
	REQUIRE(CHECK_COLUMN(result, 0, {"< / script >< script >alert(123)< / script >"}));

	result = con.Query("SELECT ' onfocus=JaVaSCript:alert(123) autofocus'");
	REQUIRE(CHECK_COLUMN(result, 0, {" onfocus=JaVaSCript:alert(123) autofocus"}));

	result = con.Query("SELECT '\" onfocus=JaVaSCript:alert(123) autofocus'");
	REQUIRE(CHECK_COLUMN(result, 0, {"\" onfocus=JaVaSCript:alert(123) autofocus"}));

	result = con.Query("SELECT ''' onfocus=JaVaSCript:alert(123) autofocus'");
	REQUIRE(CHECK_COLUMN(result, 0, {"' onfocus=JaVaSCript:alert(123) autofocus"}));

	result = con.Query("SELECT '＜script＞alert(123)＜/script＞'");
	REQUIRE(CHECK_COLUMN(result, 0, {"＜script＞alert(123)＜/script＞"}));

	result = con.Query("SELECT '<sc<script>ript>alert(123)</sc</script>ript>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<sc<script>ript>alert(123)</sc</script>ript>"}));

	result = con.Query("SELECT '--><script>alert(123)</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"--><script>alert(123)</script>"}));

	result = con.Query("SELECT '\";alert(123);t=\"'");
	REQUIRE(CHECK_COLUMN(result, 0, {"\";alert(123);t=\""}));

	result = con.Query("SELECT ''';alert(123);t='''");
	REQUIRE(CHECK_COLUMN(result, 0, {"';alert(123);t='"}));

	result = con.Query("SELECT 'JavaSCript:alert(123)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"JavaSCript:alert(123)"}));

	result = con.Query("SELECT ';alert(123);'");
	REQUIRE(CHECK_COLUMN(result, 0, {";alert(123);"}));

	result = con.Query("SELECT 'src=JaVaSCript:prompt(132)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"src=JaVaSCript:prompt(132)"}));

	result = con.Query("SELECT '\"><script>alert(123);</script x=\"'");
	REQUIRE(CHECK_COLUMN(result, 0, {"\"><script>alert(123);</script x=\""}));

	result = con.Query("SELECT '''><script>alert(123);</script x='''");
	REQUIRE(CHECK_COLUMN(result, 0, {"'><script>alert(123);</script x='"}));

	result = con.Query("SELECT '><script>alert(123);</script x='");
	REQUIRE(CHECK_COLUMN(result, 0, {"><script>alert(123);</script x="}));

	result = con.Query("SELECT '\" autofocus onkeyup=\"javascript:alert(123)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"\" autofocus onkeyup=\"javascript:alert(123)"}));

	result = con.Query("SELECT ''' autofocus onkeyup=''javascript:alert(123)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"' autofocus onkeyup='javascript:alert(123)"}));

	result = con.Query("SELECT '<script\\x20type=\"text/javascript\">javascript:alert(1);</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<script\\x20type=\"text/javascript\">javascript:alert(1);</script>"}));

	result = con.Query("SELECT '<script\\x3Etype=\"text/javascript\">javascript:alert(1);</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<script\\x3Etype=\"text/javascript\">javascript:alert(1);</script>"}));

	result = con.Query("SELECT '<script\\x0Dtype=\"text/javascript\">javascript:alert(1);</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<script\\x0Dtype=\"text/javascript\">javascript:alert(1);</script>"}));

	result = con.Query("SELECT '<script\\x09type=\"text/javascript\">javascript:alert(1);</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<script\\x09type=\"text/javascript\">javascript:alert(1);</script>"}));

	result = con.Query("SELECT '<script\\x0Ctype=\"text/javascript\">javascript:alert(1);</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<script\\x0Ctype=\"text/javascript\">javascript:alert(1);</script>"}));

	result = con.Query("SELECT '<script\\x2Ftype=\"text/javascript\">javascript:alert(1);</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<script\\x2Ftype=\"text/javascript\">javascript:alert(1);</script>"}));

	result = con.Query("SELECT '<script\\x0Atype=\"text/javascript\">javascript:alert(1);</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<script\\x0Atype=\"text/javascript\">javascript:alert(1);</script>"}));

	result = con.Query("SELECT '''`\"><\\x3Cscript>javascript:alert(1)</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"'`\"><\\x3Cscript>javascript:alert(1)</script>"}));

	result = con.Query("SELECT '''`\"><\\x00script>javascript:alert(1)</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"'`\"><\\x00script>javascript:alert(1)</script>"}));

	result = con.Query("SELECT 'ABC<div style=\"x\\x3Aexpression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x\\x3Aexpression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:expression\\x5C(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:expression\\x5C(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:expression\\x00(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:expression\\x00(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:exp\\x00ression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:exp\\x00ression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:exp\\x5Cression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:exp\\x5Cression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\x0Aexpression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\x0Aexpression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\x09expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\x09expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE3\\x80\\x80expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE3\\x80\\x80expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x84expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x84expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xC2\\xA0expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xC2\\xA0expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x80expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x80expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x8Aexpression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x8Aexpression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\x0Dexpression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\x0Dexpression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\x0Cexpression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\x0Cexpression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x87expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x87expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xEF\\xBB\\xBFexpression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xEF\\xBB\\xBFexpression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\x20expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\x20expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x88expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x88expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\x00expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\x00expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x8Bexpression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x8Bexpression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x86expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x86expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x85expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x85expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x82expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x82expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\x0Bexpression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\x0Bexpression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x81expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x81expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x83expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x83expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT 'ABC<div style=\"x:\\xE2\\x80\\x89expression(javascript:alert(1)\">DEF'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ABC<div style=\"x:\\xE2\\x80\\x89expression(javascript:alert(1)\">DEF"}));

	result = con.Query("SELECT '<a href=\"\\x0Bjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x0Bjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x0Fjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x0Fjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xC2\\xA0javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xC2\\xA0javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x05javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x05javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE1\\xA0\\x8Ejavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE1\\xA0\\x8Ejavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x18javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x18javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x11javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x11javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x88javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x88javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x89javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x89javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x80javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x80javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x17javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x17javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x03javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x03javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x0Ejavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x0Ejavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x1Ajavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x1Ajavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x00javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x00javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x10javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x10javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x82javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x82javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x20javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x20javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x13javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x13javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x09javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x09javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x8Ajavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x8Ajavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x14javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x14javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x19javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x19javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\xAFjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\xAFjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x1Fjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x1Fjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x81javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x81javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x1Djavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x1Djavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x87javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x87javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x07javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x07javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE1\\x9A\\x80javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE1\\x9A\\x80javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x83javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x83javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x04javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x04javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x01javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x01javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x08javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x08javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x84javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x84javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x86javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x86javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE3\\x80\\x80javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE3\\x80\\x80javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x12javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x12javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x0Djavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x0Djavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x0Ajavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x0Ajavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x0Cjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x0Cjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x15javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x15javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\xA8javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\xA8javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x16javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x16javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x02javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x02javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x1Bjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x1Bjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x06javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x06javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\xA9javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\xA9javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x80\\x85javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x80\\x85javascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x1Ejavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x1Ejavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\xE2\\x81\\x9Fjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\xE2\\x81\\x9Fjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"\\x1Cjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"\\x1Cjavascript:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"javascript\\x00:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"javascript\\x00:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"javascript\\x3A:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"javascript\\x3A:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"javascript\\x09:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"javascript\\x09:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"javascript\\x0D:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"javascript\\x0D:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<a href=\"javascript\\x0A:javascript:alert(1)\" id=\"fuzzelement1\">test</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=\"javascript\\x0A:javascript:alert(1)\" id=\"fuzzelement1\">test</a>"}));

	result = con.Query("SELECT '<img \\x00src=x onerror=\"alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img \\x00src=x onerror=\"alert(1)\">"}));

	result = con.Query("SELECT '<img \\x47src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img \\x47src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img \\x11src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img \\x11src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img \\x12src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img \\x12src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img\\x47src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img\\x47src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img\\x10src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img\\x10src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img\\x13src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img\\x13src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img\\x32src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img\\x32src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img\\x47src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img\\x47src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img\\x11src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img\\x11src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img \\x47src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img \\x47src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img \\x34src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img \\x34src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img \\x39src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img \\x39src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img \\x00src=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img \\x00src=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src\\x09=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src\\x09=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src\\x10=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src\\x10=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src\\x13=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src\\x13=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src\\x32=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src\\x32=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src\\x12=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src\\x12=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src\\x11=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src\\x11=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src\\x00=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src\\x00=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src\\x47=x onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src\\x47=x onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src=x\\x09onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x\\x09onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src=x\\x10onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x\\x10onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src=x\\x11onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x\\x11onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src=x\\x12onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x\\x12onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src=x\\x13onerror=\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x\\x13onerror=\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img[a][b][c]src[d]=x[e]onerror=[f]\"alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img[a][b][c]src[d]=x[e]onerror=[f]\"alert(1)\">"}));

	result = con.Query("SELECT '<img src=x onerror=\\x09\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x onerror=\\x09\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src=x onerror=\\x10\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x onerror=\\x10\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src=x onerror=\\x11\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x onerror=\\x11\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src=x onerror=\\x12\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x onerror=\\x12\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src=x onerror=\\x32\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x onerror=\\x32\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<img src=x onerror=\\x00\"javascript:alert(1)\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=x onerror=\\x00\"javascript:alert(1)\">"}));

	result = con.Query("SELECT '<a href=java&#1&#2&#3&#4&#5&#6&#7&#8&#11&#12script:javascript:alert(1)>XXX</a>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=java&#1&#2&#3&#4&#5&#6&#7&#8&#11&#12script:javascript:alert(1)>XXX</a>"}));

	result = con.Query("SELECT '<img src=\"x` `<script>javascript:alert(1)</script>\"` `>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src=\"x` `<script>javascript:alert(1)</script>\"` `>"}));

	result = con.Query("SELECT '<img src onerror /\" ''\"= alt=javascript:alert(1)//\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<img src onerror /\" '\"= alt=javascript:alert(1)//\">"}));

	result = con.Query("SELECT '<title onpropertychange=javascript:alert(1)></title><title title=>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<title onpropertychange=javascript:alert(1)></title><title title=>"}));

	result = con.Query("SELECT '<a href=http://foo.bar/#x=`y></a><img alt=\"`><img src=x:x onerror=javascript:alert(1)></a>\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<a href=http://foo.bar/#x=`y></a><img alt=\"`><img src=x:x onerror=javascript:alert(1)></a>\">"}));

	result = con.Query("SELECT '<!--[if]><script>javascript:alert(1)</script -->'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<!--[if]><script>javascript:alert(1)</script -->"}));

	result = con.Query("SELECT '<!--[if<img src=x onerror=javascript:alert(1)//]> -->'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<!--[if<img src=x onerror=javascript:alert(1)//]> -->"}));

	result = con.Query("SELECT '<script src=\"/\\%(jscript)s\"></script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<script src=\"/\\%(jscript)s\"></script>"}));

	result = con.Query("SELECT '<script src=\"\\\\%(jscript)s\"></script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<script src=\"\\\\%(jscript)s\"></script>"}));

	result = con.Query("SELECT '<IMG \"\"\"><SCRIPT>alert(\"XSS\")</SCRIPT>\">'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<IMG \"\"\"><SCRIPT>alert(\"XSS\")</SCRIPT>\">"}));

	result = con.Query("SELECT '<IMG SRC=javascript:alert(String.fromCharCode(88,83,83))>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<IMG SRC=javascript:alert(String.fromCharCode(88,83,83))>"}));

	result = con.Query("SELECT '<IMG SRC=&#106;&#97;&#118;&#97;&#115;&#99;&#114;&#105;&#112;&#116;&#58;&#97;&#108;&#101;&#114;&#116;&#40;&#39;&#88;&#83;&#83;&#39;&#41;>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<IMG SRC=&#106;&#97;&#118;&#97;&#115;&#99;&#114;&#105;&#112;&#116;&#58;&#97;&#108;&#101;&#114;&#116;&#40;&#39;&#88;&#83;&#83;&#39;&#41;>"}));

	result = con.Query("SELECT '<IMG SRC=&#0000106&#0000097&#0000118&#0000097&#0000115&#0000099&#0000114&#0000105&#0000112&#0000116&#0000058&#0000097&#0000108&#0000101&#0000114&#0000116&#0000040&#0000039&#0000088&#0000083&#0000083&#0000039&#0000041>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<IMG SRC=&#0000106&#0000097&#0000118&#0000097&#0000115&#0000099&#0000114&#0000105&#0000112&#0000116&#0000058&#0000097&#0000108&#0000101&#0000114&#0000116&#0000040&#0000039&#0000088&#0000083&#0000083&#0000039&#0000041>"}));

	result = con.Query("SELECT '<IMG SRC=&#x6A&#x61&#x76&#x61&#x73&#x63&#x72&#x69&#x70&#x74&#x3A&#x61&#x6C&#x65&#x72&#x74&#x28&#x27&#x58&#x53&#x53&#x27&#x29>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<IMG SRC=&#x6A&#x61&#x76&#x61&#x73&#x63&#x72&#x69&#x70&#x74&#x3A&#x61&#x6C&#x65&#x72&#x74&#x28&#x27&#x58&#x53&#x53&#x27&#x29>"}));

	result = con.Query("SELECT '<SCRIPT/XSS SRC=\"http://ha.ckers.org/xss.js\"></SCRIPT>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<SCRIPT/XSS SRC=\"http://ha.ckers.org/xss.js\"></SCRIPT>"}));

	result = con.Query("SELECT '<BODY onload!#$%&()*~+-_.,:;?@[/|\\]^`=alert(\"XSS\")>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<BODY onload!#$%&()*~+-_.,:;?@[/|\\]^`=alert(\"XSS\")>"}));

	result = con.Query("SELECT '<SCRIPT/SRC=\"http://ha.ckers.org/xss.js\"></SCRIPT>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<SCRIPT/SRC=\"http://ha.ckers.org/xss.js\"></SCRIPT>"}));

	result = con.Query("SELECT '<<SCRIPT>alert(\"XSS\");//<</SCRIPT>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<<SCRIPT>alert(\"XSS\");//<</SCRIPT>"}));

	result = con.Query("SELECT '<SCRIPT SRC=http://ha.ckers.org/xss.js?< B >'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<SCRIPT SRC=http://ha.ckers.org/xss.js?< B >"}));

	result = con.Query("SELECT '<SCRIPT SRC=//ha.ckers.org/.j>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<SCRIPT SRC=//ha.ckers.org/.j>"}));

	result = con.Query("SELECT '<iframe src=http://ha.ckers.org/scriptlet.html <'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<iframe src=http://ha.ckers.org/scriptlet.html <"}));

	result = con.Query("SELECT '<u oncopy=alert()> Copy me</u>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<u oncopy=alert()> Copy me</u>"}));

	result = con.Query("SELECT '<i onwheel=alert(1)> Scroll over me </i>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<i onwheel=alert(1)> Scroll over me </i>"}));

	result = con.Query("SELECT '<plaintext>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<plaintext>"}));

	result = con.Query("SELECT 'http://a/%%30%30'");
	REQUIRE(CHECK_COLUMN(result, 0, {"http://a/%%30%30"}));

	result = con.Query("SELECT '</textarea><script>alert(123)</script>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"</textarea><script>alert(123)</script>"}));

	result = con.Query("SELECT '#	SQL Injection'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	SQL Injection"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which can cause a SQL injection if inputs are not sanitized'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which can cause a SQL injection if inputs are not sanitized"}));

	result = con.Query("SELECT '1;DROP TABLE users'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1;DROP TABLE users"}));

	result = con.Query("SELECT '1''; DROP TABLE users-- 1'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1'; DROP TABLE users-- 1"}));

	result = con.Query("SELECT ''' OR 1=1 -- 1'");
	REQUIRE(CHECK_COLUMN(result, 0, {"' OR 1=1 -- 1"}));

	result = con.Query("SELECT ''' OR ''1''=''1'");
	REQUIRE(CHECK_COLUMN(result, 0, {"' OR '1'='1"}));

	result = con.Query("SELECT ' '");
	REQUIRE(CHECK_COLUMN(result, 0, {" "}));

	result = con.Query("SELECT '%'");
	REQUIRE(CHECK_COLUMN(result, 0, {"%"}));

	result = con.Query("SELECT '_'");
	REQUIRE(CHECK_COLUMN(result, 0, {"_"}));

	result = con.Query("SELECT '#	Server Code Injection'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Server Code Injection"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which can cause user to run code on server as a privileged user (c.f. https://news.ycombinator.com/item?id=7665153)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which can cause user to run code on server as a privileged user (c.f. https://news.ycombinator.com/item?id=7665153)"}));

	result = con.Query("SELECT '-'");
	REQUIRE(CHECK_COLUMN(result, 0, {"-"}));

	result = con.Query("SELECT '--'");
	REQUIRE(CHECK_COLUMN(result, 0, {"--"}));

	result = con.Query("SELECT '--version'");
	REQUIRE(CHECK_COLUMN(result, 0, {"--version"}));

	result = con.Query("SELECT '--help'");
	REQUIRE(CHECK_COLUMN(result, 0, {"--help"}));

	result = con.Query("SELECT '$USER'");
	REQUIRE(CHECK_COLUMN(result, 0, {"$USER"}));

	result = con.Query("SELECT '/dev/null; touch /tmp/blns.fail ; echo'");
	REQUIRE(CHECK_COLUMN(result, 0, {"/dev/null; touch /tmp/blns.fail ; echo"}));

	result = con.Query("SELECT '`touch /tmp/blns.fail`'");
	REQUIRE(CHECK_COLUMN(result, 0, {"`touch /tmp/blns.fail`"}));

	result = con.Query("SELECT '$(touch /tmp/blns.fail)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"$(touch /tmp/blns.fail)"}));

	result = con.Query("SELECT '@{[system \"touch /tmp/blns.fail\"]}'");
	REQUIRE(CHECK_COLUMN(result, 0, {"@{[system \"touch /tmp/blns.fail\"]}"}));

	result = con.Query("SELECT '#	Command Injection (Ruby)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Command Injection (Ruby)"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which can call system commands within Ruby/Rails applications'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which can call system commands within Ruby/Rails applications"}));

	result = con.Query("SELECT 'System(\"ls -al /\")'");
	REQUIRE(CHECK_COLUMN(result, 0, {"System(\"ls -al /\")"}));

	result = con.Query("SELECT '`ls -al /`'");
	REQUIRE(CHECK_COLUMN(result, 0, {"`ls -al /`"}));

	result = con.Query("SELECT 'Kernel.exec(\"ls -al /\")'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Kernel.exec(\"ls -al /\")"}));

	result = con.Query("SELECT 'Kernel.exit(1)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Kernel.exit(1)"}));

	result = con.Query("SELECT '#      XXE Injection (XML)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#      XXE Injection (XML)"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	String which can reveal system files when parsed by a badly configured XML parser'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	String which can reveal system files when parsed by a badly configured XML parser"}));

	result = con.Query("SELECT '<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><!DOCTYPE foo [ <!ELEMENT foo ANY ><!ENTITY xxe SYSTEM \"file:///etc/passwd\" >]><foo>&xxe;</foo>'");
	REQUIRE(CHECK_COLUMN(result, 0, {"<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><!DOCTYPE foo [ <!ELEMENT foo ANY ><!ENTITY xxe SYSTEM \"file:///etc/passwd\" >]><foo>&xxe;</foo>"}));

	result = con.Query("SELECT '#	Unwanted Interpolation'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Unwanted Interpolation"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which can be accidentally expanded into different strings if evaluated in the wrong context, e.g. used as a printf format string or via Perl or shell eval. Might expose sensitive data from the program doing the interpolation, or might just represent the wrong string.'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which can be accidentally expanded into different strings if evaluated in the wrong context, e.g. used as a printf format string or via Perl or shell eval. Might expose sensitive data from the program doing the interpolation, or might just represent the wrong string."}));

	result = con.Query("SELECT '$HOME'");
	REQUIRE(CHECK_COLUMN(result, 0, {"$HOME"}));

	result = con.Query("SELECT '%d'");
	REQUIRE(CHECK_COLUMN(result, 0, {"%d"}));

	result = con.Query("SELECT '%s%s%s%s%s'");
	REQUIRE(CHECK_COLUMN(result, 0, {"%s%s%s%s%s"}));

	result = con.Query("SELECT '{0}'");
	REQUIRE(CHECK_COLUMN(result, 0, {"{0}"}));

	result = con.Query("SELECT '%*.*s'");
	REQUIRE(CHECK_COLUMN(result, 0, {"%*.*s"}));

	result = con.Query("SELECT '%@'");
	REQUIRE(CHECK_COLUMN(result, 0, {"%@"}));

	result = con.Query("SELECT '%n'");
	REQUIRE(CHECK_COLUMN(result, 0, {"%n"}));

	result = con.Query("SELECT 'File:///'");
	REQUIRE(CHECK_COLUMN(result, 0, {"File:///"}));

	result = con.Query("SELECT '#	File Inclusion'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	File Inclusion"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which can cause user to pull in files that should not be a part of a web server'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which can cause user to pull in files that should not be a part of a web server"}));

	result = con.Query("SELECT '../../../../../../../../../../../etc/passwd%00'");
	REQUIRE(CHECK_COLUMN(result, 0, {"../../../../../../../../../../../etc/passwd%00"}));

	result = con.Query("SELECT '../../../../../../../../../../../etc/hosts'");
	REQUIRE(CHECK_COLUMN(result, 0, {"../../../../../../../../../../../etc/hosts"}));

	result = con.Query("SELECT '#	Known CVEs and Vulnerabilities'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Known CVEs and Vulnerabilities"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings that test for known vulnerabilities'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings that test for known vulnerabilities"}));

	result = con.Query("SELECT '() { 0; }; touch /tmp/blns.shellshock1.fail;'");
	REQUIRE(CHECK_COLUMN(result, 0, {"() { 0; }; touch /tmp/blns.shellshock1.fail;"}));

	result = con.Query("SELECT '() { _; } >_[$($())] { touch /tmp/blns.shellshock2.fail; }'");
	REQUIRE(CHECK_COLUMN(result, 0, {"() { _; } >_[$($())] { touch /tmp/blns.shellshock2.fail; }"}));

	result = con.Query("SELECT '+++ATH0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"+++ATH0"}));

	result = con.Query("SELECT '#	MSDOS/Windows Special Filenames'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	MSDOS/Windows Special Filenames"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which are reserved characters in MSDOS/Windows'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which are reserved characters in MSDOS/Windows"}));

	result = con.Query("SELECT 'CON'");
	REQUIRE(CHECK_COLUMN(result, 0, {"CON"}));

	result = con.Query("SELECT 'PRN'");
	REQUIRE(CHECK_COLUMN(result, 0, {"PRN"}));

	result = con.Query("SELECT 'AUX'");
	REQUIRE(CHECK_COLUMN(result, 0, {"AUX"}));

	result = con.Query("SELECT 'CLOCK$'");
	REQUIRE(CHECK_COLUMN(result, 0, {"CLOCK$"}));

	result = con.Query("SELECT 'NUL'");
	REQUIRE(CHECK_COLUMN(result, 0, {"NUL"}));

	result = con.Query("SELECT 'A:'");
	REQUIRE(CHECK_COLUMN(result, 0, {"A:"}));

	result = con.Query("SELECT 'ZZ:'");
	REQUIRE(CHECK_COLUMN(result, 0, {"ZZ:"}));

	result = con.Query("SELECT 'COM1'");
	REQUIRE(CHECK_COLUMN(result, 0, {"COM1"}));

	result = con.Query("SELECT 'LPT1'");
	REQUIRE(CHECK_COLUMN(result, 0, {"LPT1"}));

	result = con.Query("SELECT 'LPT2'");
	REQUIRE(CHECK_COLUMN(result, 0, {"LPT2"}));

	result = con.Query("SELECT 'LPT3'");
	REQUIRE(CHECK_COLUMN(result, 0, {"LPT3"}));

	result = con.Query("SELECT 'COM2'");
	REQUIRE(CHECK_COLUMN(result, 0, {"COM2"}));

	result = con.Query("SELECT 'COM3'");
	REQUIRE(CHECK_COLUMN(result, 0, {"COM3"}));

	result = con.Query("SELECT 'COM4'");
	REQUIRE(CHECK_COLUMN(result, 0, {"COM4"}));

	result = con.Query("SELECT '#   IRC specific strings'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#   IRC specific strings"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#   Strings that may occur on IRC clients that make security products freak out'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#   Strings that may occur on IRC clients that make security products freak out"}));

	result = con.Query("SELECT 'DCC SEND STARTKEYLOGGER 0 0 0'");
	REQUIRE(CHECK_COLUMN(result, 0, {"DCC SEND STARTKEYLOGGER 0 0 0"}));

	result = con.Query("SELECT '#	Scunthorpe Problem'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Scunthorpe Problem"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Innocuous strings which may be blocked by profanity filters (https://en.wikipedia.org/wiki/Scunthorpe_problem)'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Innocuous strings which may be blocked by profanity filters (https://en.wikipedia.org/wiki/Scunthorpe_problem)"}));

	result = con.Query("SELECT 'Scunthorpe General Hospital'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Scunthorpe General Hospital"}));

	result = con.Query("SELECT 'Penistone Community Church'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Penistone Community Church"}));

	result = con.Query("SELECT 'Lightwater Country Park'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Lightwater Country Park"}));

	result = con.Query("SELECT 'Jimmy Clitheroe'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Jimmy Clitheroe"}));

	result = con.Query("SELECT 'Horniman Museum'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Horniman Museum"}));

	result = con.Query("SELECT 'shitake mushrooms'");
	REQUIRE(CHECK_COLUMN(result, 0, {"shitake mushrooms"}));

	result = con.Query("SELECT 'RomansInSussex.co.uk'");
	REQUIRE(CHECK_COLUMN(result, 0, {"RomansInSussex.co.uk"}));

	result = con.Query("SELECT 'http://www.cum.qc.ca/'");
	REQUIRE(CHECK_COLUMN(result, 0, {"http://www.cum.qc.ca/"}));

	result = con.Query("SELECT 'Craig Cockburn, Software Specialist'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Craig Cockburn, Software Specialist"}));

	result = con.Query("SELECT 'Linda Callahan'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Linda Callahan"}));

	result = con.Query("SELECT 'Dr. Herman I. Libshitz'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Dr. Herman I. Libshitz"}));

	result = con.Query("SELECT 'magna cum laude'");
	REQUIRE(CHECK_COLUMN(result, 0, {"magna cum laude"}));

	result = con.Query("SELECT 'Super Bowl XXX'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Super Bowl XXX"}));

	result = con.Query("SELECT 'medieval erection of parapets'");
	REQUIRE(CHECK_COLUMN(result, 0, {"medieval erection of parapets"}));

	result = con.Query("SELECT 'evaluate'");
	REQUIRE(CHECK_COLUMN(result, 0, {"evaluate"}));

	result = con.Query("SELECT 'mocha'");
	REQUIRE(CHECK_COLUMN(result, 0, {"mocha"}));

	result = con.Query("SELECT 'expression'");
	REQUIRE(CHECK_COLUMN(result, 0, {"expression"}));

	result = con.Query("SELECT 'Arsenal canal'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Arsenal canal"}));

	result = con.Query("SELECT 'classic'");
	REQUIRE(CHECK_COLUMN(result, 0, {"classic"}));

	result = con.Query("SELECT 'Tyson Gay'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Tyson Gay"}));

	result = con.Query("SELECT 'Dick Van Dyke'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Dick Van Dyke"}));

	result = con.Query("SELECT 'basement'");
	REQUIRE(CHECK_COLUMN(result, 0, {"basement"}));

	result = con.Query("SELECT '#	Human injection'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Human injection"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which may cause human to reinterpret worldview'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which may cause human to reinterpret worldview"}));

	result = con.Query("SELECT '#	Terminal escape codes'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Terminal escape codes"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which punish the fools who use cat/type on this file'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which punish the fools who use cat/type on this file"}));

	result = con.Query("SELECT 'Roses are [0;31mred[0m, violets are [0;34mblue. Hope you enjoy terminal hue'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Roses are [0;31mred[0m, violets are [0;34mblue. Hope you enjoy terminal hue"}));

	result = con.Query("SELECT 'But now...[20Cfor my greatest trick...[8m'");
	REQUIRE(CHECK_COLUMN(result, 0, {"But now...[20Cfor my greatest trick...[8m"}));

	result = con.Query("SELECT 'The quick brown fox... [Beeeep]'");
	REQUIRE(CHECK_COLUMN(result, 0, {"The quick brown fox... [Beeeep]"}));

	result = con.Query("SELECT '#	iOS Vulnerabilities'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	iOS Vulnerabilities"}));

	result = con.Query("SELECT '#'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#"}));

	result = con.Query("SELECT '#	Strings which crashed iMessage in various versions of iOS'");
	REQUIRE(CHECK_COLUMN(result, 0, {"#	Strings which crashed iMessage in various versions of iOS"}));

	result = con.Query("SELECT 'Powerلُلُصّبُلُلصّبُررً ॣ ॣh ॣ ॣ冗'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Powerلُلُصّبُلُلصّبُررً ॣ ॣh ॣ ॣ冗"}));

	result = con.Query("SELECT '🏳0🌈️'");
	REQUIRE(CHECK_COLUMN(result, 0, {"🏳0🌈️"}));

	result = con.Query("SELECT 'జ్ఞ‌ా'");
	REQUIRE(CHECK_COLUMN(result, 0, {"జ్ఞ‌ా"}));
}
