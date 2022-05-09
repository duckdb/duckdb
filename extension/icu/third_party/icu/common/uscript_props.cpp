// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
*******************************************************************************
*   Copyright (C) 2013-2016, International Business Machines
*   Corporation and others.  All Rights Reserved.
*******************************************************************************
*   file name:  uscript_props.cpp
*   encoding:   UTF-8
*   tab size:   8 (not used)
*   indentation:4
*
*   created on: 2013feb16
*   created by: Markus W. Scherer
*/

#include "unicode/utypes.h"
#include "unicode/unistr.h"
#include "unicode/uscript.h"
#include "unicode/utf16.h"
#include "ustr_imp.h"
#include "cmemory.h"

namespace {

// Script metadata (script properties).
// See http://unicode.org/cldr/trac/browser/trunk/common/properties/scriptMetadata.txt

// 0 = NOT_ENCODED, no sample character, default false script properties.
// Bits 20.. 0: sample character

// Bits 23..21: usage
const int32_t up_UNKNOWN = 1 << 21;
const int32_t up_EXCLUSION = 2 << 21;
const int32_t up_LIMITED_USE = 3 << 21;
// st int32_t ASPIRATIONAL = 4 << 21; -- not used any more since Unicode 10
const int32_t up_RECOMMENDED = 5 << 21;

// Bits 31..24: Single-bit flags
const int32_t up_RTL = 1 << 24;
const int32_t up_LB_LETTERS = 1 << 25;
const int32_t up_CASED = 1 << 26;

const int32_t up_SCRIPT_PROPS[] = {
    // Begin copy-paste output from
    // tools/trunk/unicode/py/parsescriptmetadata.py
    0x0040 | up_RECOMMENDED,  // Zyyy
    0x0308 | up_RECOMMENDED,  // Zinh
    0x0628 | up_RECOMMENDED | up_RTL,  // Arab
    0x0531 | up_RECOMMENDED | up_CASED,  // Armn
    0x0995 | up_RECOMMENDED,  // Beng
    0x3105 | up_RECOMMENDED | up_LB_LETTERS,  // Bopo
    0x13C4 | up_LIMITED_USE | up_CASED,  // Cher
    0x03E2 | up_EXCLUSION | up_CASED,  // Copt
    0x042F | up_RECOMMENDED | up_CASED,  // Cyrl
    0x10414 | up_EXCLUSION | up_CASED,  // Dsrt
    0x0905 | up_RECOMMENDED,  // Deva
    0x12A0 | up_RECOMMENDED,  // Ethi
    0x10D3 | up_RECOMMENDED,  // Geor
    0x10330 | up_EXCLUSION,  // Goth
    0x03A9 | up_RECOMMENDED | up_CASED,  // Grek
    0x0A95 | up_RECOMMENDED,  // Gujr
    0x0A15 | up_RECOMMENDED,  // Guru
    0x5B57 | up_RECOMMENDED | up_LB_LETTERS,  // Hani
    0xAC00 | up_RECOMMENDED,  // Hang
    0x05D0 | up_RECOMMENDED | up_RTL,  // Hebr
    0x304B | up_RECOMMENDED | up_LB_LETTERS,  // Hira
    0x0C95 | up_RECOMMENDED,  // Knda
    0x30AB | up_RECOMMENDED | up_LB_LETTERS,  // Kana
    0x1780 | up_RECOMMENDED | up_LB_LETTERS,  // Khmr
    0x0EA5 | up_RECOMMENDED | up_LB_LETTERS,  // Laoo
    0x004C | up_RECOMMENDED | up_CASED,  // Latn
    0x0D15 | up_RECOMMENDED,  // Mlym
    0x1826 | up_EXCLUSION,  // Mong
    0x1000 | up_RECOMMENDED | up_LB_LETTERS,  // Mymr
    0x168F | up_EXCLUSION,  // Ogam
    0x10300 | up_EXCLUSION,  // Ital
    0x0B15 | up_RECOMMENDED,  // Orya
    0x16A0 | up_EXCLUSION,  // Runr
    0x0D85 | up_RECOMMENDED,  // Sinh
    0x0710 | up_LIMITED_USE | up_RTL,  // Syrc
    0x0B95 | up_RECOMMENDED,  // Taml
    0x0C15 | up_RECOMMENDED,  // Telu
    0x078C | up_RECOMMENDED | up_RTL,  // Thaa
    0x0E17 | up_RECOMMENDED | up_LB_LETTERS,  // Thai
    0x0F40 | up_RECOMMENDED,  // Tibt
    0x14C0 | up_LIMITED_USE,  // Cans
    0xA288 | up_LIMITED_USE | up_LB_LETTERS,  // Yiii
    0x1703 | up_EXCLUSION,  // Tglg
    0x1723 | up_EXCLUSION,  // Hano
    0x1743 | up_EXCLUSION,  // Buhd
    0x1763 | up_EXCLUSION,  // Tagb
    0x280E | up_UNKNOWN,  // Brai
    0x10800 | up_EXCLUSION | up_RTL,  // Cprt
    0x1900 | up_LIMITED_USE,  // Limb
    0x10000 | up_EXCLUSION,  // Linb
    0x10480 | up_EXCLUSION,  // Osma
    0x10450 | up_EXCLUSION,  // Shaw
    0x1950 | up_LIMITED_USE | up_LB_LETTERS,  // Tale
    0x10380 | up_EXCLUSION,  // Ugar
    0,
    0x1A00 | up_EXCLUSION,  // Bugi
    0x2C00 | up_EXCLUSION | up_CASED,  // Glag
    0x10A00 | up_EXCLUSION | up_RTL,  // Khar
    0xA800 | up_LIMITED_USE,  // Sylo
    0x1980 | up_LIMITED_USE | up_LB_LETTERS,  // Talu
    0x2D30 | up_LIMITED_USE,  // Tfng
    0x103A0 | up_EXCLUSION,  // Xpeo
    0x1B05 | up_LIMITED_USE,  // Bali
    0x1BC0 | up_LIMITED_USE,  // Batk
    0,
    0x11005 | up_EXCLUSION,  // Brah
    0xAA00 | up_LIMITED_USE,  // Cham
    0,
    0,
    0,
    0,
    0x13153 | up_EXCLUSION,  // Egyp
    0,
    0x5B57 | up_RECOMMENDED | up_LB_LETTERS,  // Hans
    0x5B57 | up_RECOMMENDED | up_LB_LETTERS,  // Hant
    0x16B1C | up_EXCLUSION,  // Hmng
    0x10CA1 | up_EXCLUSION | up_RTL | up_CASED,  // Hung
    0,
    0xA984 | up_LIMITED_USE,  // Java
    0xA90A | up_LIMITED_USE,  // Kali
    0,
    0,
    0x1C00 | up_LIMITED_USE,  // Lepc
    0x10647 | up_EXCLUSION,  // Lina
    0x0840 | up_LIMITED_USE | up_RTL,  // Mand
    0,
    0x10980 | up_EXCLUSION | up_RTL,  // Mero
    0x07CA | up_LIMITED_USE | up_RTL,  // Nkoo
    0x10C00 | up_EXCLUSION | up_RTL,  // Orkh
    0x1036B | up_EXCLUSION,  // Perm
    0xA840 | up_EXCLUSION,  // Phag
    0x10900 | up_EXCLUSION | up_RTL,  // Phnx
    0x16F00 | up_LIMITED_USE,  // Plrd
    0,
    0,
    0,
    0,
    0,
    0,
    0xA549 | up_LIMITED_USE,  // Vaii
    0,
    0x12000 | up_EXCLUSION,  // Xsux
    0,
    0xFDD0 | up_UNKNOWN,  // Zzzz
    0x102A0 | up_EXCLUSION,  // Cari
    0x304B | up_RECOMMENDED | up_LB_LETTERS,  // Jpan
    0x1A20 | up_LIMITED_USE | up_LB_LETTERS,  // Lana
    0x10280 | up_EXCLUSION,  // Lyci
    0x10920 | up_EXCLUSION | up_RTL,  // Lydi
    0x1C5A | up_LIMITED_USE,  // Olck
    0xA930 | up_EXCLUSION,  // Rjng
    0xA882 | up_LIMITED_USE,  // Saur
    0x1D850 | up_EXCLUSION,  // Sgnw
    0x1B83 | up_LIMITED_USE,  // Sund
    0,
    0xABC0 | up_LIMITED_USE,  // Mtei
    0x10840 | up_EXCLUSION | up_RTL,  // Armi
    0x10B00 | up_EXCLUSION | up_RTL,  // Avst
    0x11103 | up_LIMITED_USE,  // Cakm
    0xAC00 | up_RECOMMENDED,  // Kore
    0x11083 | up_EXCLUSION,  // Kthi
    0x10AD8 | up_EXCLUSION | up_RTL,  // Mani
    0x10B60 | up_EXCLUSION | up_RTL,  // Phli
    0x10B8F | up_EXCLUSION | up_RTL,  // Phlp
    0,
    0x10B40 | up_EXCLUSION | up_RTL,  // Prti
    0x0800 | up_EXCLUSION | up_RTL,  // Samr
    0xAA80 | up_LIMITED_USE | up_LB_LETTERS,  // Tavt
    0,
    0,
    0xA6A0 | up_LIMITED_USE,  // Bamu
    0xA4D0 | up_LIMITED_USE,  // Lisu
    0,
    0x10A60 | up_EXCLUSION | up_RTL,  // Sarb
    0x16AE6 | up_EXCLUSION,  // Bass
    0x1BC20 | up_EXCLUSION,  // Dupl
    0x10500 | up_EXCLUSION,  // Elba
    0x11315 | up_EXCLUSION,  // Gran
    0,
    0,
    0x1E802 | up_EXCLUSION | up_RTL,  // Mend
    0x109A0 | up_EXCLUSION | up_RTL,  // Merc
    0x10A95 | up_EXCLUSION | up_RTL,  // Narb
    0x10896 | up_EXCLUSION | up_RTL,  // Nbat
    0x10873 | up_EXCLUSION | up_RTL,  // Palm
    0x112BE | up_EXCLUSION,  // Sind
    0x118B4 | up_EXCLUSION | up_CASED,  // Wara
    0,
    0,
    0x16A4F | up_EXCLUSION,  // Mroo
    0x1B1C4 | up_EXCLUSION | up_LB_LETTERS,  // Nshu
    0x11183 | up_EXCLUSION,  // Shrd
    0x110D0 | up_EXCLUSION,  // Sora
    0x11680 | up_EXCLUSION,  // Takr
    0x18229 | up_EXCLUSION | up_LB_LETTERS,  // Tang
    0,
    0x14400 | up_EXCLUSION,  // Hluw
    0x11208 | up_EXCLUSION,  // Khoj
    0x11484 | up_EXCLUSION,  // Tirh
    0x10537 | up_EXCLUSION,  // Aghb
    0x11152 | up_EXCLUSION,  // Mahj
    0x11717 | up_EXCLUSION | up_LB_LETTERS,  // Ahom
    0x108F4 | up_EXCLUSION | up_RTL,  // Hatr
    0x1160E | up_EXCLUSION,  // Modi
    0x1128F | up_EXCLUSION,  // Mult
    0x11AC0 | up_EXCLUSION,  // Pauc
    0x1158E | up_EXCLUSION,  // Sidd
    0x1E909 | up_LIMITED_USE | up_RTL | up_CASED,  // Adlm
    0x11C0E | up_EXCLUSION,  // Bhks
    0x11C72 | up_EXCLUSION,  // Marc
    0x11412 | up_LIMITED_USE,  // Newa
    0x104B5 | up_LIMITED_USE | up_CASED,  // Osge
    0x5B57 | up_RECOMMENDED | up_LB_LETTERS,  // Hanb
    0x1112 | up_RECOMMENDED,  // Jamo
    0,
    0x11D10 | up_EXCLUSION,  // Gonm
    0x11A5C | up_EXCLUSION,  // Soyo
    0x11A0B | up_EXCLUSION,  // Zanb
    0x1180B | up_EXCLUSION,  // Dogr
    0x11D71 | up_LIMITED_USE,  // Gong
    0x11EE5 | up_EXCLUSION,  // Maka
    0x16E40 | up_EXCLUSION | up_CASED,  // Medf
    0x10D12 | up_LIMITED_USE | up_RTL,  // Rohg
    0x10F42 | up_EXCLUSION | up_RTL,  // Sogd
    0x10F19 | up_EXCLUSION | up_RTL,  // Sogo
    0x10FF1 | up_EXCLUSION | up_RTL,  // Elym
    0x1E108 | up_LIMITED_USE,  // Hmnp
    0x119CE | up_EXCLUSION,  // Nand
    0x1E2E1 | up_LIMITED_USE,  // Wcho
    0x10FBF | up_EXCLUSION | up_RTL,  // Chrs
    0x1190C | up_EXCLUSION,  // Diak
    0x18C65 | up_EXCLUSION | up_LB_LETTERS,  // Kits
    0x10E88 | up_EXCLUSION | up_RTL,  // Yezi
    // End copy-paste from parsescriptmetadata.py
};

int32_t getScriptProps(UScriptCode script) {
    if (0 <= script && script < UPRV_LENGTHOF(up_SCRIPT_PROPS)) {
        return up_SCRIPT_PROPS[script];
    } else {
        return 0;
    }
}

}  // namespace

U_CAPI int32_t U_EXPORT2
uscript_getSampleString(UScriptCode script, UChar *dest, int32_t capacity, UErrorCode *pErrorCode) {
    if(U_FAILURE(*pErrorCode)) { return 0; }
    if(capacity < 0 || (capacity > 0 && dest == NULL)) {
        *pErrorCode = U_ILLEGAL_ARGUMENT_ERROR;
        return 0;
    }
    int32_t sampleChar = getScriptProps(script) & 0x1fffff;
    int32_t length;
    if(sampleChar == 0) {
        length = 0;
    } else {
        length = U16_LENGTH(sampleChar);
        if(length <= capacity) {
            int32_t i = 0;
            U16_APPEND_UNSAFE(dest, i, sampleChar);
        }
    }
    return u_terminateUChars(dest, capacity, length, pErrorCode);
}

U_COMMON_API icu::UnicodeString U_EXPORT2
uscript_getSampleUnicodeString(UScriptCode script) {
    icu::UnicodeString sample;
    int32_t sampleChar = getScriptProps(script) & 0x1fffff;
    if(sampleChar != 0) {
        sample.append(sampleChar);
    }
    return sample;
}

U_CAPI UScriptUsage U_EXPORT2
uscript_getUsage(UScriptCode script) {
    return (UScriptUsage)((getScriptProps(script) >> 21) & 7);
}

U_CAPI UBool U_EXPORT2
uscript_isRightToLeft(UScriptCode script) {
    return (getScriptProps(script) & up_RTL) != 0;
}

U_CAPI UBool U_EXPORT2
uscript_breaksBetweenLetters(UScriptCode script) {
    return (getScriptProps(script) & up_LB_LETTERS) != 0;
}

U_CAPI UBool U_EXPORT2
uscript_isCased(UScriptCode script) {
    return (getScriptProps(script) & up_CASED) != 0;
}
