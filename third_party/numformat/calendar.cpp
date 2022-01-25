#include <stdio.h>
#include <string.h>
#include "calendar.h"

namespace duckdb_numformat {

#define erDUMP_ICU_CALENDAR 0
#define erDUMP_I18N_CALENDAR 0
#define DUMP_ICU_CAL_MSG(x)
#define DUMP_I18N_CAL_MSG(x)

//#define ERROR RuntimeException()

const double MILLISECONDS_PER_DAY = 1000.0 * 60.0 * 60.0 * 24.0;

Calendar::Calendar(LocaleData* pFormatterP)
{
	pFormatter = pFormatterP;
    init(NULL);
	pNullDate = new Date(30, 12, 1899);
	memset(fieldValue, 0, sizeof(fieldValue));
}
Calendar::Calendar(Era *_eraArray)
{
    init(_eraArray);
	pNullDate = new Date(30, 12, 1899);
	memset(fieldValue, 0, sizeof(fieldValue));
}
void Calendar::init(Era *_eraArray)
{
    eraArray=_eraArray;
	aEpochStart = DateTime(Date(1, 1, 1970));
	timeInDays = 0.0;
}

Calendar::~Calendar()
{
	delete pNullDate;
}

static Era gengou_eraArray[] = {
    {1868,  1,  1},
    {1912,  7, 30},
    {1926, 12, 25},
    {1989,  1,  8},
    {2019,  5,  1},
    {0, 0,  0}
};

void Calendar::setValue( sal_Int16 fieldIndex, sal_Int16 value )
{
	if (fieldIndex < 0 || FIELD_INDEX_COUNT <= fieldIndex) {
		return;
	}
    fieldSet |= (1 << fieldIndex);
    fieldValue[fieldIndex] = value;
}

bool Calendar::getCombinedOffset( sal_Int32 & o_nOffset,
        sal_Int16 nParentFieldIndex, sal_Int16 nChildFieldIndex ) const
{
    o_nOffset = 0;
    bool bFieldsSet = false;
    if (fieldSet & (1 << nParentFieldIndex))
    {
        bFieldsSet = true;
        o_nOffset = static_cast<sal_Int32>( fieldValue[nParentFieldIndex]) * 60000;
    }
    if (fieldSet & (1 << nChildFieldIndex))
    {
        bFieldsSet = true;
        if (o_nOffset < 0)
            o_nOffset -= static_cast<sal_uInt16>( fieldValue[nChildFieldIndex]);
        else
            o_nOffset += static_cast<sal_uInt16>( fieldValue[nChildFieldIndex]);
    }
    return bFieldsSet;
}

bool Calendar::getZoneOffset( sal_Int32 & o_nOffset ) const
{
    return getCombinedOffset( o_nOffset, CalendarFieldIndex::ZONE_OFFSET,
            CalendarFieldIndex::ZONE_OFFSET_SECOND_MILLIS);
}

bool Calendar::getDSTOffset( sal_Int32 & o_nOffset ) const
{
    return getCombinedOffset( o_nOffset, CalendarFieldIndex::DST_OFFSET,
            CalendarFieldIndex::DST_OFFSET_SECOND_MILLIS);
}

void Calendar::submitFields()
{
    sal_Int32 nZoneOffset, nDSTOffset;
	getZoneOffset(nZoneOffset);
	getDSTOffset(nDSTOffset);
}

void Calendar::submitValues( sal_Int32 nYear,
        sal_Int32 nMonth, sal_Int32 nDay, sal_Int32 nHour, sal_Int32 nMinute,
        sal_Int32 nSecond, sal_Int32 nMilliSecond, sal_Int32 nZone, sal_Int32 nDST )
{
    submitFields();
}

static void lcl_setCombinedOffsetFieldValues( sal_Int32 nValue,
        sal_Int16 rFieldSetValue[], sal_Int16 rFieldValue[],
        sal_Int16 nParentFieldIndex, sal_Int16 nChildFieldIndex )
{
    sal_Int32 nTrunc = nValue / 60000;
    rFieldSetValue[nParentFieldIndex] = rFieldValue[nParentFieldIndex] =
        static_cast<sal_Int16>( nTrunc);
    sal_uInt16 nMillis = static_cast<sal_uInt16>( abs( nValue - nTrunc * 60000));
    rFieldSetValue[nChildFieldIndex] = rFieldValue[nChildFieldIndex] =
        static_cast<sal_Int16>( nMillis);
}

sal_Int16 Calendar::getValue( sal_Int16 fieldIndex )
{
	if (fieldIndex < 0 || FIELD_INDEX_COUNT <= fieldIndex)
	{
		return -1;
	}

    return fieldValue[fieldIndex];
}

sal_Bool Calendar::isValid()
{
#if (0)
        if (fieldSet) {
            sal_Int32 tmp = fieldSet;
            setValue();
			sal_Int16 fieldSetValue[FIELD_INDEX_COUNT];
			memcpy(fieldSetValue, fieldValue, sizeof(fieldSetValue));
            getValue();
            for ( sal_Int16 fieldIndex = 0; fieldIndex < FIELD_INDEX_COUNT; fieldIndex++ ) {
                // compare only with fields that are set and reset fieldSet[]
                if (tmp & (1 << fieldIndex)) {
                    if (fieldSetValue[fieldIndex] != fieldValue[fieldIndex])
                        return sal_False;
                }
            }
        }
#endif
        return true;
}

static sal_Int32  DisplayCode2FieldIndex(sal_Int32 nCalendarDisplayCode)
{
    switch( nCalendarDisplayCode ) {
        case CalendarDisplayCode::SHORT_DAY: 
        case CalendarDisplayCode::LONG_DAY: 
            return CalendarFieldIndex::DAY_OF_MONTH;
        case CalendarDisplayCode::SHORT_DAY_NAME: 
        case CalendarDisplayCode::LONG_DAY_NAME: 
            return CalendarFieldIndex::DAY_OF_WEEK;
        case CalendarDisplayCode::SHORT_QUARTER: 
        case CalendarDisplayCode::LONG_QUARTER: 
        case CalendarDisplayCode::SHORT_MONTH: 
        case CalendarDisplayCode::LONG_MONTH: 
        case CalendarDisplayCode::SHORT_MONTH_NAME: 
        case CalendarDisplayCode::LONG_MONTH_NAME: 
            return CalendarFieldIndex::MONTH;
        case CalendarDisplayCode::SHORT_YEAR: 
        case CalendarDisplayCode::LONG_YEAR: 
            return CalendarFieldIndex::YEAR;
        case CalendarDisplayCode::SHORT_ERA: 
        case CalendarDisplayCode::LONG_ERA: 
            return CalendarFieldIndex::ERA;
        case CalendarDisplayCode::SHORT_YEAR_AND_ERA: 
        case CalendarDisplayCode::LONG_YEAR_AND_ERA: 
            return CalendarFieldIndex::YEAR;
        default:
            return 0;
    }
}

std::wstring Calendar::getDisplayName( sal_Int16 displayIndex, sal_Int16 idx, sal_Int16 nameType )
{
        std::wstring aStr;

        switch( displayIndex ) {
            case CalendarDisplayIndex::AM_PM:/* ==0 */
                if (idx == 0) aStr = pFormatter->getTimeAM();
                else if (idx == 1) aStr = pFormatter->getTimePM();
                else return L"";
                break;
            case CalendarDisplayIndex::DAY:
                if( idx >= pFormatter->getDayOfWeekSize() ) return L"";
                if (nameType == 0) aStr = pFormatter->getDayOfWeekAbbrvName(idx);
                else if (nameType == 1) aStr = pFormatter->getDayOfWeekFullName(idx);
                else return L"";
                break;
            case CalendarDisplayIndex::MONTH:
                if( idx >= pFormatter->getMonthsOfYearSize()) return L"";
                if (nameType == 0) aStr = pFormatter->getMonthsOfYearAbbrvName(idx);
                else if (nameType == 1) aStr = pFormatter->getMonthsOfYearFullName(idx);
                else return L"";
                break;
            case CalendarDisplayIndex::ERA:
                if( idx >= pFormatter->getEraSize() ) return L"";
                if (nameType == 0) aStr = pFormatter->getEraAbbrvName(idx);
                else if (nameType == 1) aStr = pFormatter->getEraFullName(idx);
                else return L"";
                break;
            case CalendarDisplayIndex::YEAR:
                break;
            default:
				return L"";
        }
        return aStr;
}

// Methods in XExtendedCalendar
std::wstring  Calendar::getDisplayString( sal_Int32 nCalendarDisplayCode, sal_Int16 nNativeNumberMode )
{
    sal_Int16 value = getValue((sal_Int16)( DisplayCode2FieldIndex(nCalendarDisplayCode) ));
    std::wstring aOUStr;

    if (nCalendarDisplayCode == CalendarDisplayCode::SHORT_QUARTER ||
            nCalendarDisplayCode == CalendarDisplayCode::LONG_QUARTER) {
        sal_Int16 quarter = value / 3;
        // Since this base class method may be called by derived calendar
        // classes where a year consists of more than 12 months we need a check
        // to not run out of bounds of reserved quarter words. Perhaps a more
        // clean way (instead of dividing by 3) would be to first get the
        // number of months, divide by 4 and then use that result to divide the
        // actual month value.
        if ( quarter > 3 )
            quarter = 3;    
        quarter = (sal_Int16)( quarter + ((nCalendarDisplayCode == CalendarDisplayCode::SHORT_QUARTER) ?
            reservedWords::QUARTER1_ABBREVIATION : reservedWords::QUARTER1_WORD) );
        aOUStr = pFormatter->getReservedWord(quarter);
    } else {
        // The "#100211# - checked" comments serve for detection of "use of
        // sprintf is safe here" conditions. An sprintf encountered without
        // having that comment triggers alarm ;-)
        wchar_t aStr[10];
        switch( nCalendarDisplayCode ) {
            case CalendarDisplayCode::SHORT_MONTH: 
                value += 1;     // month is zero based
                // fall thru
            case CalendarDisplayCode::SHORT_DAY: 
                swprintf(aStr, sizeof(aStr), L"%d", value);     // #100211# - checked
                break;
            case CalendarDisplayCode::LONG_YEAR: 
                //if (aCalendar.Name.equalsAscii("gengou"))
                //    swprintf(aStr, L"%02d", value);     // #100211# - checked
                //else
                    swprintf(aStr, sizeof(aStr), L"%d", value);     // #100211# - checked
                break;
            case CalendarDisplayCode::LONG_MONTH: 
                value += 1;     // month is zero based
                swprintf(aStr, sizeof(aStr), L"%02d", value);   // #100211# - checked
                break;
            case CalendarDisplayCode::SHORT_YEAR: 
                // Take last 2 digits, or only one if value<10, for example,
                // in case of the Gengou calendar.
                // #i116701# For values in non-Gregorian era years use all
                // digits.
                if (value < 100 || eraArray)
                    swprintf(aStr, sizeof(aStr), L"%d", value); // #100211# - checked
                else
                    swprintf(aStr, sizeof(aStr), L"%02d", value % 100); // #100211# - checked
                break;
            case CalendarDisplayCode::LONG_DAY: 
                swprintf(aStr, sizeof(aStr), L"%02d", value);   // #100211# - checked
                break;

            case CalendarDisplayCode::SHORT_DAY_NAME: 
                return getDisplayName(CalendarDisplayIndex::DAY, value, 0);
            case CalendarDisplayCode::LONG_DAY_NAME: 
                return getDisplayName(CalendarDisplayIndex::DAY, value, 1);
            case CalendarDisplayCode::SHORT_MONTH_NAME: 
                return getDisplayName(CalendarDisplayIndex::MONTH, value, 0);
            case CalendarDisplayCode::LONG_MONTH_NAME: 
                return getDisplayName(CalendarDisplayIndex::MONTH, value, 1);
            case CalendarDisplayCode::SHORT_ERA: 
                return getDisplayName(CalendarDisplayIndex::ERA, value, 0);
            case CalendarDisplayCode::LONG_ERA: 
                return getDisplayName(CalendarDisplayIndex::ERA, value, 1);

            case CalendarDisplayCode::SHORT_YEAR_AND_ERA: 
                return  getDisplayString( CalendarDisplayCode::SHORT_ERA, nNativeNumberMode ) +
                    getDisplayString( CalendarDisplayCode::SHORT_YEAR, nNativeNumberMode );

            case CalendarDisplayCode::LONG_YEAR_AND_ERA: 
                return  getDisplayString( CalendarDisplayCode::LONG_ERA, nNativeNumberMode ) +
                    getDisplayString( CalendarDisplayCode::LONG_YEAR, nNativeNumberMode );

            default:
                return L"";
        }
        aOUStr = aStr;
    }
  //  if (nNativeNumberMode > 0) {
  //      // For Japanese calendar, first year calls GAN, see bug 111668 for detail.
		//if (eraArray == gengou_eraArray && value == 1
		//	&& (nCalendarDisplayCode == CalendarDisplayCode::SHORT_YEAR ||
		//		nCalendarDisplayCode == CalendarDisplayCode::LONG_YEAR)
		//	&& (nNativeNumberMode == NativeNumberMode::NATNUM1 ||
		//		nNativeNumberMode == NativeNumberMode::NATNUM2)) {
		//	static sal_Unicode gan = 0x5143;
		//	return std::wstring(&gan, 1);
		//}
  //      sal_Int16 nNatNum = nNativeNumberMode;
  //      if (nNatNum > 0)
  //          return aNatNum.getNativeNumberString(aOUStr, aLocale, nNatNum);
  //  }
    return aOUStr;
}

void Calendar::setDateTime(double timeInDays_val)
{
#if (0)
	UErrorCode status;
	body->setTime(timeInDays_val * U_MILLIS_PER_DAY, status = U_ZERO_ERROR);
	if (!U_SUCCESS(status)) throw ERROR;
	getValue();
#else
	double fDiff = DateTime(*(GetNullDate())) - getEpochStart();
	timeInDays = timeInDays_val - fDiff;
	DateTime dt;
	dt += timeInDays;
	setValue(CalendarFieldIndex::AM_PM, dt.GetHour() < 12 ? 0 : 1);
	setValue(CalendarFieldIndex::DAY_OF_MONTH, dt.GetDay());
	setValue(CalendarFieldIndex::DAY_OF_WEEK, dt.GetDayOfWeek() + 1);
	setValue(CalendarFieldIndex::DAY_OF_YEAR, dt.GetDayOfYear());
	setValue(CalendarFieldIndex::HOUR, dt.GetHour());
	setValue(CalendarFieldIndex::MINUTE, dt.GetMin());
	setValue(CalendarFieldIndex::SECOND, dt.GetSec());
	setValue(CalendarFieldIndex::MILLISECOND, dt.Get100Sec() * 10);
	setValue(CalendarFieldIndex::WEEK_OF_YEAR, dt.GetWeekOfYear());
	setValue(CalendarFieldIndex::YEAR, dt.GetYear());
	setValue(CalendarFieldIndex::MONTH, dt.GetMonth() - 1);
#endif
}

double Calendar::getDateTime()
{
#if (0)
	////if (fieldSet) {
	////	setValue();
	////	getValue();
	////}
	UErrorCode status;
	double r = body->getTime(status = U_ZERO_ERROR);
	if (!U_SUCCESS(status)) throw ERROR;
	return r / U_MILLIS_PER_DAY;
#else
	return timeInDays;
#endif
}

sal_Int32 Calendar::getCombinedOffsetInMillis(sal_Int16 nParentFieldIndex, sal_Int16 nChildFieldIndex)
{
	sal_Int32 nOffset = 0;
	nOffset = (sal_Int32)(getValue(nParentFieldIndex)) * 60000;
	sal_Int16 nSecondMillis = getValue(nChildFieldIndex);
	if (nOffset < 0)
		nOffset -= static_cast<sal_uInt16>(nSecondMillis);
	else
		nOffset += static_cast<sal_uInt16>(nSecondMillis);
	return nOffset;
}


sal_Int32 Calendar::getZoneOffsetInMillis()
{
	return getCombinedOffsetInMillis(CalendarFieldIndex::ZONE_OFFSET,
		CalendarFieldIndex::ZONE_OFFSET_SECOND_MILLIS);
}


sal_Int32 Calendar::getDSTOffsetInMillis()
{
	return getCombinedOffsetInMillis(CalendarFieldIndex::DST_OFFSET,
		CalendarFieldIndex::DST_OFFSET_SECOND_MILLIS);
}


void Calendar::setLocalDateTime(double nTimeInDays)
{
	setDateTime(nTimeInDays);
	sal_Int32 nZone1 = getZoneOffsetInMillis();
	sal_Int32 nDST1 = getDSTOffsetInMillis();
	double nLoc = nTimeInDays - (double)(nZone1 + nDST1) / MILLISECONDS_PER_DAY;
	setDateTime(nLoc);
	sal_Int32 nZone2 = getZoneOffsetInMillis();
	sal_Int32 nDST2 = getDSTOffsetInMillis();
	if (nDST1 != nDST2)
	{
		nLoc = nTimeInDays - (double)(nZone2 + nDST2) / MILLISECONDS_PER_DAY;
		setDateTime(nLoc);
		sal_Int32 nDST3 = getDSTOffsetInMillis();
		if (nDST2 != nDST3 && !nDST3)
		{
			nLoc = nTimeInDays - (double)(nZone2 + nDST3) / MILLISECONDS_PER_DAY;
			setDateTime(nLoc);
		}
	}
}

double Calendar::getLocalDateTime()
{
	double nTimeInDays = getDateTime();
	sal_Int32 nZone = getZoneOffsetInMillis();
	sal_Int32 nDST = getDSTOffsetInMillis();
	nTimeInDays += (double)(nZone + nDST) / MILLISECONDS_PER_DAY;
	return nTimeInDays;
}

//---------------------------------------------------------------------------
//      ChangeNullDate

void Calendar::ChangeNullDate(
	const sal_uInt16 Day,
	const sal_uInt16 Month,
	const sal_uInt16 Year)
{
	if (pNullDate)
		*pNullDate = Date(Day, Month, Year);
	else
		pNullDate = new Date(Day, Month, Year);
}
} // namespace duckdb_numformat
