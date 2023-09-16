from xmlrpc.client import DateTime
import duckdb
import os
from pyduckdb.value.constant import (
    Value,
    NullValue,
    BooleanValue,
    UnsignedBinaryValue,
    UnsignedShortValue,
    UnsignedIntegerValue,
    UnsignedLongValue,
    BinaryValue,
    ShortValue,
    IntegerValue,
    LongValue,
    HugeIntegerValue,
    FloatValue,
    DoubleValue,
    DecimalValue,
    StringValue,
    UUIDValue,
    BitValue,
    BlobValue,
    DateValue,
    IntervalValue,
    TimestampValue,
    TimestampSecondValue,
    TimestampMilisecondValue,
    TimestampNanosecondValue,
    TimestampTimeZoneValue,
    TimeValue,
    TimeTimeZoneValue,
)
import uuid
import datetime
import pytest
import decimal

from duckdb.typing import (
    SQLNULL,
    BOOLEAN,
    TINYINT,
    UTINYINT,
    SMALLINT,
    USMALLINT,
    INTEGER,
    UINTEGER,
    BIGINT,
    UBIGINT,
    HUGEINT,
    UUID,
    FLOAT,
    DOUBLE,
    DATE,
    TIMESTAMP,
    TIMESTAMP_MS,
    TIMESTAMP_NS,
    TIMESTAMP_S,
    TIME,
    TIME_TZ,
    TIMESTAMP_TZ,
    VARCHAR,
    BLOB,
    BIT,
    INTERVAL,
)


class TestValue(object):
    # This excludes timezone aware values, as those are a pain to test
    @pytest.mark.parametrize(
        'item',
        [
            (BOOLEAN, BooleanValue(True), True),
            (UTINYINT, UnsignedBinaryValue(129), 129),
            (USMALLINT, UnsignedShortValue(12356), 12356),
            (UINTEGER, UnsignedIntegerValue(2435035), 2435035),
            (UBIGINT, UnsignedLongValue(243503523), 243503523),
            (TINYINT, BinaryValue(-1), -1),
            (SMALLINT, ShortValue(-1), -1),
            (INTEGER, IntegerValue(-1), -1),
            (BIGINT, LongValue(-1), -1),
            (HUGEINT, HugeIntegerValue(-1), -1),
            (FLOAT, FloatValue(1.8349000215530396), 1.8349000215530396),
            (DOUBLE, DoubleValue(0.23234234234), 0.23234234234),
            (
                duckdb.decimal_type(12, 8),
                DecimalValue(decimal.Decimal('1234.12345678'), 12, 8),
                decimal.Decimal('1234.12345678'),
            ),
            (VARCHAR, StringValue('this is a long string'), 'this is a long string'),
            (
                UUID,
                UUIDValue(uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')),
                uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),
            ),
            (BIT, BitValue(b'010101010101'), '010101010101'),
            (BLOB, BlobValue(b'\x00\x00\x00a'), b'\x00\x00\x00a'),
            (DATE, DateValue(datetime.date(2000, 5, 4)), datetime.date(2000, 5, 4)),
            (INTERVAL, IntervalValue(datetime.timedelta(days=5)), datetime.timedelta(days=5)),
            (
                TIMESTAMP,
                TimestampValue(datetime.datetime(1970, 3, 21, 12, 5, 43, 120)),
                datetime.datetime(1970, 3, 21, 12, 5, 43, 120),
            ),
            (SQLNULL, NullValue(), None),
            (TIME, TimeValue(datetime.time(12, 3, 12, 80)), datetime.time(12, 3, 12, 80)),
        ],
    )
    def test_value_helpers(self, item):
        expected_type = item[0]
        value_object = item[1]
        expected_value = item[2]

        con = duckdb.connect()
        observed_type = con.execute('select typeof(a) from (select $1) tbl(a)', [value_object]).fetchall()[0][0]
        assert observed_type == str(expected_type)

        con.execute('select $1', [value_object])
        result = con.fetchone()
        result = result[0]
        assert result == expected_value

    def test_float_to_decimal_prevention(self):
        value = DecimalValue(1.2345, 12, 8)

        con = duckdb.connect()
        with pytest.raises(duckdb.ConversionException, match="Can't losslessly convert"):
            con.execute('select $1', [value]).fetchall()

    @pytest.mark.parametrize(
        'value',
        [
            TimestampSecondValue(datetime.datetime(1970, 3, 21, 12, 36, 43)),
            TimestampMilisecondValue(datetime.datetime(1970, 3, 21, 12, 36, 43)),
            TimestampNanosecondValue(datetime.datetime(1970, 3, 21, 12, 36, 43)),
        ],
    )
    def test_timestamp_sec_not_supported(self, value):
        con = duckdb.connect()
        with pytest.raises(
            duckdb.NotImplementedException, match="Conversion from 'datetime' to type .* is not implemented yet"
        ):
            con.execute('select $1', [value]).fetchall()

    @pytest.mark.parametrize(
        'test',
        [
            (TINYINT, 0, True),
            (TINYINT, 255, False),
            (TINYINT, -128, True),
            (UTINYINT, 80, True),
            (UTINYINT, -1, False),
            (UTINYINT, 255, True),
            (SMALLINT, 0, True),
            (SMALLINT, 128, True),
            (SMALLINT, -255, True),
            (SMALLINT, -32780, False),
            (USMALLINT, 0, True),
            (USMALLINT, -1, False),
            (USMALLINT, 1337, True),
            (USMALLINT, 32780, True),
            (INTEGER, 0, True),
            (INTEGER, 32780, True),
            (INTEGER, -32780, True),
            (INTEGER, -1337, True),
            (UINTEGER, 0, True),
            (UINTEGER, -1337, False),
            (UINTEGER, 65534, True),
            (BIGINT, 0, True),
            (BIGINT, -1234567, True),
            (BIGINT, 9223372036854775808, False),
            (UBIGINT, 9223372036854775808, True),
            (UBIGINT, -1, False),
            (UBIGINT, 18446744073709551615, True),
            (HUGEINT, -9223372036854775808, True),
            (HUGEINT, 9223372036854775807, True),
            (HUGEINT, 0, True),
            (HUGEINT, -1337, True),
            (HUGEINT, 12334214123, True),
        ],
    )
    def test_numeric_values(self, test):
        target_type = test[0]
        test_value = test[1]
        expected_conversion_success = test[2]

        value = Value(test_value, target_type)
        con = duckdb.connect()

        try:
            res = con.execute('select typeof(a) from (select $1) tbl(a)', [value]).fetchall()
            assert expected_conversion_success == True
            assert str(target_type) == res[0][0]
        except:
            assert expected_conversion_success == False
