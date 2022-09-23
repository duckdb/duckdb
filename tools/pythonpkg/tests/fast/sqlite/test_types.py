#-*- coding: iso-8859-1 -*-
# pysqlite2/test/types.py: tests for type conversion and detection
#
# Copyright (C) 2005 Gerhard Häring <gh@ghaering.de>
#
# This file is part of pyduckdb.
#
# This software is provided 'as-is', without any express or implied
# warranty.  In no event will the authors be held liable for any damages
# arising from the use of this software.
#
# Permission is granted to anyone to use this software for any purpose,
# including commercial applications, and to alter it and redistribute it
# freely, subject to the following restrictions:
#
# 1. The origin of this software must not be misrepresented; you must not
#    claim that you wrote the original software. If you use this software
#    in a product, an acknowledgment in the product documentation would be
#    appreciated but is not required.
# 2. Altered source versions must be plainly marked as such, and must not be
#    misrepresented as being the original software.
# 3. This notice may not be removed or altered from any source distribution.
#
# This library is derived from the pysqlite testing library, with small modifications
# to remove tests that are for features that are not supported by DuckDB.

import datetime
import decimal
import unittest
import duckdb
import pytest

class DuckDBTypeTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(":memory:")
        self.cur = self.con.cursor()
        self.cur.execute("create table test(i bigint, s varchar, f double, b BLOB)")

    def tearDown(self):
        self.cur.close()
        self.con.close()

    def test_CheckString(self):
        self.cur.execute("insert into test(s) values (?)", (u"Österreich",))
        self.cur.execute("select s from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], u"Österreich")

    def test_CheckSmallInt(self):
        self.cur.execute("insert into test(i) values (?)", (42,))
        self.cur.execute("select i from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], 42)

    def test_CheckLargeInt(self):
        num = 2**40
        self.cur.execute("insert into test(i) values (?)", (num,))
        self.cur.execute("select i from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], num)

    def test_CheckFloat(self):
        val = 3.14
        self.cur.execute("insert into test(f) values (?)", (val,))
        self.cur.execute("select f from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], val)

    def test_CheckDecimalTooBig(self):
        val = 17.29
        with pytest.raises(duckdb.ConversionException):
            self.cur.execute("insert into test(f) values (?)", (decimal.Decimal(val),))

    def test_CheckDecimal(self):
        val = '17.29'
        val = decimal.Decimal(val)
        self.cur.execute("insert into test(f) values (?)", (val,))
        self.cur.execute("select f from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], self.cur.execute("select 17.29::DOUBLE").fetchone()[0])

    def test_CheckDecimalWithExponent(self):
        val = '1E5'
        val = decimal.Decimal(val)
        self.cur.execute("insert into test(f) values (?)", (val,))
        self.cur.execute("select f from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], self.cur.execute("select 1.00000::DOUBLE").fetchone()[0])

    def test_CheckNaN(self):
        import math
        val = decimal.Decimal('nan')
        self.cur.execute("insert into test(f) values (?)", (val,))
        self.cur.execute("select f from test")
        row = self.cur.fetchone()
        self.assertEqual(math.isnan(row[0]), True)

    def test_CheckInf(self):
        val = decimal.Decimal('inf')
        self.cur.execute("insert into test(f) values (?)", (val,))
        self.cur.execute("select f from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], val)

    def test_CheckBytesBlob(self):
        val = b"Guglhupf"
        self.cur.execute("insert into test(b) values (?)", (val,))
        self.cur.execute("select b from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], val)

    def test_CheckMemoryviewBlob(self):
        sample = b"Guglhupf"
        val = memoryview(sample)
        self.cur.execute("insert into test(b) values (?)", (val,))
        self.cur.execute("select b from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], sample)

    def test_CheckMemoryviewFromhexBlob(self):
        sample = bytes.fromhex('00FF0F2E3D4C5B6A798800FF00')
        val = memoryview(sample)
        self.cur.execute("insert into test(b) values (?)", (val,))
        self.cur.execute("select b from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], sample)

    def test_CheckNoneBlob(self):
        val = None
        self.cur.execute("insert into test(b) values (?)", (val,))
        self.cur.execute("select b from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], val)

    def test_CheckUnicodeExecute(self):
        self.cur.execute(u"select 'Österreich'")
        row = self.cur.fetchone()
        self.assertEqual(row[0], u"Österreich")



class CommonTableExpressionTests(unittest.TestCase):

    def setUp(self):
        self.con = duckdb.connect(":memory:")
        self.cur = self.con.cursor()
        self.cur.execute("create table test(x int)")

    def tearDown(self):
        self.cur.close()
        self.con.close()

    def test_CheckCursorDescriptionCTESimple(self):
        self.cur.execute("with one as (select 1) select * from one")
        self.assertIsNotNone(self.cur.description)
        self.assertEqual(self.cur.description[0][0], "1")

    def test_CheckCursorDescriptionCTESMultipleColumns(self):
        self.cur.execute("insert into test values(1)")
        self.cur.execute("insert into test values(2)")
        self.cur.execute("with testCTE as (select * from test) select * from testCTE")
        self.assertIsNotNone(self.cur.description)
        self.assertEqual(self.cur.description[0][0], "x")

    def test_CheckCursorDescriptionCTE(self):
        self.cur.execute("insert into test values (1)")
        self.cur.execute("with bar as (select * from test) select * from test where x = 1")
        self.assertIsNotNone(self.cur.description)
        self.assertEqual(self.cur.description[0][0], "x")
        self.cur.execute("with bar as (select * from test) select * from test where x = 2")
        self.assertIsNotNone(self.cur.description)
        self.assertEqual(self.cur.description[0][0], "x")

class DateTimeTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(":memory:")
        self.cur = self.con.cursor()
        self.cur.execute("create table test(d date, t time, ts timestamp)")

    def tearDown(self):
        self.cur.close()
        self.con.close()

    def test_CheckDate(self):
        d = datetime.date(2004, 2, 14)
        self.cur.execute("insert into test(d) values (?)", (d,))
        self.cur.execute("select d from test")
        d2 = self.cur.fetchone()[0]
        self.assertEqual(d, d2)

    def test_CheckTime(self):
        t = datetime.time(7, 15, 0)
        self.cur.execute("insert into test(t) values (?)", (t,))
        self.cur.execute("select t from test")
        t2 = self.cur.fetchone()[0]
        self.assertEqual(t, t2)

    def test_CheckTimestamp(self):
        ts = datetime.datetime(2004, 2, 14, 7, 15, 0)
        self.cur.execute("insert into test(ts) values (?)", (ts,))
        self.cur.execute("select ts from test")
        ts2 = self.cur.fetchone()[0]
        self.assertEqual(ts, ts2)

    def test_CheckSqlTimestamp(self):
        now = datetime.datetime.utcnow()
        self.cur.execute("insert into test(ts) values (current_timestamp)")
        self.cur.execute("select ts from test")
        ts = self.cur.fetchone()[0]
        self.assertEqual(type(ts), datetime.datetime)
        self.assertEqual(ts.year, now.year)

    def test_CheckDateTimeSubSeconds(self):
        ts = datetime.datetime(2004, 2, 14, 7, 15, 0, 500000)
        self.cur.execute("insert into test(ts) values (?)", (ts,))
        self.cur.execute("select ts from test")
        ts2 = self.cur.fetchone()[0]
        self.assertEqual(ts, ts2)

    def test_CheckTimeSubSeconds(self):
        t = datetime.time(7, 15, 0, 500000)
        self.cur.execute("insert into test(t) values (?)", (t,))
        self.cur.execute("select t from test")
        t2 = self.cur.fetchone()[0]
        self.assertEqual(t, t2)

    def test_CheckDateTimeSubSecondsFloatingPoint(self):
        ts = datetime.datetime(2004, 2, 14, 7, 15, 0, 510241)
        self.cur.execute("insert into test(ts) values (?)", (ts,))
        self.cur.execute("select ts from test")
        ts2 = self.cur.fetchone()[0]
        self.assertEqual(ts.year, ts2.year)
        self.assertEqual(ts2.microsecond, 510241)


class ListTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(":memory:")
        self.cur = self.con.cursor()
        self.cur.execute(
            "create table test(single INTEGER[], nested INTEGER[][])"
        )

    def tearDown(self):
        self.cur.close()
        self.con.close()

    def test_CheckEmptyList(self):
        val = []
        self.cur.execute("insert into test values (?, ?)", (val, val))
        self.assertEqual(
            self.cur.execute("select * from test").fetchall(),
            [(val, val)],
        )

    def test_CheckSingleList(self):
        val = [1, 2, 3]
        self.cur.execute("insert into test(single) values (?)", (val,))
        self.assertEqual(
            self.cur.execute("select * from test").fetchall(),
            [(val, None)],
        )

    def test_CheckNestedList(self):
        val = [[1], [2], [3, 4]]
        self.cur.execute("insert into test(nested) values (?)", (val,))
        self.assertEqual(
            self.cur.execute("select * from test").fetchall(),
            [(None, val,)],
        )

    def test_CheckNone(self):
        val = None
        self.cur.execute("insert into test values (?, ?)", (val, val))
        self.assertEqual(
            self.cur.execute("select * from test").fetchall(),
            [(val, val)],
        )

    def test_CheckEmbeddedNone(self):
        val = [None]
        self.cur.execute("insert into test values (?, ?)", (val, val))
        self.assertEqual(
            self.cur.execute("select * from test").fetchall(),
            [(val, val)],
        )
