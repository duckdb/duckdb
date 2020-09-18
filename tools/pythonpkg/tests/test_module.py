#!/usr/bin/env python
# -*- coding: utf-8 -*-

import duckdb

class TestModule(object):

    def test_module_attributes(self, duckdb_cursor):
        assert 'qmark' == duckdb.paramstyle
        assert '1.0' == duckdb.apilevel
        assert 1 == duckdb.threadsafety      
