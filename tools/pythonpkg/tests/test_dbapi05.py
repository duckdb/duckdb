#simple DB API testcase

from datetime import date

class TestSimpleDBAPI(object):
	def test_prepare(self, duckdb_cursor):
		result = duckdb_cursor.execute('SELECT CAST(? AS INTEGER), CAST(? AS INTEGER)', ['42', '84']).fetchall()
		assert result == [(42, 84, )], "Incorrect result returned"

		c = duckdb_cursor
		
		# from python docs
		c.execute('''CREATE TABLE stocks
			 (date date, trans text, symbol text, qty real, price real)''')
		c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

		t = ('RHAT',)
		result = c.execute('SELECT COUNT(*) FROM stocks WHERE symbol=?', t).fetchone()
		assert result == (1,)


		t = ['RHAT']
		result = c.execute('SELECT COUNT(*) FROM stocks WHERE symbol=?', t).fetchone()
		assert result == (1,)

		# Larger example that inserts many records at a time
		purchases = [(date(2006, 3, 28), 'BUY', 'IBM', 1000, 45.00),
					 (date(2006, 4, 5), 'BUY', 'MSFT', 1000, 72.00),
					 (date(2006, 4, 6), 'SELL', 'IBM', 500, 53.00),
					]
		c.executemany('INSERT INTO stocks VALUES (?,?,?,?,?)', purchases)

		result = c.execute('SELECT count(*) FROM stocks').fetchone()
		assert result == (4, )

		result = c.execute('SELECT date FROM stocks ORDER BY date DESC LIMIT 1').fetchone()
		assert result == (date(2006, 4, 6), )
