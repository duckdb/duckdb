# run SQL smith and collect breaking queries
import os
import re
import subprocess
import sys
import sqlite3
from python_helpers import open_utf8

sqlsmith_db = 'sqlsmith.db'
sqlsmith_test_dir = 'test/sqlsmith/queries'

export_queries = False

con = sqlite3.connect(sqlsmith_db)
c = con.cursor()

if len(sys.argv) == 2:
    if sys.argv[1] == '--export':
        export_queries = True
    elif sys.argv[1] == '--reset':
        c.execute('DROP TABLE IF EXISTS sqlsmith_errors')
    else:
        print('Unknown query option ' + sys.argv[1])
        exit(1)

if export_queries:
    c.execute('SELECT query FROM sqlsmith_errors')
    results = c.fetchall()
    for fname in os.listdir(sqlsmith_test_dir):
        os.remove(os.path.join(sqlsmith_test_dir, fname))

    for i in range(len(results)):
        with open(os.path.join(sqlsmith_test_dir, 'sqlsmith-%d.sql' % (i + 1)), 'w+') as f:
            f.write(results[i][0] + "\n")
    exit(0)


def run_sqlsmith():
    subprocess.call(['build/debug/third_party/sqlsmith/sqlsmith', '--duckdb=:memory:'])


c.execute('CREATE TABLE IF NOT EXISTS sqlsmith_errors(query VARCHAR)')

while True:
    # run SQL smith
    run_sqlsmith()
    # get the breaking query
    with open_utf8('sqlsmith.log', 'r') as f:
        text = re.sub('[ \t\n]+', ' ', f.read())

    c.execute('INSERT INTO sqlsmith_errors VALUES (?)', (text,))
    con.commit()
