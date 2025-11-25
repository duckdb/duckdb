# fmt: off

import pytest
from conftest import ShellTest

@pytest.mark.parametrize("mode", ['ascii', 'box', 'column', 'csv', 'duckbox', 'html', 'insert', 'json', 'jsonlines', 'latex', 'line', 'list', 'markdown', 'quote', 'table', 'tabs', 'tcl'])
def test_mode_regression(shell, mode):
    test = (
        ShellTest(shell)
        .statement(f".mode {mode}")
        .statement("FROM 'data/parquet-testing/lineitem-top10000.gzip.parquet' LIMIT 10")
    )

    result = test.run()
    lines = [x.strip() for x in get_expected_output(mode).split('\n')]
    for line in lines:
        if len(line) == 0:
            continue
        result.check_stdout(line)


expected_results = {}

expected_results['ascii'] = '''l_orderkey
l_partkey
l_suppkey
l_linenumber
l_quantity
l_extendedprice
l_discount
l_tax
l_returnflag
l_linestatus
l_shipdate
l_commitdate
l_receiptdate
l_shipinstruct
l_shipmode
l_comment
1
155190
7706
1
17
21168.23
0.04
0.02
N
O
1996-03-13
1996-02-12
1996-03-22
DELIVER IN PERSON
TRUCK
egular courts above the
1
67310
7311
2
36
45983.16
0.09
0.06
N
O
1996-04-12
1996-02-28
1996-04-20
TAKE BACK RETURN
MAIL
ly final dependencies: slyly bold 
1
63700
3701
3
8
13309.6
0.1
0.02
N
O
1996-01-29
1996-03-05
1996-01-31
TAKE BACK RETURN
REG AIR
riously. regular, express dep
1
2132
4633
4
28
28955.64
0.09
0.06
N
O
1996-04-21
1996-03-30
1996-05-16
NONE
AIR
lites. fluffily even de
1
24027
1534
5
24
22824.48
0.1
0.04
N
O
1996-03-30
1996-03-14
1996-04-01
NONE
FOB
 pending foxes. slyly re
1
15635
638
6
32
49620.16
0.07
0.02
N
O
1996-01-30
1996-02-07
1996-02-03
DELIVER IN PERSON
MAIL
arefully slyly ex
2
106170
1191
1
38
44694.46
0.0
0.05
N
O
1997-01-28
1997-01-14
1997-02-02
TAKE BACK RETURN
RAIL
ven requests. deposits breach a
3
4297
1798
1
45
54058.05
0.06
0.0
R
F
1994-02-02
1994-01-04
1994-02-23
NONE
AIR
ongside of the furiously brave acco
3
19036
6540
2
49
46796.47
0.1
0.0
R
F
1993-11-09
1993-12-20
1993-11-24
TAKE BACK RETURN
RAIL
 unusual accounts. eve
3
128449
3474
3
27
39890.88
0.06
0.07
A
F
1994-01-16
1993-11-22
1994-01-23
DELIVER IN PERSON
SHIP
nal foxes wake. 
'''

expected_results['box'] = '''┌────────────┬───────────┬───────────┬──────────────┬────────────┬─────────────────┬────────────┬───────┬──────────────┬──────────────┬────────────┬──────────────┬───────────────┬───────────────────┬────────────┬─────────────────────────────────────┐
│ l_orderkey │ l_partkey │ l_suppkey │ l_linenumber │ l_quantity │ l_extendedprice │ l_discount │ l_tax │ l_returnflag │ l_linestatus │ l_shipdate │ l_commitdate │ l_receiptdate │  l_shipinstruct   │ l_shipmode │              l_comment              │
├────────────┼───────────┼───────────┼──────────────┼────────────┼─────────────────┼────────────┼───────┼──────────────┼──────────────┼────────────┼──────────────┼───────────────┼───────────────────┼────────────┼─────────────────────────────────────┤
│ 1          │ 155190    │ 7706      │ 1            │ 17         │ 21168.23        │ 0.04       │ 0.02  │ N            │ O            │ 1996-03-13 │ 1996-02-12   │ 1996-03-22    │ DELIVER IN PERSON │ TRUCK      │ egular courts above the             │
│ 1          │ 67310     │ 7311      │ 2            │ 36         │ 45983.16        │ 0.09       │ 0.06  │ N            │ O            │ 1996-04-12 │ 1996-02-28   │ 1996-04-20    │ TAKE BACK RETURN  │ MAIL       │ ly final dependencies: slyly bold   │
│ 1          │ 63700     │ 3701      │ 3            │ 8          │ 13309.6         │ 0.1        │ 0.02  │ N            │ O            │ 1996-01-29 │ 1996-03-05   │ 1996-01-31    │ TAKE BACK RETURN  │ REG AIR    │ riously. regular, express dep       │
│ 1          │ 2132      │ 4633      │ 4            │ 28         │ 28955.64        │ 0.09       │ 0.06  │ N            │ O            │ 1996-04-21 │ 1996-03-30   │ 1996-05-16    │ NONE              │ AIR        │ lites. fluffily even de             │
│ 1          │ 24027     │ 1534      │ 5            │ 24         │ 22824.48        │ 0.1        │ 0.04  │ N            │ O            │ 1996-03-30 │ 1996-03-14   │ 1996-04-01    │ NONE              │ FOB        │  pending foxes. slyly re            │
│ 1          │ 15635     │ 638       │ 6            │ 32         │ 49620.16        │ 0.07       │ 0.02  │ N            │ O            │ 1996-01-30 │ 1996-02-07   │ 1996-02-03    │ DELIVER IN PERSON │ MAIL       │ arefully slyly ex                   │
│ 2          │ 106170    │ 1191      │ 1            │ 38         │ 44694.46        │ 0.0        │ 0.05  │ N            │ O            │ 1997-01-28 │ 1997-01-14   │ 1997-02-02    │ TAKE BACK RETURN  │ RAIL       │ ven requests. deposits breach a     │
│ 3          │ 4297      │ 1798      │ 1            │ 45         │ 54058.05        │ 0.06       │ 0.0   │ R            │ F            │ 1994-02-02 │ 1994-01-04   │ 1994-02-23    │ NONE              │ AIR        │ ongside of the furiously brave acco │
│ 3          │ 19036     │ 6540      │ 2            │ 49         │ 46796.47        │ 0.1        │ 0.0   │ R            │ F            │ 1993-11-09 │ 1993-12-20   │ 1993-11-24    │ TAKE BACK RETURN  │ RAIL       │  unusual accounts. eve              │
│ 3          │ 128449    │ 3474      │ 3            │ 27         │ 39890.88        │ 0.06       │ 0.07  │ A            │ F            │ 1994-01-16 │ 1993-11-22   │ 1994-01-23    │ DELIVER IN PERSON │ SHIP       │ nal foxes wake.                     │
└────────────┴───────────┴───────────┴──────────────┴────────────┴─────────────────┴────────────┴───────┴──────────────┴──────────────┴────────────┴──────────────┴───────────────┴───────────────────┴────────────┴─────────────────────────────────────┘
'''

expected_results['column'] = '''l_orderkey  l_partkey  l_suppkey  l_linenumber  l_quantity  l_extendedprice  l_discount  l_tax  l_returnflag  l_linestatus  l_shipdate  l_commitdate  l_receiptdate  l_shipinstruct     l_shipmode  l_comment                          
----------  ---------  ---------  ------------  ----------  ---------------  ----------  -----  ------------  ------------  ----------  ------------  -------------  -----------------  ----------  -----------------------------------
1           155190     7706       1             17          21168.23         0.04        0.02   N             O             1996-03-13  1996-02-12    1996-03-22     DELIVER IN PERSON  TRUCK       egular courts above the            
1           67310      7311       2             36          45983.16         0.09        0.06   N             O             1996-04-12  1996-02-28    1996-04-20     TAKE BACK RETURN   MAIL        ly final dependencies: slyly bold  
1           63700      3701       3             8           13309.6          0.1         0.02   N             O             1996-01-29  1996-03-05    1996-01-31     TAKE BACK RETURN   REG AIR     riously. regular, express dep      
1           2132       4633       4             28          28955.64         0.09        0.06   N             O             1996-04-21  1996-03-30    1996-05-16     NONE               AIR         lites. fluffily even de            
1           24027      1534       5             24          22824.48         0.1         0.04   N             O             1996-03-30  1996-03-14    1996-04-01     NONE               FOB          pending foxes. slyly re           
1           15635      638        6             32          49620.16         0.07        0.02   N             O             1996-01-30  1996-02-07    1996-02-03     DELIVER IN PERSON  MAIL        arefully slyly ex                  
2           106170     1191       1             38          44694.46         0.0         0.05   N             O             1997-01-28  1997-01-14    1997-02-02     TAKE BACK RETURN   RAIL        ven requests. deposits breach a    
3           4297       1798       1             45          54058.05         0.06        0.0    R             F             1994-02-02  1994-01-04    1994-02-23     NONE               AIR         ongside of the furiously brave acco
3           19036      6540       2             49          46796.47         0.1         0.0    R             F             1993-11-09  1993-12-20    1993-11-24     TAKE BACK RETURN   RAIL         unusual accounts. eve             
3           128449     3474       3             27          39890.88         0.06        0.07   A             F             1994-01-16  1993-11-22    1994-01-23     DELIVER IN PERSON  SHIP        nal foxes wake.                    
'''

expected_results['csv'] = '''l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment
1,155190,7706,1,17,21168.23,0.04,0.02,N,O,1996-03-13,1996-02-12,1996-03-22,DELIVER IN PERSON,TRUCK,egular courts above the
1,67310,7311,2,36,45983.16,0.09,0.06,N,O,1996-04-12,1996-02-28,1996-04-20,TAKE BACK RETURN,MAIL,ly final dependencies: slyly bold 
1,63700,3701,3,8,13309.6,0.1,0.02,N,O,1996-01-29,1996-03-05,1996-01-31,TAKE BACK RETURN,REG AIR,"riously. regular, express dep"
1,2132,4633,4,28,28955.64,0.09,0.06,N,O,1996-04-21,1996-03-30,1996-05-16,NONE,AIR,lites. fluffily even de
1,24027,1534,5,24,22824.48,0.1,0.04,N,O,1996-03-30,1996-03-14,1996-04-01,NONE,FOB, pending foxes. slyly re
1,15635,638,6,32,49620.16,0.07,0.02,N,O,1996-01-30,1996-02-07,1996-02-03,DELIVER IN PERSON,MAIL,arefully slyly ex
2,106170,1191,1,38,44694.46,0.0,0.05,N,O,1997-01-28,1997-01-14,1997-02-02,TAKE BACK RETURN,RAIL,ven requests. deposits breach a
3,4297,1798,1,45,54058.05,0.06,0.0,R,F,1994-02-02,1994-01-04,1994-02-23,NONE,AIR,ongside of the furiously brave acco
3,19036,6540,2,49,46796.47,0.1,0.0,R,F,1993-11-09,1993-12-20,1993-11-24,TAKE BACK RETURN,RAIL, unusual accounts. eve
3,128449,3474,3,27,39890.88,0.06,0.07,A,F,1994-01-16,1993-11-22,1994-01-23,DELIVER IN PERSON,SHIP,nal foxes wake. 
'''

expected_results['duckbox'] = '''┌────────────┬───────────┬───────────┬──────────────┬────────────┬─────────────────┬────────────┬────────┬──────────────┬──────────────┬────────────┬──────────────┬───────────────┬───────────────────┬────────────┬─────────────────────────────────────┐
│ l_orderkey │ l_partkey │ l_suppkey │ l_linenumber │ l_quantity │ l_extendedprice │ l_discount │ l_tax  │ l_returnflag │ l_linestatus │ l_shipdate │ l_commitdate │ l_receiptdate │  l_shipinstruct   │ l_shipmode │              l_comment              │
│   int64    │   int64   │   int64   │    int32     │   int32    │     double      │   double   │ double │   varchar    │   varchar    │  varchar   │   varchar    │    varchar    │      varchar      │  varchar   │               varchar               │
├────────────┼───────────┼───────────┼──────────────┼────────────┼─────────────────┼────────────┼────────┼──────────────┼──────────────┼────────────┼──────────────┼───────────────┼───────────────────┼────────────┼─────────────────────────────────────┤
│          1 │    155190 │      7706 │            1 │         17 │        21168.23 │       0.04 │   0.02 │ N            │ O            │ 1996-03-13 │ 1996-02-12   │ 1996-03-22    │ DELIVER IN PERSON │ TRUCK      │ egular courts above the             │
│          1 │     67310 │      7311 │            2 │         36 │        45983.16 │       0.09 │   0.06 │ N            │ O            │ 1996-04-12 │ 1996-02-28   │ 1996-04-20    │ TAKE BACK RETURN  │ MAIL       │ ly final dependencies: slyly bold   │
│          1 │     63700 │      3701 │            3 │          8 │         13309.6 │        0.1 │   0.02 │ N            │ O            │ 1996-01-29 │ 1996-03-05   │ 1996-01-31    │ TAKE BACK RETURN  │ REG AIR    │ riously. regular, express dep       │
│          1 │      2132 │      4633 │            4 │         28 │        28955.64 │       0.09 │   0.06 │ N            │ O            │ 1996-04-21 │ 1996-03-30   │ 1996-05-16    │ NONE              │ AIR        │ lites. fluffily even de             │
│          1 │     24027 │      1534 │            5 │         24 │        22824.48 │        0.1 │   0.04 │ N            │ O            │ 1996-03-30 │ 1996-03-14   │ 1996-04-01    │ NONE              │ FOB        │  pending foxes. slyly re            │
│          1 │     15635 │       638 │            6 │         32 │        49620.16 │       0.07 │   0.02 │ N            │ O            │ 1996-01-30 │ 1996-02-07   │ 1996-02-03    │ DELIVER IN PERSON │ MAIL       │ arefully slyly ex                   │
│          2 │    106170 │      1191 │            1 │         38 │        44694.46 │        0.0 │   0.05 │ N            │ O            │ 1997-01-28 │ 1997-01-14   │ 1997-02-02    │ TAKE BACK RETURN  │ RAIL       │ ven requests. deposits breach a     │
│          3 │      4297 │      1798 │            1 │         45 │        54058.05 │       0.06 │    0.0 │ R            │ F            │ 1994-02-02 │ 1994-01-04   │ 1994-02-23    │ NONE              │ AIR        │ ongside of the furiously brave acco │
│          3 │     19036 │      6540 │            2 │         49 │        46796.47 │        0.1 │    0.0 │ R            │ F            │ 1993-11-09 │ 1993-12-20   │ 1993-11-24    │ TAKE BACK RETURN  │ RAIL       │  unusual accounts. eve              │
│          3 │    128449 │      3474 │            3 │         27 │        39890.88 │       0.06 │   0.07 │ A            │ F            │ 1994-01-16 │ 1993-11-22   │ 1994-01-23    │ DELIVER IN PERSON │ SHIP       │ nal foxes wake.                     │
├────────────┴───────────┴───────────┴──────────────┴────────────┴─────────────────┴────────────┴────────┴──────────────┴──────────────┴────────────┴──────────────┴───────────────┴───────────────────┴────────────┴─────────────────────────────────────┤
│ 10 rows                                                                                                                                                                                                                                      16 columns │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
'''

expected_results['html'] = '''<tr><th>l_orderkey</th>
<th>l_partkey</th>
<th>l_suppkey</th>
<th>l_linenumber</th>
<th>l_quantity</th>
<th>l_extendedprice</th>
<th>l_discount</th>
<th>l_tax</th>
<th>l_returnflag</th>
<th>l_linestatus</th>
<th>l_shipdate</th>
<th>l_commitdate</th>
<th>l_receiptdate</th>
<th>l_shipinstruct</th>
<th>l_shipmode</th>
<th>l_comment</th>
</tr>
<tr><td>1</td>
<td>155190</td>
<td>7706</td>
<td>1</td>
<td>17</td>
<td>21168.23</td>
<td>0.04</td>
<td>0.02</td>
<td>N</td>
<td>O</td>
<td>1996-03-13</td>
<td>1996-02-12</td>
<td>1996-03-22</td>
<td>DELIVER IN PERSON</td>
<td>TRUCK</td>
<td>egular courts above the</td>
</tr>
<tr><td>1</td>
<td>67310</td>
<td>7311</td>
<td>2</td>
<td>36</td>
<td>45983.16</td>
<td>0.09</td>
<td>0.06</td>
<td>N</td>
<td>O</td>
<td>1996-04-12</td>
<td>1996-02-28</td>
<td>1996-04-20</td>
<td>TAKE BACK RETURN</td>
<td>MAIL</td>
<td>ly final dependencies: slyly bold </td>
</tr>
<tr><td>1</td>
<td>63700</td>
<td>3701</td>
<td>3</td>
<td>8</td>
<td>13309.6</td>
<td>0.1</td>
<td>0.02</td>
<td>N</td>
<td>O</td>
<td>1996-01-29</td>
<td>1996-03-05</td>
<td>1996-01-31</td>
<td>TAKE BACK RETURN</td>
<td>REG AIR</td>
<td>riously. regular, express dep</td>
</tr>
<tr><td>1</td>
<td>2132</td>
<td>4633</td>
<td>4</td>
<td>28</td>
<td>28955.64</td>
<td>0.09</td>
<td>0.06</td>
<td>N</td>
<td>O</td>
<td>1996-04-21</td>
<td>1996-03-30</td>
<td>1996-05-16</td>
<td>NONE</td>
<td>AIR</td>
<td>lites. fluffily even de</td>
</tr>
<tr><td>1</td>
<td>24027</td>
<td>1534</td>
<td>5</td>
<td>24</td>
<td>22824.48</td>
<td>0.1</td>
<td>0.04</td>
<td>N</td>
<td>O</td>
<td>1996-03-30</td>
<td>1996-03-14</td>
<td>1996-04-01</td>
<td>NONE</td>
<td>FOB</td>
<td> pending foxes. slyly re</td>
</tr>
<tr><td>1</td>
<td>15635</td>
<td>638</td>
<td>6</td>
<td>32</td>
<td>49620.16</td>
<td>0.07</td>
<td>0.02</td>
<td>N</td>
<td>O</td>
<td>1996-01-30</td>
<td>1996-02-07</td>
<td>1996-02-03</td>
<td>DELIVER IN PERSON</td>
<td>MAIL</td>
<td>arefully slyly ex</td>
</tr>
<tr><td>2</td>
<td>106170</td>
<td>1191</td>
<td>1</td>
<td>38</td>
<td>44694.46</td>
<td>0.0</td>
<td>0.05</td>
<td>N</td>
<td>O</td>
<td>1997-01-28</td>
<td>1997-01-14</td>
<td>1997-02-02</td>
<td>TAKE BACK RETURN</td>
<td>RAIL</td>
<td>ven requests. deposits breach a</td>
</tr>
<tr><td>3</td>
<td>4297</td>
<td>1798</td>
<td>1</td>
<td>45</td>
<td>54058.05</td>
<td>0.06</td>
<td>0.0</td>
<td>R</td>
<td>F</td>
<td>1994-02-02</td>
<td>1994-01-04</td>
<td>1994-02-23</td>
<td>NONE</td>
<td>AIR</td>
<td>ongside of the furiously brave acco</td>
</tr>
<tr><td>3</td>
<td>19036</td>
<td>6540</td>
<td>2</td>
<td>49</td>
<td>46796.47</td>
<td>0.1</td>
<td>0.0</td>
<td>R</td>
<td>F</td>
<td>1993-11-09</td>
<td>1993-12-20</td>
<td>1993-11-24</td>
<td>TAKE BACK RETURN</td>
<td>RAIL</td>
<td> unusual accounts. eve</td>
</tr>
<tr><td>3</td>
<td>128449</td>
<td>3474</td>
<td>3</td>
<td>27</td>
<td>39890.88</td>
<td>0.06</td>
<td>0.07</td>
<td>A</td>
<td>F</td>
<td>1994-01-16</td>
<td>1993-11-22</td>
<td>1994-01-23</td>
<td>DELIVER IN PERSON</td>
<td>SHIP</td>
<td>nal foxes wake. </td>
</tr>
'''

expected_results['insert'] = '''INSERT INTO "table"(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES(1,155190,7706,1,17,21168.23,0.04,0.02,'N','O','1996-03-13','1996-02-12','1996-03-22','DELIVER IN PERSON','TRUCK','egular courts above the');
INSERT INTO "table"(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES(1,67310,7311,2,36,45983.16,0.09,0.06,'N','O','1996-04-12','1996-02-28','1996-04-20','TAKE BACK RETURN','MAIL','ly final dependencies: slyly bold ');
INSERT INTO "table"(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES(1,63700,3701,3,8,13309.6,0.1,0.02,'N','O','1996-01-29','1996-03-05','1996-01-31','TAKE BACK RETURN','REG AIR','riously. regular, express dep');
INSERT INTO "table"(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES(1,2132,4633,4,28,28955.64,0.09,0.06,'N','O','1996-04-21','1996-03-30','1996-05-16','NONE','AIR','lites. fluffily even de');
INSERT INTO "table"(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES(1,24027,1534,5,24,22824.48,0.1,0.04,'N','O','1996-03-30','1996-03-14','1996-04-01','NONE','FOB',' pending foxes. slyly re');
INSERT INTO "table"(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES(1,15635,638,6,32,49620.16,0.07,0.02,'N','O','1996-01-30','1996-02-07','1996-02-03','DELIVER IN PERSON','MAIL','arefully slyly ex');
INSERT INTO "table"(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES(2,106170,1191,1,38,44694.46,0.0,0.05,'N','O','1997-01-28','1997-01-14','1997-02-02','TAKE BACK RETURN','RAIL','ven requests. deposits breach a');
INSERT INTO "table"(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES(3,4297,1798,1,45,54058.05,0.06,0.0,'R','F','1994-02-02','1994-01-04','1994-02-23','NONE','AIR','ongside of the furiously brave acco');
INSERT INTO "table"(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES(3,19036,6540,2,49,46796.47,0.1,0.0,'R','F','1993-11-09','1993-12-20','1993-11-24','TAKE BACK RETURN','RAIL',' unusual accounts. eve');
INSERT INTO "table"(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES(3,128449,3474,3,27,39890.88,0.06,0.07,'A','F','1994-01-16','1993-11-22','1994-01-23','DELIVER IN PERSON','SHIP','nal foxes wake. ');
'''

expected_results['json'] = '''[{"l_orderkey":1,"l_partkey":155190,"l_suppkey":7706,"l_linenumber":1,"l_quantity":17,"l_extendedprice":21168.23,"l_discount":0.04,"l_tax":0.02,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-03-13","l_commitdate":"1996-02-12","l_receiptdate":"1996-03-22","l_shipinstruct":"DELIVER IN PERSON","l_shipmode":"TRUCK","l_comment":"egular courts above the"},
{"l_orderkey":1,"l_partkey":67310,"l_suppkey":7311,"l_linenumber":2,"l_quantity":36,"l_extendedprice":45983.16,"l_discount":0.09,"l_tax":0.06,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-04-12","l_commitdate":"1996-02-28","l_receiptdate":"1996-04-20","l_shipinstruct":"TAKE BACK RETURN","l_shipmode":"MAIL","l_comment":"ly final dependencies: slyly bold "},
{"l_orderkey":1,"l_partkey":63700,"l_suppkey":3701,"l_linenumber":3,"l_quantity":8,"l_extendedprice":13309.6,"l_discount":0.1,"l_tax":0.02,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-01-29","l_commitdate":"1996-03-05","l_receiptdate":"1996-01-31","l_shipinstruct":"TAKE BACK RETURN","l_shipmode":"REG AIR","l_comment":"riously. regular, express dep"},
{"l_orderkey":1,"l_partkey":2132,"l_suppkey":4633,"l_linenumber":4,"l_quantity":28,"l_extendedprice":28955.64,"l_discount":0.09,"l_tax":0.06,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-04-21","l_commitdate":"1996-03-30","l_receiptdate":"1996-05-16","l_shipinstruct":"NONE","l_shipmode":"AIR","l_comment":"lites. fluffily even de"},
{"l_orderkey":1,"l_partkey":24027,"l_suppkey":1534,"l_linenumber":5,"l_quantity":24,"l_extendedprice":22824.48,"l_discount":0.1,"l_tax":0.04,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-03-30","l_commitdate":"1996-03-14","l_receiptdate":"1996-04-01","l_shipinstruct":"NONE","l_shipmode":"FOB","l_comment":" pending foxes. slyly re"},
{"l_orderkey":1,"l_partkey":15635,"l_suppkey":638,"l_linenumber":6,"l_quantity":32,"l_extendedprice":49620.16,"l_discount":0.07,"l_tax":0.02,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-01-30","l_commitdate":"1996-02-07","l_receiptdate":"1996-02-03","l_shipinstruct":"DELIVER IN PERSON","l_shipmode":"MAIL","l_comment":"arefully slyly ex"},
{"l_orderkey":2,"l_partkey":106170,"l_suppkey":1191,"l_linenumber":1,"l_quantity":38,"l_extendedprice":44694.46,"l_discount":0.0,"l_tax":0.05,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1997-01-28","l_commitdate":"1997-01-14","l_receiptdate":"1997-02-02","l_shipinstruct":"TAKE BACK RETURN","l_shipmode":"RAIL","l_comment":"ven requests. deposits breach a"},
{"l_orderkey":3,"l_partkey":4297,"l_suppkey":1798,"l_linenumber":1,"l_quantity":45,"l_extendedprice":54058.05,"l_discount":0.06,"l_tax":0.0,"l_returnflag":"R","l_linestatus":"F","l_shipdate":"1994-02-02","l_commitdate":"1994-01-04","l_receiptdate":"1994-02-23","l_shipinstruct":"NONE","l_shipmode":"AIR","l_comment":"ongside of the furiously brave acco"},
{"l_orderkey":3,"l_partkey":19036,"l_suppkey":6540,"l_linenumber":2,"l_quantity":49,"l_extendedprice":46796.47,"l_discount":0.1,"l_tax":0.0,"l_returnflag":"R","l_linestatus":"F","l_shipdate":"1993-11-09","l_commitdate":"1993-12-20","l_receiptdate":"1993-11-24","l_shipinstruct":"TAKE BACK RETURN","l_shipmode":"RAIL","l_comment":" unusual accounts. eve"},
{"l_orderkey":3,"l_partkey":128449,"l_suppkey":3474,"l_linenumber":3,"l_quantity":27,"l_extendedprice":39890.88,"l_discount":0.06,"l_tax":0.07,"l_returnflag":"A","l_linestatus":"F","l_shipdate":"1994-01-16","l_commitdate":"1993-11-22","l_receiptdate":"1994-01-23","l_shipinstruct":"DELIVER IN PERSON","l_shipmode":"SHIP","l_comment":"nal foxes wake. "}]
'''

expected_results['jsonlines'] = '''{"l_orderkey":1,"l_partkey":155190,"l_suppkey":7706,"l_linenumber":1,"l_quantity":17,"l_extendedprice":21168.23,"l_discount":0.04,"l_tax":0.02,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-03-13","l_commitdate":"1996-02-12","l_receiptdate":"1996-03-22","l_shipinstruct":"DELIVER IN PERSON","l_shipmode":"TRUCK","l_comment":"egular courts above the"}
{"l_orderkey":1,"l_partkey":67310,"l_suppkey":7311,"l_linenumber":2,"l_quantity":36,"l_extendedprice":45983.16,"l_discount":0.09,"l_tax":0.06,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-04-12","l_commitdate":"1996-02-28","l_receiptdate":"1996-04-20","l_shipinstruct":"TAKE BACK RETURN","l_shipmode":"MAIL","l_comment":"ly final dependencies: slyly bold "}
{"l_orderkey":1,"l_partkey":63700,"l_suppkey":3701,"l_linenumber":3,"l_quantity":8,"l_extendedprice":13309.6,"l_discount":0.1,"l_tax":0.02,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-01-29","l_commitdate":"1996-03-05","l_receiptdate":"1996-01-31","l_shipinstruct":"TAKE BACK RETURN","l_shipmode":"REG AIR","l_comment":"riously. regular, express dep"}
{"l_orderkey":1,"l_partkey":2132,"l_suppkey":4633,"l_linenumber":4,"l_quantity":28,"l_extendedprice":28955.64,"l_discount":0.09,"l_tax":0.06,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-04-21","l_commitdate":"1996-03-30","l_receiptdate":"1996-05-16","l_shipinstruct":"NONE","l_shipmode":"AIR","l_comment":"lites. fluffily even de"}
{"l_orderkey":1,"l_partkey":24027,"l_suppkey":1534,"l_linenumber":5,"l_quantity":24,"l_extendedprice":22824.48,"l_discount":0.1,"l_tax":0.04,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-03-30","l_commitdate":"1996-03-14","l_receiptdate":"1996-04-01","l_shipinstruct":"NONE","l_shipmode":"FOB","l_comment":" pending foxes. slyly re"}
{"l_orderkey":1,"l_partkey":15635,"l_suppkey":638,"l_linenumber":6,"l_quantity":32,"l_extendedprice":49620.16,"l_discount":0.07,"l_tax":0.02,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1996-01-30","l_commitdate":"1996-02-07","l_receiptdate":"1996-02-03","l_shipinstruct":"DELIVER IN PERSON","l_shipmode":"MAIL","l_comment":"arefully slyly ex"}
{"l_orderkey":2,"l_partkey":106170,"l_suppkey":1191,"l_linenumber":1,"l_quantity":38,"l_extendedprice":44694.46,"l_discount":0.0,"l_tax":0.05,"l_returnflag":"N","l_linestatus":"O","l_shipdate":"1997-01-28","l_commitdate":"1997-01-14","l_receiptdate":"1997-02-02","l_shipinstruct":"TAKE BACK RETURN","l_shipmode":"RAIL","l_comment":"ven requests. deposits breach a"}
{"l_orderkey":3,"l_partkey":4297,"l_suppkey":1798,"l_linenumber":1,"l_quantity":45,"l_extendedprice":54058.05,"l_discount":0.06,"l_tax":0.0,"l_returnflag":"R","l_linestatus":"F","l_shipdate":"1994-02-02","l_commitdate":"1994-01-04","l_receiptdate":"1994-02-23","l_shipinstruct":"NONE","l_shipmode":"AIR","l_comment":"ongside of the furiously brave acco"}
{"l_orderkey":3,"l_partkey":19036,"l_suppkey":6540,"l_linenumber":2,"l_quantity":49,"l_extendedprice":46796.47,"l_discount":0.1,"l_tax":0.0,"l_returnflag":"R","l_linestatus":"F","l_shipdate":"1993-11-09","l_commitdate":"1993-12-20","l_receiptdate":"1993-11-24","l_shipinstruct":"TAKE BACK RETURN","l_shipmode":"RAIL","l_comment":" unusual accounts. eve"}
{"l_orderkey":3,"l_partkey":128449,"l_suppkey":3474,"l_linenumber":3,"l_quantity":27,"l_extendedprice":39890.88,"l_discount":0.06,"l_tax":0.07,"l_returnflag":"A","l_linestatus":"F","l_shipdate":"1994-01-16","l_commitdate":"1993-11-22","l_receiptdate":"1994-01-23","l_shipinstruct":"DELIVER IN PERSON","l_shipmode":"SHIP","l_comment":"nal foxes wake. "}
'''

expected_results['latex'] = '''\\begin{tabular}{|rrrrrrrrllllllll|}
\\hline
l_orderkey & l_partkey & l_suppkey & l_linenumber & l_quantity & l_extendedprice & l_discount & l_tax & l_returnflag & l_linestatus & l_shipdate & l_commitdate & l_receiptdate &  l_shipinstruct   & l_shipmode &              l_comment              \\\\
\\hline
1          & 155190    & 7706      & 1            & 17         & 21168.23        & 0.04       & 0.02  & N            & O            & 1996-03-13 & 1996-02-12   & 1996-03-22    & DELIVER IN PERSON & TRUCK      & egular courts above the             \\\\
1          & 67310     & 7311      & 2            & 36         & 45983.16        & 0.09       & 0.06  & N            & O            & 1996-04-12 & 1996-02-28   & 1996-04-20    & TAKE BACK RETURN  & MAIL       & ly final dependencies: slyly bold   \\\\
1          & 63700     & 3701      & 3            & 8          & 13309.6         & 0.1        & 0.02  & N            & O            & 1996-01-29 & 1996-03-05   & 1996-01-31    & TAKE BACK RETURN  & REG AIR    & riously. regular, express dep       \\\\
1          & 2132      & 4633      & 4            & 28         & 28955.64        & 0.09       & 0.06  & N            & O            & 1996-04-21 & 1996-03-30   & 1996-05-16    & NONE              & AIR        & lites. fluffily even de             \\\\
1          & 24027     & 1534      & 5            & 24         & 22824.48        & 0.1        & 0.04  & N            & O            & 1996-03-30 & 1996-03-14   & 1996-04-01    & NONE              & FOB        &  pending foxes. slyly re            \\\\
1          & 15635     & 638       & 6            & 32         & 49620.16        & 0.07       & 0.02  & N            & O            & 1996-01-30 & 1996-02-07   & 1996-02-03    & DELIVER IN PERSON & MAIL       & arefully slyly ex                   \\\\
2          & 106170    & 1191      & 1            & 38         & 44694.46        & 0.0        & 0.05  & N            & O            & 1997-01-28 & 1997-01-14   & 1997-02-02    & TAKE BACK RETURN  & RAIL       & ven requests. deposits breach a     \\\\
3          & 4297      & 1798      & 1            & 45         & 54058.05        & 0.06       & 0.0   & R            & F            & 1994-02-02 & 1994-01-04   & 1994-02-23    & NONE              & AIR        & ongside of the furiously brave acco \\\\
3          & 19036     & 6540      & 2            & 49         & 46796.47        & 0.1        & 0.0   & R            & F            & 1993-11-09 & 1993-12-20   & 1993-11-24    & TAKE BACK RETURN  & RAIL       &  unusual accounts. eve              \\\\
3          & 128449    & 3474      & 3            & 27         & 39890.88        & 0.06       & 0.07  & A            & F            & 1994-01-16 & 1993-11-22   & 1994-01-23    & DELIVER IN PERSON & SHIP       & nal foxes wake.                     \\\\
\\hline
\\end{tabular}
'''

expected_results['line'] = '''     l_orderkey = 1
      l_partkey = 155190
      l_suppkey = 7706
   l_linenumber = 1
     l_quantity = 17
l_extendedprice = 21168.23
     l_discount = 0.04
          l_tax = 0.02
   l_returnflag = N
   l_linestatus = O
     l_shipdate = 1996-03-13
   l_commitdate = 1996-02-12
  l_receiptdate = 1996-03-22
 l_shipinstruct = DELIVER IN PERSON
     l_shipmode = TRUCK
      l_comment = egular courts above the

     l_orderkey = 1
      l_partkey = 67310
      l_suppkey = 7311
   l_linenumber = 2
     l_quantity = 36
l_extendedprice = 45983.16
     l_discount = 0.09
          l_tax = 0.06
   l_returnflag = N
   l_linestatus = O
     l_shipdate = 1996-04-12
   l_commitdate = 1996-02-28
  l_receiptdate = 1996-04-20
 l_shipinstruct = TAKE BACK RETURN
     l_shipmode = MAIL
      l_comment = ly final dependencies: slyly bold 

     l_orderkey = 1
      l_partkey = 63700
      l_suppkey = 3701
   l_linenumber = 3
     l_quantity = 8
l_extendedprice = 13309.6
     l_discount = 0.1
          l_tax = 0.02
   l_returnflag = N
   l_linestatus = O
     l_shipdate = 1996-01-29
   l_commitdate = 1996-03-05
  l_receiptdate = 1996-01-31
 l_shipinstruct = TAKE BACK RETURN
     l_shipmode = REG AIR
      l_comment = riously. regular, express dep

     l_orderkey = 1
      l_partkey = 2132
      l_suppkey = 4633
   l_linenumber = 4
     l_quantity = 28
l_extendedprice = 28955.64
     l_discount = 0.09
          l_tax = 0.06
   l_returnflag = N
   l_linestatus = O
     l_shipdate = 1996-04-21
   l_commitdate = 1996-03-30
  l_receiptdate = 1996-05-16
 l_shipinstruct = NONE
     l_shipmode = AIR
      l_comment = lites. fluffily even de

     l_orderkey = 1
      l_partkey = 24027
      l_suppkey = 1534
   l_linenumber = 5
     l_quantity = 24
l_extendedprice = 22824.48
     l_discount = 0.1
          l_tax = 0.04
   l_returnflag = N
   l_linestatus = O
     l_shipdate = 1996-03-30
   l_commitdate = 1996-03-14
  l_receiptdate = 1996-04-01
 l_shipinstruct = NONE
     l_shipmode = FOB
      l_comment =  pending foxes. slyly re

     l_orderkey = 1
      l_partkey = 15635
      l_suppkey = 638
   l_linenumber = 6
     l_quantity = 32
l_extendedprice = 49620.16
     l_discount = 0.07
          l_tax = 0.02
   l_returnflag = N
   l_linestatus = O
     l_shipdate = 1996-01-30
   l_commitdate = 1996-02-07
  l_receiptdate = 1996-02-03
 l_shipinstruct = DELIVER IN PERSON
     l_shipmode = MAIL
      l_comment = arefully slyly ex

     l_orderkey = 2
      l_partkey = 106170
      l_suppkey = 1191
   l_linenumber = 1
     l_quantity = 38
l_extendedprice = 44694.46
     l_discount = 0.0
          l_tax = 0.05
   l_returnflag = N
   l_linestatus = O
     l_shipdate = 1997-01-28
   l_commitdate = 1997-01-14
  l_receiptdate = 1997-02-02
 l_shipinstruct = TAKE BACK RETURN
     l_shipmode = RAIL
      l_comment = ven requests. deposits breach a

     l_orderkey = 3
      l_partkey = 4297
      l_suppkey = 1798
   l_linenumber = 1
     l_quantity = 45
l_extendedprice = 54058.05
     l_discount = 0.06
          l_tax = 0.0
   l_returnflag = R
   l_linestatus = F
     l_shipdate = 1994-02-02
   l_commitdate = 1994-01-04
  l_receiptdate = 1994-02-23
 l_shipinstruct = NONE
     l_shipmode = AIR
      l_comment = ongside of the furiously brave acco

     l_orderkey = 3
      l_partkey = 19036
      l_suppkey = 6540
   l_linenumber = 2
     l_quantity = 49
l_extendedprice = 46796.47
     l_discount = 0.1
          l_tax = 0.0
   l_returnflag = R
   l_linestatus = F
     l_shipdate = 1993-11-09
   l_commitdate = 1993-12-20
  l_receiptdate = 1993-11-24
 l_shipinstruct = TAKE BACK RETURN
     l_shipmode = RAIL
      l_comment =  unusual accounts. eve

     l_orderkey = 3
      l_partkey = 128449
      l_suppkey = 3474
   l_linenumber = 3
     l_quantity = 27
l_extendedprice = 39890.88
     l_discount = 0.06
          l_tax = 0.07
   l_returnflag = A
   l_linestatus = F
     l_shipdate = 1994-01-16
   l_commitdate = 1993-11-22
  l_receiptdate = 1994-01-23
 l_shipinstruct = DELIVER IN PERSON
     l_shipmode = SHIP
      l_comment = nal foxes wake. 
'''

expected_results['list'] = '''l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode|l_comment
1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the
1|67310|7311|2|36|45983.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold 
1|63700|3701|3|8|13309.6|0.1|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|riously. regular, express dep
1|2132|4633|4|28|28955.64|0.09|0.06|N|O|1996-04-21|1996-03-30|1996-05-16|NONE|AIR|lites. fluffily even de
1|24027|1534|5|24|22824.48|0.1|0.04|N|O|1996-03-30|1996-03-14|1996-04-01|NONE|FOB| pending foxes. slyly re
1|15635|638|6|32|49620.16|0.07|0.02|N|O|1996-01-30|1996-02-07|1996-02-03|DELIVER IN PERSON|MAIL|arefully slyly ex
2|106170|1191|1|38|44694.46|0.0|0.05|N|O|1997-01-28|1997-01-14|1997-02-02|TAKE BACK RETURN|RAIL|ven requests. deposits breach a
3|4297|1798|1|45|54058.05|0.06|0.0|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|ongside of the furiously brave acco
3|19036|6540|2|49|46796.47|0.1|0.0|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL| unusual accounts. eve
3|128449|3474|3|27|39890.88|0.06|0.07|A|F|1994-01-16|1993-11-22|1994-01-23|DELIVER IN PERSON|SHIP|nal foxes wake. 
'''

expected_results['markdown'] = '''| l_orderkey | l_partkey | l_suppkey | l_linenumber | l_quantity | l_extendedprice | l_discount | l_tax | l_returnflag | l_linestatus | l_shipdate | l_commitdate | l_receiptdate |  l_shipinstruct   | l_shipmode |              l_comment              |
|-----------:|----------:|----------:|-------------:|-----------:|----------------:|-----------:|------:|--------------|--------------|------------|--------------|---------------|-------------------|------------|-------------------------------------|
| 1          | 155190    | 7706      | 1            | 17         | 21168.23        | 0.04       | 0.02  | N            | O            | 1996-03-13 | 1996-02-12   | 1996-03-22    | DELIVER IN PERSON | TRUCK      | egular courts above the             |
| 1          | 67310     | 7311      | 2            | 36         | 45983.16        | 0.09       | 0.06  | N            | O            | 1996-04-12 | 1996-02-28   | 1996-04-20    | TAKE BACK RETURN  | MAIL       | ly final dependencies: slyly bold   |
| 1          | 63700     | 3701      | 3            | 8          | 13309.6         | 0.1        | 0.02  | N            | O            | 1996-01-29 | 1996-03-05   | 1996-01-31    | TAKE BACK RETURN  | REG AIR    | riously. regular, express dep       |
| 1          | 2132      | 4633      | 4            | 28         | 28955.64        | 0.09       | 0.06  | N            | O            | 1996-04-21 | 1996-03-30   | 1996-05-16    | NONE              | AIR        | lites. fluffily even de             |
| 1          | 24027     | 1534      | 5            | 24         | 22824.48        | 0.1        | 0.04  | N            | O            | 1996-03-30 | 1996-03-14   | 1996-04-01    | NONE              | FOB        |  pending foxes. slyly re            |
| 1          | 15635     | 638       | 6            | 32         | 49620.16        | 0.07       | 0.02  | N            | O            | 1996-01-30 | 1996-02-07   | 1996-02-03    | DELIVER IN PERSON | MAIL       | arefully slyly ex                   |
| 2          | 106170    | 1191      | 1            | 38         | 44694.46        | 0.0        | 0.05  | N            | O            | 1997-01-28 | 1997-01-14   | 1997-02-02    | TAKE BACK RETURN  | RAIL       | ven requests. deposits breach a     |
| 3          | 4297      | 1798      | 1            | 45         | 54058.05        | 0.06       | 0.0   | R            | F            | 1994-02-02 | 1994-01-04   | 1994-02-23    | NONE              | AIR        | ongside of the furiously brave acco |
| 3          | 19036     | 6540      | 2            | 49         | 46796.47        | 0.1        | 0.0   | R            | F            | 1993-11-09 | 1993-12-20   | 1993-11-24    | TAKE BACK RETURN  | RAIL       |  unusual accounts. eve              |
| 3          | 128449    | 3474      | 3            | 27         | 39890.88        | 0.06       | 0.07  | A            | F            | 1994-01-16 | 1993-11-22   | 1994-01-23    | DELIVER IN PERSON | SHIP       | nal foxes wake.                     |
'''

expected_results['quote'] = ''''l_orderkey','l_partkey','l_suppkey','l_linenumber','l_quantity','l_extendedprice','l_discount','l_tax','l_returnflag','l_linestatus','l_shipdate','l_commitdate','l_receiptdate','l_shipinstruct','l_shipmode','l_comment'
1,155190,7706,1,17,21168.23,0.04,0.02,'N','O','1996-03-13','1996-02-12','1996-03-22','DELIVER IN PERSON','TRUCK','egular courts above the'
1,67310,7311,2,36,45983.16,0.09,0.06,'N','O','1996-04-12','1996-02-28','1996-04-20','TAKE BACK RETURN','MAIL','ly final dependencies: slyly bold '
1,63700,3701,3,8,13309.6,0.1,0.02,'N','O','1996-01-29','1996-03-05','1996-01-31','TAKE BACK RETURN','REG AIR','riously. regular, express dep'
1,2132,4633,4,28,28955.64,0.09,0.06,'N','O','1996-04-21','1996-03-30','1996-05-16','NONE','AIR','lites. fluffily even de'
1,24027,1534,5,24,22824.48,0.1,0.04,'N','O','1996-03-30','1996-03-14','1996-04-01','NONE','FOB',' pending foxes. slyly re'
1,15635,638,6,32,49620.16,0.07,0.02,'N','O','1996-01-30','1996-02-07','1996-02-03','DELIVER IN PERSON','MAIL','arefully slyly ex'
2,106170,1191,1,38,44694.46,0.0,0.05,'N','O','1997-01-28','1997-01-14','1997-02-02','TAKE BACK RETURN','RAIL','ven requests. deposits breach a'
3,4297,1798,1,45,54058.05,0.06,0.0,'R','F','1994-02-02','1994-01-04','1994-02-23','NONE','AIR','ongside of the furiously brave acco'
3,19036,6540,2,49,46796.47,0.1,0.0,'R','F','1993-11-09','1993-12-20','1993-11-24','TAKE BACK RETURN','RAIL',' unusual accounts. eve'
3,128449,3474,3,27,39890.88,0.06,0.07,'A','F','1994-01-16','1993-11-22','1994-01-23','DELIVER IN PERSON','SHIP','nal foxes wake. '
'''

expected_results['table'] = '''+------------+-----------+-----------+--------------+------------+-----------------+------------+-------+--------------+--------------+------------+--------------+---------------+-------------------+------------+-------------------------------------+
| l_orderkey | l_partkey | l_suppkey | l_linenumber | l_quantity | l_extendedprice | l_discount | l_tax | l_returnflag | l_linestatus | l_shipdate | l_commitdate | l_receiptdate |  l_shipinstruct   | l_shipmode |              l_comment              |
+------------+-----------+-----------+--------------+------------+-----------------+------------+-------+--------------+--------------+------------+--------------+---------------+-------------------+------------+-------------------------------------+
| 1          | 155190    | 7706      | 1            | 17         | 21168.23        | 0.04       | 0.02  | N            | O            | 1996-03-13 | 1996-02-12   | 1996-03-22    | DELIVER IN PERSON | TRUCK      | egular courts above the             |
| 1          | 67310     | 7311      | 2            | 36         | 45983.16        | 0.09       | 0.06  | N            | O            | 1996-04-12 | 1996-02-28   | 1996-04-20    | TAKE BACK RETURN  | MAIL       | ly final dependencies: slyly bold   |
| 1          | 63700     | 3701      | 3            | 8          | 13309.6         | 0.1        | 0.02  | N            | O            | 1996-01-29 | 1996-03-05   | 1996-01-31    | TAKE BACK RETURN  | REG AIR    | riously. regular, express dep       |
| 1          | 2132      | 4633      | 4            | 28         | 28955.64        | 0.09       | 0.06  | N            | O            | 1996-04-21 | 1996-03-30   | 1996-05-16    | NONE              | AIR        | lites. fluffily even de             |
| 1          | 24027     | 1534      | 5            | 24         | 22824.48        | 0.1        | 0.04  | N            | O            | 1996-03-30 | 1996-03-14   | 1996-04-01    | NONE              | FOB        |  pending foxes. slyly re            |
| 1          | 15635     | 638       | 6            | 32         | 49620.16        | 0.07       | 0.02  | N            | O            | 1996-01-30 | 1996-02-07   | 1996-02-03    | DELIVER IN PERSON | MAIL       | arefully slyly ex                   |
| 2          | 106170    | 1191      | 1            | 38         | 44694.46        | 0.0        | 0.05  | N            | O            | 1997-01-28 | 1997-01-14   | 1997-02-02    | TAKE BACK RETURN  | RAIL       | ven requests. deposits breach a     |
| 3          | 4297      | 1798      | 1            | 45         | 54058.05        | 0.06       | 0.0   | R            | F            | 1994-02-02 | 1994-01-04   | 1994-02-23    | NONE              | AIR        | ongside of the furiously brave acco |
| 3          | 19036     | 6540      | 2            | 49         | 46796.47        | 0.1        | 0.0   | R            | F            | 1993-11-09 | 1993-12-20   | 1993-11-24    | TAKE BACK RETURN  | RAIL       |  unusual accounts. eve              |
| 3          | 128449    | 3474      | 3            | 27         | 39890.88        | 0.06       | 0.07  | A            | F            | 1994-01-16 | 1993-11-22   | 1994-01-23    | DELIVER IN PERSON | SHIP       | nal foxes wake.                     |
+------------+-----------+-----------+--------------+------------+-----------------+------------+-------+--------------+--------------+------------+--------------+---------------+-------------------+------------+-------------------------------------+
'''

expected_results['tabs'] = '''l_orderkey	l_partkey	l_suppkey	l_linenumber	l_quantity	l_extendedprice	l_discount	l_tax	l_returnflag	l_linestatus	l_shipdate	l_commitdate	l_receiptdate	l_shipinstruct	l_shipmode	l_comment
1	155190	7706	1	17	21168.23	0.04	0.02	N	O	1996-03-13	1996-02-12	1996-03-22	DELIVER IN PERSON	TRUCK	egular courts above the
1	67310	7311	2	36	45983.16	0.09	0.06	N	O	1996-04-12	1996-02-28	1996-04-20	TAKE BACK RETURN	MAIL	ly final dependencies: slyly bold 
1	63700	3701	3	8	13309.6	0.1	0.02	N	O	1996-01-29	1996-03-05	1996-01-31	TAKE BACK RETURN	REG AIR	riously. regular, express dep
1	2132	4633	4	28	28955.64	0.09	0.06	N	O	1996-04-21	1996-03-30	1996-05-16	NONE	AIR	lites. fluffily even de
1	24027	1534	5	24	22824.48	0.1	0.04	N	O	1996-03-30	1996-03-14	1996-04-01	NONE	FOB	 pending foxes. slyly re
1	15635	638	6	32	49620.16	0.07	0.02	N	O	1996-01-30	1996-02-07	1996-02-03	DELIVER IN PERSON	MAIL	arefully slyly ex
2	106170	1191	1	38	44694.46	0.0	0.05	N	O	1997-01-28	1997-01-14	1997-02-02	TAKE BACK RETURN	RAIL	ven requests. deposits breach a
3	4297	1798	1	45	54058.05	0.06	0.0	R	F	1994-02-02	1994-01-04	1994-02-23	NONE	AIR	ongside of the furiously brave acco
3	19036	6540	2	49	46796.47	0.1	0.0	R	F	1993-11-09	1993-12-20	1993-11-24	TAKE BACK RETURN	RAIL	 unusual accounts. eve
3	128449	3474	3	27	39890.88	0.06	0.07	A	F	1994-01-16	1993-11-22	1994-01-23	DELIVER IN PERSON	SHIP	nal foxes wake. 
'''

expected_results['tcl'] = '''"l_orderkey" "l_partkey" "l_suppkey" "l_linenumber" "l_quantity" "l_extendedprice" "l_discount" "l_tax" "l_returnflag" "l_linestatus" "l_shipdate" "l_commitdate" "l_receiptdate" "l_shipinstruct" "l_shipmode" "l_comment"
"1" "155190" "7706" "1" "17" "21168.23" "0.04" "0.02" "N" "O" "1996-03-13" "1996-02-12" "1996-03-22" "DELIVER IN PERSON" "TRUCK" "egular courts above the"
"1" "67310" "7311" "2" "36" "45983.16" "0.09" "0.06" "N" "O" "1996-04-12" "1996-02-28" "1996-04-20" "TAKE BACK RETURN" "MAIL" "ly final dependencies: slyly bold "
"1" "63700" "3701" "3" "8" "13309.6" "0.1" "0.02" "N" "O" "1996-01-29" "1996-03-05" "1996-01-31" "TAKE BACK RETURN" "REG AIR" "riously. regular, express dep"
"1" "2132" "4633" "4" "28" "28955.64" "0.09" "0.06" "N" "O" "1996-04-21" "1996-03-30" "1996-05-16" "NONE" "AIR" "lites. fluffily even de"
"1" "24027" "1534" "5" "24" "22824.48" "0.1" "0.04" "N" "O" "1996-03-30" "1996-03-14" "1996-04-01" "NONE" "FOB" " pending foxes. slyly re"
"1" "15635" "638" "6" "32" "49620.16" "0.07" "0.02" "N" "O" "1996-01-30" "1996-02-07" "1996-02-03" "DELIVER IN PERSON" "MAIL" "arefully slyly ex"
"2" "106170" "1191" "1" "38" "44694.46" "0.0" "0.05" "N" "O" "1997-01-28" "1997-01-14" "1997-02-02" "TAKE BACK RETURN" "RAIL" "ven requests. deposits breach a"
"3" "4297" "1798" "1" "45" "54058.05" "0.06" "0.0" "R" "F" "1994-02-02" "1994-01-04" "1994-02-23" "NONE" "AIR" "ongside of the furiously brave acco"
"3" "19036" "6540" "2" "49" "46796.47" "0.1" "0.0" "R" "F" "1993-11-09" "1993-12-20" "1993-11-24" "TAKE BACK RETURN" "RAIL" " unusual accounts. eve"
"3" "128449" "3474" "3" "27" "39890.88" "0.06" "0.07" "A" "F" "1994-01-16" "1993-11-22" "1994-01-23" "DELIVER IN PERSON" "SHIP" "nal foxes wake. "
'''

def get_expected_output(mode):
    return expected_results[mode]
