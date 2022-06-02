from dateutil.relativedelta import relativedelta
import datetime
import ibis

def add_date(datestr, dy=0, dm=0, dd=0):
    dt = datetime.date.fromisoformat(datestr)
    dt += relativedelta(years=dy, months=dm, days=dd)
    return dt.isoformat()

def tpc_h01(con, DELTA=90, DATE="1998-12-01"):
    """
    The Pricing Summary Report Query provides a summary pricing report for all
    lineitems shipped as of a given date.  The  date is  within  60  - 120 days
    of  the  greatest  ship  date  contained  in  the database.  The query
    lists totals  for extended  price,  discounted  extended price, discounted
    extended price  plus  tax,  average  quantity, average extended price,  and
    average discount.  These  aggregates  are grouped  by RETURNFLAG  and
    LINESTATUS, and  listed  in ascending  order of RETURNFLAG and  LINESTATUS.
    A  count  of the  number  of  lineitems in each  group  is included.
    """

    t = con.table("lineitem")

    interval = add_date(DATE, dd=-1 * DELTA)
    q = t.filter(t.l_shipdate <= interval)
    discount_price = t.l_extendedprice * (1 - t.l_discount)
    charge = discount_price * (1 + t.l_tax)
    q = q.group_by(["l_returnflag", "l_linestatus"])
    q = q.aggregate(
        sum_qty=t.l_quantity.sum(),
        sum_base_price=t.l_extendedprice.sum(),
        sum_disc_price=discount_price.sum(),
        sum_charge=charge.sum(),
        avg_qty=t.l_quantity.mean(),
        avg_price=t.l_extendedprice.mean(),
        avg_disc=t.l_discount.mean(),
        count_order=t.count(),
    )
    q = q.sort_by(["l_returnflag", "l_linestatus"])
    return q

def tpc_h02(con, REGION="EUROPE", SIZE=25, TYPE="BRASS"):
    "Minimum Cost Supplier Query (Q2)"

    part = con.table("part")
    supplier = con.table("supplier")
    partsupp = con.table("partsupp")
    nation = con.table("nation")
    region = con.table("region")

    expr = (
        part.join(partsupp, part.p_partkey == partsupp.ps_partkey)
        .join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    )

    subexpr = (
        partsupp.join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    )

    subexpr = subexpr[
        (subexpr.r_name == REGION) & (expr.p_partkey == subexpr.ps_partkey)
    ]

    filters = [
        expr.p_size == SIZE,
        expr.p_type.like("%" + TYPE),
        expr.r_name == REGION,
        expr.ps_supplycost == subexpr.ps_supplycost.min(),
    ]
    q = expr.filter(filters)

    q = q.select(
        [
            q.s_acctbal,
            q.s_name,
            q.n_name,
            q.p_partkey,
            q.p_mfgr,
            q.s_address,
            q.s_phone,
            q.s_comment,
        ]
    )

    return q.sort_by(
        [
            ibis.desc(q.s_acctbal),
            q.n_name,
            q.s_name,
            q.p_partkey,
        ]
    ).limit(100)

def tpc_h03(con, MKTSEGMENT="BUILDING", DATE="1995-03-15"):
    customer = con.table("customer")
    orders = con.table("orders")
    lineitem = con.table("lineitem")

    q = customer.join(orders, customer.c_custkey == orders.o_custkey)
    q = q.join(lineitem, lineitem.l_orderkey == orders.o_orderkey)
    q = q.filter(
        [q.c_mktsegment == MKTSEGMENT, q.o_orderdate < DATE, q.l_shipdate > DATE]
    )
    qg = q.group_by([q.l_orderkey, q.o_orderdate, q.o_shippriority])
    q = qg.aggregate(revenue=(q.l_extendedprice * (1 - q.l_discount)).sum())
    q = q.sort_by([ibis.desc(q.revenue), q.o_orderdate])
    q = q.limit(10)

    return q

def tpc_h04(con, DATE="1993-07-01"):
    orders = con.table("orders")
    lineitem = con.table("lineitem")
    cond = (lineitem.l_orderkey == orders.o_orderkey) & (
        lineitem.l_commitdate < lineitem.l_receiptdate
    )
    q = orders.filter(
        [
            cond.any(),
            orders.o_orderdate >= DATE,
            orders.o_orderdate < add_date(DATE, dm=3),
        ]
    )
    q = q.group_by([orders.o_orderpriority])
    q = q.aggregate(order_count=orders.count())
    q = q.sort_by([orders.o_orderpriority])
    return q

def tpc_h05(con, NAME="ASIA", DATE="1994-01-01"):
    customer = con.table("customer")
    orders = con.table("orders")
    lineitem = con.table("lineitem")
    supplier = con.table("supplier")
    nation = con.table("nation")
    region = con.table("region")

    q = customer
    q = q.join(orders, customer.c_custkey == orders.o_custkey)
    q = q.join(lineitem, lineitem.l_orderkey == orders.o_orderkey)
    q = q.join(supplier, lineitem.l_suppkey == supplier.s_suppkey)
    q = q.join(
        nation,
        (customer.c_nationkey == supplier.s_nationkey)
        & (supplier.s_nationkey == nation.n_nationkey),
    )
    q = q.join(region, nation.n_regionkey == region.r_regionkey)

    q = q.filter(
        [q.r_name == NAME, q.o_orderdate >= DATE, q.o_orderdate < add_date(DATE, dy=1)]
    )
    revexpr = q.l_extendedprice * (1 - q.l_discount)
    gq = q.group_by([q.n_name])
    q = gq.aggregate(revenue=revexpr.sum())
    q = q.sort_by([ibis.desc(q.revenue)])
    return q

def tpc_h06(con, DATE="1994-01-01", DISCOUNT=0.06, QUANTITY=24):
    q = con.table("lineitem")
    discount_min = round(DISCOUNT - 0.01, 2)
    discount_max = round(DISCOUNT + 0.01, 2)
    q = q.filter(
        [
            q.l_shipdate >= DATE,
            q.l_shipdate < add_date(DATE, dy=1),
            q.l_discount.between(discount_min, discount_max),
            q.l_quantity < QUANTITY,
        ]
    )
    q = q.aggregate(revenue=(q.l_extendedprice * q.l_discount).sum())
    return q

def tpc_h07(con, NATION1="FRANCE", NATION2="GERMANY", DATE="1995-01-01"):
    supplier = con.table("supplier")
    lineitem = con.table("lineitem")
    orders = con.table("orders")
    customer = con.table("customer")
    nation = con.table("nation")

    q = supplier
    q = q.join(lineitem, supplier.s_suppkey == lineitem.l_suppkey)
    q = q.join(orders, orders.o_orderkey == lineitem.l_orderkey)
    q = q.join(customer, customer.c_custkey == orders.o_custkey)
    n1 = nation
    n2 = nation.view()
    q = q.join(n1, supplier.s_nationkey == n1.n_nationkey)
    q = q.join(n2, customer.c_nationkey == n2.n_nationkey)

    q = q[
        n1.n_name.name("supp_nation"),
        n2.n_name.name("cust_nation"),
        lineitem.l_shipdate,
        lineitem.l_extendedprice,
        lineitem.l_discount,
        lineitem.l_shipdate.year().cast("string").name("l_year"),
        (lineitem.l_extendedprice * (1 - lineitem.l_discount)).name("volume"),
    ]

    q = q.filter(
        [
            ((q.cust_nation == NATION1) & (q.supp_nation == NATION2))
            | ((q.cust_nation == NATION2) & (q.supp_nation == NATION1)),
            q.l_shipdate.between(DATE, add_date(DATE, dy=2, dd=-1)),
        ]
    )

    gq = q.group_by(["supp_nation", "cust_nation", "l_year"])
    q = gq.aggregate(revenue=q.volume.sum())
    q = q.sort_by(["supp_nation", "cust_nation", "l_year"])

    return q

def tpc_h08(
    con,
    NATION="BRAZIL",
    REGION="AMERICA",
    TYPE="ECONOMY ANODIZED STEEL",
    DATE="1995-01-01",
):
    part = con.table("part")
    supplier = con.table("supplier")
    lineitem = con.table("lineitem")
    orders = con.table("orders")
    customer = con.table("customer")
    region = con.table("region")
    n1 = con.table("nation")
    n2 = n1.view()

    q = part
    q = q.join(lineitem, part.p_partkey == lineitem.l_partkey)
    q = q.join(supplier, supplier.s_suppkey == lineitem.l_suppkey)
    q = q.join(orders, lineitem.l_orderkey == orders.o_orderkey)
    q = q.join(customer, orders.o_custkey == customer.c_custkey)
    q = q.join(n1, customer.c_nationkey == n1.n_nationkey)
    q = q.join(region, n1.n_regionkey == region.r_regionkey)
    q = q.join(n2, supplier.s_nationkey == n2.n_nationkey)

    q = q[
        orders.o_orderdate.year().cast("string").name("o_year"),
        (lineitem.l_extendedprice * (1 - lineitem.l_discount)).name("volume"),
        n2.n_name.name("nation"),
        region.r_name,
        orders.o_orderdate,
        part.p_type,
    ]

    q = q.filter(
        [
            q.r_name == REGION,
            q.o_orderdate.between(DATE, add_date(DATE, dy=2, dd=-1)),
            q.p_type == TYPE,
        ]
    )

    q = q.mutate(
        nation_volume=ibis.case().when(q.nation == NATION, q.volume).else_(0).end()
    )
    gq = q.group_by([q.o_year])
    q = gq.aggregate(mkt_share=q.nation_volume.sum() / q.volume.sum())
    q = q.sort_by([q.o_year])
    return q

def tpc_h09(con, COLOR="green"):
    part = con.table("part")
    supplier = con.table("supplier")
    lineitem = con.table("lineitem")
    partsupp = con.table("partsupp")
    orders = con.table("orders")
    nation = con.table("nation")

    q = lineitem
    q = q.join(supplier, supplier.s_suppkey == lineitem.l_suppkey)
    q = q.join(
        partsupp,
        (partsupp.ps_suppkey == lineitem.l_suppkey)
        & (partsupp.ps_partkey == lineitem.l_partkey),
    )
    q = q.join(part, part.p_partkey == lineitem.l_partkey)
    q = q.join(orders, orders.o_orderkey == lineitem.l_orderkey)
    q = q.join(nation, supplier.s_nationkey == nation.n_nationkey)

    q = q[
        (q.l_extendedprice * (1 - q.l_discount) - q.ps_supplycost * q.l_quantity).name(
            "amount"
        ),
        q.o_orderdate.year().cast("string").name("o_year"),
        q.n_name.name("nation"),
        q.p_name,
    ]

    q = q.filter([q.p_name.like("%" + COLOR + "%")])

    gq = q.group_by([q.nation, q.o_year])
    q = gq.aggregate(sum_profit=q.amount.sum())
    q = q.sort_by([q.nation, ibis.desc(q.o_year)])
    return q

def tpc_h10(con, DATE="1993-10-01"):
    customer = con.table("customer")
    orders = con.table("orders")
    lineitem = con.table("lineitem")
    nation = con.table("nation")

    q = customer
    q = q.join(orders, customer.c_custkey == orders.o_custkey)
    q = q.join(lineitem, lineitem.l_orderkey == orders.o_orderkey)
    q = q.join(nation, customer.c_nationkey == nation.n_nationkey)

    q = q.filter(
        [
            (q.o_orderdate >= DATE) & (q.o_orderdate < add_date(DATE, dm=3)),
            q.l_returnflag == "R",
        ]
    )

    gq = q.group_by(
        [
            q.c_custkey,
            q.c_name,
            q.c_acctbal,
            q.c_phone,
            q.n_name,
            q.c_address,
            q.c_comment,
        ]
    )
    q = gq.aggregate(revenue=(q.l_extendedprice * (1 - q.l_discount)).sum())

    q = q.sort_by(ibis.desc(q.revenue))
    return q.limit(20)

def tpc_h11(con, NATION="GERMANY", FRACTION=0.0001):
    partsupp = con.table("partsupp")
    supplier = con.table("supplier")
    nation = con.table("nation")

    q = partsupp
    q = q.join(supplier, partsupp.ps_suppkey == supplier.s_suppkey)
    q = q.join(nation, nation.n_nationkey == supplier.s_nationkey)

    q = q.filter([q.n_name == NATION])

    innerq = partsupp
    innerq = innerq.join(supplier, partsupp.ps_suppkey == supplier.s_suppkey)
    innerq = innerq.join(nation, nation.n_nationkey == supplier.s_nationkey)
    innerq = innerq.filter([innerq.n_name == NATION])
    innerq = innerq.aggregate(total=(innerq.ps_supplycost * innerq.ps_availqty).sum())

    gq = q.group_by([q.ps_partkey])
    q = gq.aggregate(value=(q.ps_supplycost * q.ps_availqty).sum())
    q = q.filter([q.value > innerq.total * FRACTION])
    q = q.sort_by(ibis.desc(q.value))
    return q

def tpc_h12(con, SHIPMODE1="MAIL", SHIPMODE2="SHIP", DATE="1994-01-01"):
    """'Shipping Modes and Order Priority Query (Q12)
    This query determines whether selecting less expensive modes of shipping is
    negatively affecting the critical-prior- ity orders by causing more parts
    to be received by customers after the committed date."""

    orders = con.table("orders")
    lineitem = con.table("lineitem")
    q = orders
    q = q.join(lineitem, orders.o_orderkey == lineitem.l_orderkey)

    q = q.filter(
        [
            q.l_shipmode.isin([SHIPMODE1, SHIPMODE2]),
            q.l_commitdate < q.l_receiptdate,
            q.l_shipdate < q.l_commitdate,
            q.l_receiptdate >= DATE,
            q.l_receiptdate < add_date(DATE, dy=1),
        ]
    )

    gq = q.group_by([q.l_shipmode])
    q = gq.aggregate(
        high_line_count=(
            q.o_orderpriority.case()
            .when("1-URGENT", 1)
            .when("2-HIGH", 1)
            .else_(0)
            .end()
        ).sum(),
        low_line_count=(
            q.o_orderpriority.case()
            .when("1-URGENT", 0)
            .when("2-HIGH", 0)
            .else_(1)
            .end()
        ).sum(),
    )
    q = q.sort_by(q.l_shipmode)

    return q

def tpc_h13(con, WORD1="special", WORD2="requests"):
    """Customer Distribution Query (Q13)
    This query seeks relationships between customers and the size of their
    orders."""

    customer = con.table("customer")
    orders = con.table("orders")
    innerq = customer
    innerq = innerq.left_join(
        orders,
        (customer.c_custkey == orders.o_custkey)
        & ~orders.o_comment.like(f"%{WORD1}%{WORD2}%"),
    )
    innergq = innerq.group_by([innerq.c_custkey])
    innerq = innergq.aggregate(c_count=innerq.o_orderkey.count())

    gq = innerq.group_by([innerq.c_count])
    q = gq.aggregate(custdist=innerq.count())

    q = q.sort_by([ibis.desc(q.custdist), ibis.desc(q.c_count)])
    return q

def tpc_h14(con, DATE="1995-09-01"):
    """Promotion Effect Query (Q14)
    This query monitors the market response to a promotion such as TV
    advertisements or a special campaign."""

    lineitem = con.table("lineitem")
    part = con.table("part")
    q = lineitem
    q = q.join(part, lineitem.l_partkey == part.p_partkey)
    q = q.filter([q.l_shipdate >= DATE, q.l_shipdate < add_date(DATE, dm=1)])

    revenue = q.l_extendedprice * (1 - q.l_discount)
    promo_revenue = q.p_type.like("PROMO%").ifelse(revenue, 0)

    q = q.aggregate(promo_revenue=100 * promo_revenue.sum() / revenue.sum())
    return q

def tpc_h15(con, DATE="1996-01-01"):
    "Top Supplier Query (Q15)"

    lineitem = con.table("lineitem")
    supplier = con.table("supplier")

    qrev = lineitem
    qrev = qrev.filter(
        [lineitem.l_shipdate >= DATE, lineitem.l_shipdate < add_date(DATE, dm=3)]
    )

    gqrev = qrev.group_by([lineitem.l_suppkey])
    qrev = gqrev.aggregate(
        total_revenue=(qrev.l_extendedprice * (1 - qrev.l_discount)).sum()
    )

    q = supplier.join(qrev, supplier.s_suppkey == qrev.l_suppkey)
    q = q.filter([q.total_revenue == qrev.total_revenue.max()])
    q = q.sort_by([q.s_suppkey])
    q = q[q.s_suppkey, q.s_name, q.s_address, q.s_phone, q.total_revenue]
    return q



def tpc_h16(
    con, BRAND="Brand#45", TYPE="MEDIUM POLISHED", SIZES=(49, 14, 23, 45, 19, 3, 36, 9)
):
    #ibis.options.sql.default_limit = 100000

    """Parts/Supplier Relationship Query (Q16)
    This query finds out how many suppliers can supply parts with given
    attributes. It might be used, for example, to determine whether there is
    a sufficient number of suppliers for heavily ordered parts."""

    partsupp = con.table("partsupp")
    part = con.table("part")
    supplier = con.table("supplier")

    q = partsupp.join(part, part.p_partkey == partsupp.ps_partkey)
    q = q.filter(
        [
            q.p_brand != BRAND,
            ~q.p_type.like(f"{TYPE}%"),
            q.p_size.isin(SIZES),
            ~q.ps_suppkey.isin(
                supplier.filter(
                    [supplier.s_comment.like("%Customer%Complaints%")]
                ).s_suppkey
            ),
        ]
    )
    gq = q.groupby([q.p_brand, q.p_type, q.p_size])
    q = gq.aggregate(supplier_cnt=q.ps_suppkey.nunique())
    q = q.sort_by([ibis.desc(q.supplier_cnt), q.p_brand, q.p_type, q.p_size])
    return q

def tpc_h17(con, BRAND="Brand#23", CONTAINER="MED BOX"):
    """Small-Quantity-Order Revenue Query (Q17)
    This query determines how much average yearly revenue would be lost if
    orders were no longer filled for small quantities of certain parts. This
    may reduce overhead expenses by concentrating sales on larger shipments."""

    lineitem = con.table("lineitem")
    part = con.table("part")

    q = lineitem.join(part, part.p_partkey == lineitem.l_partkey)

    innerq = lineitem
    innerq = innerq.filter([innerq.l_partkey == q.p_partkey])

    q = q.filter(
        [
            q.p_brand == BRAND,
            q.p_container == CONTAINER,
            q.l_quantity < (0.2 * innerq.l_quantity.mean()),
        ]
    )
    q = q.aggregate(avg_yearly=q.l_extendedprice.sum() / 7.0)
    return q

def tpc_h18(con, QUANTITY=300):
    """Large Volume Customer Query (Q18)
    The Large Volume Customer Query ranks customers based on their having
    placed a large quantity order. Large quantity orders are defined as those
    orders whose total quantity is above a certain level."""

    customer = con.table("customer")
    orders = con.table("orders")
    lineitem = con.table("lineitem")

    subgq = lineitem.groupby([lineitem.l_orderkey])
    subq = subgq.aggregate(qty_sum=lineitem.l_quantity.sum())
    subq = subq.filter([subq.qty_sum > QUANTITY])

    q = customer
    q = q.join(orders, customer.c_custkey == orders.o_custkey)
    q = q.join(lineitem, orders.o_orderkey == lineitem.l_orderkey)
    q = q.filter([q.o_orderkey.isin(subq.l_orderkey)])

    gq = q.groupby([q.c_name, q.c_custkey, q.o_orderkey, q.o_orderdate, q.o_totalprice])
    q = gq.aggregate(sum_qty=q.l_quantity.sum())
    q = q.sort_by([ibis.desc(q.o_totalprice), q.o_orderdate])
    return q.limit(100)

def tpc_h19(
    con,
    QUANTITY1=1,
    QUANTITY2=10,
    QUANTITY3=20,
    BRAND1="Brand#12",
    BRAND2="Brand#23",
    BRAND3="Brand#34",
):
    """Discounted Revenue Query (Q19)
    The Discounted Revenue Query reports the gross discounted revenue
    attributed to the sale of selected parts handled in a particular manner.
    This query is an example of code such as might be produced programmatically
    by a data mining tool."""

    lineitem = con.table("lineitem")
    part = con.table("part")
    q = lineitem.join(part, part.p_partkey == lineitem.l_partkey)

    q1 = (
        (q.p_brand == BRAND1)
        & (q.p_container.isin(("SM CASE", "SM BOX", "SM PACK", "SM PKG")))
        & (q.l_quantity >= QUANTITY1)
        & (q.l_quantity <= QUANTITY1 + 10)
        & (q.p_size.between(1, 5))
        & (q.l_shipmode.isin(("AIR", "AIR REG")))
        & (q.l_shipinstruct == "DELIVER IN PERSON")
    )

    q2 = (
        (q.p_brand == BRAND2)
        & (q.p_container.isin(("MED BAG", "MED BOX", "MED PKG", "MED PACK")))
        & (q.l_quantity >= QUANTITY2)
        & (q.l_quantity <= QUANTITY2 + 10)
        & (q.p_size.between(1, 10))
        & (q.l_shipmode.isin(("AIR", "AIR REG")))
        & (q.l_shipinstruct == "DELIVER IN PERSON")
    )

    q3 = (
        (q.p_brand == BRAND3)
        & (q.p_container.isin(("LG CASE", "LG BOX", "LG PACK", "LG PKG")))
        & (q.l_quantity >= QUANTITY3)
        & (q.l_quantity <= QUANTITY3 + 10)
        & (q.p_size.between(1, 15))
        & (q.l_shipmode.isin(("AIR", "AIR REG")))
        & (q.l_shipinstruct == "DELIVER IN PERSON")
    )

    q = q.filter([q1 | q2 | q3])
    q = q.aggregate(revenue=(q.l_extendedprice * (1 - q.l_discount)).sum())
    return q

def tpc_h20(con, COLOR="forest", DATE="1994-01-01", NATION="CANADA"):
    """Potential Part Promotion Query (Q20)
    The Potential Part Promotion Query identifies suppliers in a particular
    nation having selected parts that may be candidates for a promotional
    offer."""

    supplier = con.table("supplier")
    nation = con.table("nation")
    partsupp = con.table("partsupp")
    part = con.table("part")
    lineitem = con.table("lineitem")

    q1 = supplier.join(nation, supplier.s_nationkey == nation.n_nationkey)

    q3 = part.filter([part.p_name.like(f"{COLOR}%")])
    q2 = partsupp

    q4 = lineitem.filter(
        [
            lineitem.l_partkey == q2.ps_partkey,
            lineitem.l_suppkey == q2.ps_suppkey,
            lineitem.l_shipdate >= DATE,
            lineitem.l_shipdate < add_date(DATE, dy=1),
        ]
    )

    q2 = q2.filter(
        [
            partsupp.ps_partkey.isin(q3.p_partkey),
            partsupp.ps_availqty > 0.5 * q4.l_quantity.sum(),
        ]
    )

    q1 = q1.filter([q1.n_name == NATION, q1.s_suppkey.isin(q2.ps_suppkey)])

    q1 = q1[q1.s_name, q1.s_address]

    return q1.sort_by(q1.s_name)

def tpc_h21(con, NATION="SAUDI ARABIA"):
    """Suppliers Who Kept Orders Waiting Query (Q21)
    This query identifies certain suppliers who were not able to ship required
    parts in a timely manner."""

    supplier = con.table("supplier")
    lineitem = con.table("lineitem")
    orders = con.table("orders")
    nation = con.table("nation")

    L2 = lineitem.view()
    L3 = lineitem.view()

    q = supplier
    q = q.join(lineitem, supplier.s_suppkey == lineitem.l_suppkey)
    q = q.join(orders, orders.o_orderkey == lineitem.l_orderkey)
    q = q.join(nation, supplier.s_nationkey == nation.n_nationkey)
    q = q[
        q.l_orderkey.name("l1_orderkey"),
        q.o_orderstatus,
        q.l_receiptdate,
        q.l_commitdate,
        q.l_suppkey.name("l1_suppkey"),
        q.s_name,
        q.n_name,
    ]
    q = q.filter(
        [
            q.o_orderstatus == "F",
            q.l_receiptdate > q.l_commitdate,
            q.n_name == NATION,
            ((L2.l_orderkey == q.l1_orderkey) & (L2.l_suppkey != q.l1_suppkey)).any(),
            ~(
                (
                    (L3.l_orderkey == q.l1_orderkey)
                    & (L3.l_suppkey != q.l1_suppkey)
                    & (L3.l_receiptdate > L3.l_commitdate)
                ).any()
            ),
        ]
    )

    gq = q.group_by([q.s_name])
    q = gq.aggregate(numwait=q.count())
    q = q.sort_by([ibis.desc(q.numwait), q.s_name])
    return q.limit(100)

def tpc_h22(con, COUNTRY_CODES=("13", "31", "23", "29", "30", "18", "17")):
    """Global Sales Opportunity Query (Q22)
    The Global Sales Opportunity Query identifies geographies where there are
    customers who may be likely to make a purchase."""

    customer = con.table("customer")
    orders = con.table("orders")

    q = customer.filter(
        [
            customer.c_acctbal > 0.00,
            customer.c_phone.substr(0, 2).isin(COUNTRY_CODES),
        ]
    )
    q = q.aggregate(avg_bal=customer.c_acctbal.mean())

    custsale = customer.filter(
        [
            customer.c_phone.substr(0, 2).isin(COUNTRY_CODES),
            customer.c_acctbal > q.avg_bal,
            ~(orders.o_custkey == customer.c_custkey).any(),
        ]
    )
    custsale = custsale[
        customer.c_phone.substr(0, 2).name("cntrycode"), customer.c_acctbal
    ]

    gq = custsale.group_by(custsale.cntrycode)
    outerq = gq.aggregate(numcust=custsale.count(), totacctbal=custsale.c_acctbal.sum())

    return outerq.sort_by(outerq.cntrycode)

def invalid_query(con):
	assert 0

def get_tpch_query(query_number):
	return {
        1: tpc_h01,
        2: tpc_h02,
        3: tpc_h03,
        4: tpc_h04,
        5: tpc_h05,
        6: tpc_h06,
        7: tpc_h07,
        8: tpc_h08,
        9: tpc_h09,
        10: tpc_h10,
        11: tpc_h11,
        12: tpc_h12,
        13: tpc_h13,
        14: tpc_h14,
        15: tpc_h15,
        16: tpc_h16,
        17: tpc_h17,
        18: tpc_h18,
        19: tpc_h19,
        20: tpc_h20,
        21: tpc_h21,
        22: tpc_h22
    }.get(query_number, invalid_query)
	
