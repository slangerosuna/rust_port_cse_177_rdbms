SELECT SUM(l_extendedprice * l_discount) 
FROM lineitem
WHERE l_discount < 0.07 AND l_quantity < 24;

SELECT l_extendedprice
  FROM lineitem, supplier;

SELECT SUM(ps_supplycost), suppkey 
FROM part, supplier, partsupp 
WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_acctbal > 2500.00
GROUP BY s_suppkey;

SELECT SUM(c_acctbal), name 
FROM customer, orders 
WHERE c_custkey = o_custkey AND o_totalprice < 10000
GROUP BY c_name;

SELECT l_orderkey, l_partkey, l_suppkey 
FROM lineitem
WHERE l_returnflag = 'R' AND l_discount < 0.04 AND l_shipmode = 'MAIL';

SELECT DISTINCT c_name, c_address, c_acctbal 
FROM customer 
WHERE c_name = 'Customer#000070919';

SELECT n_name, SUM(l_discount) 
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey AND c_nationkey = n_nationkey AND
	c_acctbal < 1000 AND l_quantity > 30 AND l_discount < 0.03
GROUP BY n_name;

SELECT l_orderkey 
FROM lineitem
WHERE l_quantity > 30;

SELECT DISTINCT c_name 
FROM lineitem, orders, customer, nation, region
WHERE l_orderkey = o_orderkey AND o_custkey = c_custkey AND 
	c_nationkey = n_nationkey AND n_regionkey = r_regionkey;

SELECT l_discount
FROM lineitem, orders, customer, nation, region
WHERE l_orderkey = o_orderkey AND o_custkey = c_custkey AND 
	c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND 
	r_regionkey = 1 AND o_orderkey < 10000;
