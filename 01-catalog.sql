--create table data to store data about tables
CREATE TABLE tables (
  name VARCHAR(255),
  num_tuples INT,
  file VARCHAR(255)
);

--create attributes table to store data about attributes
CREATE TABLE attributes (
  table_name VARCHAR(255),
  position INT,
  name VARCHAR(255),
  type VARCHAR(255),
  num_distinct INT
);

--insert sampling data into tables
insert into tables values('customer', 1500, 'customer.dat');
insert into tables values('lineitem', 60175, 'lineitem.dat');
insert into tables values('nation', 25, 'nation.dat');
insert into tables values('orders', 15000, 'orders.dat');
insert into tables values('part', 2000, 'part.dat');
insert into tables values('region', 5, 'region.dat');
insert into tables values('partsupp', 8000, 'partsupp.dat');
insert into tables values('supplier', 100, 'supplier.dat');


insert into attributes values('customer', 0, 'c_custkey', 'INTEGER', 1500);
insert into attributes values('customer', 1, 'c_name', 'STRING', 1500);
insert into attributes values('customer', 2, 'c_address', 'STRING', 1500);
insert into attributes values('customer', 3, 'c_nationkey', 'INTEGER', 25);
insert into attributes values('customer', 4, 'c_phone', 'STRING', 1500);
insert into attributes values('customer', 5, 'c_acctbal', 'FLOAT', 1499);
insert into attributes values('customer', 6, 'c_mktsegment', 'STRING', 5);
insert into attributes values('customer', 7, 'c_comment', 'STRING', 1500);

insert into attributes values('lineitem', 0, 'l_orderkey', 'INTEGER', 15000);
insert into attributes values('lineitem', 1, 'l_partkey', 'INTEGER', 2000);
insert into attributes values('lineitem', 2, 'l_suppkey', 'INTEGER', 100);
insert into attributes values('lineitem', 3, 'l_linenumber', 'INTEGER', 7);
insert into attributes values('lineitem', 4, 'l_quantity', 'INTEGER', 50);
insert into attributes values('lineitem', 5, 'l_extendedprice', 'FLOAT', 35921);
insert into attributes values('lineitem', 6, 'l_discount', 'FLOAT', 11);
insert into attributes values('lineitem', 7, 'l_tax', 'FLOAT', 9);
insert into attributes values('lineitem', 8, 'l_returnflag', 'STRING', 3);
insert into attributes values('lineitem', 9, 'l_linestatus', 'STRING', 2);
insert into attributes values('lineitem', 10, 'l_shipdate', 'STRING', 2518);
insert into attributes values('lineitem', 11, 'l_commitdate', 'STRING', 2460);
insert into attributes values('lineitem', 12, 'l_receiptdate', 'STRING', 2529);
insert into attributes values('lineitem', 13, 'l_shipinstruct', 'STRING', 4);
insert into attributes values('lineitem', 14, 'l_shipmode', 'STRING', 7);
insert into attributes values('lineitem', 15, 'l_comment', 'STRING', 60175);

insert into attributes values('nation', 0, 'n_nationkey', 'INTEGER', 25);
insert into attributes values('nation', 1, 'n_name', 'STRING', 25);
insert into attributes values('nation', 2, 'n_regionkey', 'INTEGER', 5);
insert into attributes values('nation', 3, 'n_comment', 'STRING', 25);

insert into attributes values('orders', 0, 'o_orderkey', 'INTEGER', 15000);
insert into attributes values('orders', 1, 'o_custkey', 'INTEGER', 1000);
insert into attributes values('orders', 2, 'o_orderstatus', 'STRING', 3);
insert into attributes values('orders', 3, 'o_totalprice', 'FLOAT', 14996);
insert into attributes values('orders', 4, 'o_orderdate', 'STRING', 2401);
insert into attributes values('orders', 5, 'o_orderpriority', 'STRING', 5);
insert into attributes values('orders', 6, 'o_clerk', 'STRING', 1000);
insert into attributes values('orders', 7, 'o_shippriority', 'STRING', 1);
insert into attributes values('orders', 8, 'o_comment', 'STRING', 15000);

insert into attributes values('part', 0, 'p_partkey', 'INTEGER', 2000);
insert into attributes values('part', 1, 'p_name', 'STRING', 2000);
insert into attributes values('part', 2, 'p_mfgr', 'STRING', 5);
insert into attributes values('part', 3, 'p_brand', 'STRING', 25);
insert into attributes values('part', 4, 'p_type', 'STRING', 150);
insert into attributes values('part', 5, 'p_size', 'INTEGER', 50);
insert into attributes values('part', 6, 'p_container', 'STRING', 40);
insert into attributes values('part', 7, 'p_retailprice', 'FLOAT', 1099);
insert into attributes values('part', 8, 'p_comment', 'STRING', 2000);

insert into attributes values('partsupp', 0, 'ps_partkey', 'INTEGER', 2000);
insert into attributes values('partsupp', 1, 'ps_suppkey', 'INTEGER', 2000);
insert into attributes values('partsupp', 2, 'ps_availqty', 'INTEGER', 5497);
insert into attributes values('partsupp', 3, 'ps_supplycost', 'FLOAT', 7665);
insert into attributes values('partsupp', 4, 'ps_comment', 'STRING', 8000);

insert into attributes values('region', 0, 'r_regionkey', 'INTEGER', 5);
insert into attributes values('region', 1, 'r_name', 'STRING', 5);
insert into attributes values('region', 2, 'r_comment', 'STRING', 5);

insert into attributes values('supplier', 0, 's_suppkey', 'INTEGER', 100);
insert into attributes values('supplier', 1, 's_name', 'STRING', 100);
insert into attributes values('supplier', 2, 's_address', 'STRING', 100);
insert into attributes values('supplier', 3, 's_nationkey', 'INTEGER', 25);
insert into attributes values('supplier', 4, 's_phone', 'STRING', 100);
insert into attributes values('supplier', 5, 's_acctbal', 'FLOAT', 100);
insert into attributes values('supplier', 6, 's_comment', 'STRING', 100);
