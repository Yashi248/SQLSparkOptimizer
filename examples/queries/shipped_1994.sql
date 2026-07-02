SELECT l_returnflag, SUM(l_extendedprice) AS revenue
FROM lineitem
WHERE YEAR(l_shipdate) = 1994
GROUP BY l_returnflag
