SELECT name
FROM Artist
WHERE ArtistId
NOT IN
(SELECT ArtistId FROM Album)

/*
I used the following syntax
```
SELECT customerName, customercity, customermail, salestotal
FROM onlinecustomers AS oc
   INNER JOIN
   orders AS o
   ON oc.customerid = o.customerid
   INNER JOIN
   sales AS s
   ON o.orderId = s.orderId
```

source: https://www.sqlshack.com/sql-multiple-joins-for-beginners-with-examples/
*/