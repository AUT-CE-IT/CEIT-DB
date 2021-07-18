SELECT SUM(t.Bytes), SUM(t.UnitPrice)
FROM Track AS t
INNER JOIN Genre AS g
	ON t.GenreId = g.GenreId
WHERE g.Name = 'Grunge'