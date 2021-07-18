SELECT ar.name
FROM Track AS t
INNER JOIN Genre AS g
	ON t.GenreId = g.GenreId
INNER JOIN Album AS al
	ON t.AlbumId = al.AlbumId
INNER JOIN Artist AS ar
	ON  al.ArtistId = ar.ArtistId
WHERE g.name = 'latin'