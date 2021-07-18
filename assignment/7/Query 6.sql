SELECT *
FROM PlayList AS p
INNER JOIN PlayListTrack AS pt
	ON p.playlistId = pt.playlistId
INNER JOIN Track AS t
	ON t.trackId = pt.trackId
INNER JOIN genre AS g
	ON g.GenreId = t.GenreId
WHERE g.Name <> 'latin'