SELECT a.Name
FROM album AS al
INNER JOIN artist AS a
	ON al.ArtistId = a.ArtistId
GROUP BY a.Name 
HAVING COUNT(a.Name) > 5