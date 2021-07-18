SELECT DISTINCT m.year
FROM Movie AS m
JOIN Rating AS r
	ON m.mId = r.mId
WHERE r.stars >= 4 and r.stars <= 5
ORDER BY m.year ASC