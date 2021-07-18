SELECT m.title, SUM(r.stars)
FROM Movie AS m
JOIN Rating AS r
	ON m.mId = r.mId
ORDER BY Title ASC