SELECT rw.Name, m.Title, r.Stars, r.RatingDate
FROM Movie AS m
JOIN Rating AS r
	ON m.mId = r.mId
JOIN Reviewer AS rw
	ON rw.rId = r.rId
ORDER BY rw.Name, m.Title, r.Stars, r.RatingDate ASC