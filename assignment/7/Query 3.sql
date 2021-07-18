SELECT c.firstName, c.LastName
FROM customers AS c
INNER JOIN Employee AS em
	ON c.SupportReportId = em.EmployeeId
WHERE em.ReportsTo = null