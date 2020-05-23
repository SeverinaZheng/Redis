package db;

public class Queries {
	static String[] readQueries = {
			"SELECT ccode, credits\r\n" + 
			"FROM course\r\n" + 
			"WHERE dept = 'computer science' AND credits IN (3, 1)\r\n" + 
			"ORDER BY credits DESC, ccode;",
			
			"SELECT ccode, credits\r\n" + 
			"FROM course \r\n" + 
			"WHERE dept = 'computer science'\r\n" + 
			"  AND ccode in (SELECT ccode FROM courseoffer WHERE term = 'winter 2018')\r\n" + 
			"ORDER BY ccode;",
			
			"SELECT DISTINCT c.ccode, c.credits\r\n" + 
			"FROM course c, courseoffer o \r\n" + 
			"WHERE c.ccode = o.ccode\r\n" + 
			"  AND c.dept = 'computer science'\r\n" + 
			"	AND o.term = 'winter 2018' \r\n" + 
			"ORDER BY ccode;",
			
			"SELECT ccode, credits\r\n" + 
			"FROM course c\r\n" + 
			"WHERE dept = 'computer science'\r\n" + 
			"  AND EXISTS (SELECT * FROM courseoffer WHERE term = 'winter 2018' AND ccode = c.ccode)\r\n" + 
			"ORDER BY ccode;",
			
			"SELECT DISTINCT c.ccode, c.credits\r\n" + 
			"FROM enroll e, course c\r\n" + 
			"WHERE e.ccode = c.ccode AND e.sid = 12345678\r\n" + 
			"ORDER BY c.ccode;",
			
			"SELECT c.ccode, c.credits\r\n" + 
			"FROM enroll e, course c\r\n" + 
			"WHERE e.ccode = c.ccode\r\n" + 
			"  AND e.sid = 12345678  AND e.term = 'winter 2018'\r\n" + 
			"ORDER BY c.ccode;",
			
			"SELECT DISTINCT ccode\r\n" + 
			"FROM courseoffer\r\n" + 
			"WHERE term = 'winter 2017' AND ccode NOT IN\r\n" + 
			"  (SELECT ccode FROM courseoffer WHERE term = 'winter 2018')\r\n" + 
			"ORDER BY ccode;",
			
			"SELECT ccode, term, grade\r\n" + 
			"FROM enroll e\r\n" + 
			"WHERE e.sid = 12345678 \r\n" + 
			"  AND EXISTS\r\n" + 
			"(\r\n" + 
			"  SELECT * FROM course WHERE dept = 'computer science' and ccode = e.ccode\r\n" + 
			")\r\n" + 
			"ORDER BY ccode, term;",
			
			"SELECT sid, sname\r\n" + 
			"FROM student\r\n" + 
			"WHERE sid IN\r\n" + 
			"(\r\n" + 
			"  SELECT sid\r\n" + 
			"	FROM enroll\r\n" + 
			"	WHERE (ccode, term) IN (SELECT ccode, term FROM enroll WHERE sid = 12345678) \r\n" + 
			"	  AND sid <> 12345678\r\n" + 
			")\r\n" + 
			"ORDER BY sid;",

			"SELECT COUNT(*) as numstudents FROM student;",

			"SELECT COUNT(distinct sid) AS numstudents\r\n" + 
			"FROM enroll\r\n" + 
			"WHERE term = 'winter 2018';",

			"SELECT dept, COUNT(*) as numcourses\r\n" + 
			"FROM course\r\n" + 
			"GROUP BY dept\r\n" + 
			"ORDER BY numcourses DESC, dept;",

			"SELECT ccode, credits\r\n" + 
			"FROM course\r\n" + 
			"WHERE dept = 'computer science' AND ccode IN\r\n" + 
			"(\r\n" + 
			"  SELECT ccode\r\n" + 
			"	FROM enroll\r\n" + 
			"	WHERE term = 'winter 2018'\r\n" + 
			"	GROUP BY ccode\r\n" + 
			"	HAVING COUNT(*) >= 5\r\n" + 
			")\r\n" + 
			"ORDER BY ccode;",

			"SELECT dept\r\n" + 
			"FROM\r\n" + 
			"(\r\n" + 
			"  SELECT distinct dept, sid\r\n" + 
			"  FROM course c, enroll e\r\n" + 
			"  WHERE c.ccode = e.ccode AND e.term = 'winter 2018'\r\n" + 
			")s\r\n" + 
			"GROUP BY dept\r\n" + 
			"HAVING COUNT(*) =\r\n" + 
			"(\r\n" + 
			"  SELECT COUNT(distinct sid)\r\n" + 
			"  FROM enroll\r\n" + 
			"  WHERE term = 'winter 2018'\r\n" + 
			")\r\n" + 
			"ORDER BY dept;",

			"SELECT ccode, COUNT(*) as numstudents\r\n" + 
			"FROM enroll\r\n" + 
			"WHERE term = 'winter 2018'\r\n" + 
			"GROUP BY ccode\r\n" + 
			"UNION\r\n" + 
			"SELECT ccode, 0 as numstudents\r\n" + 
			"FROM courseoffer\r\n" + 
			"WHERE term = 'winter 2018' AND ccode NOT IN\r\n" + 
			"( SELECT ccode FROM enroll WHERE term = 'winter 2018' )\r\n" + 
			"ORDER BY ccode;",

			"SELECT DISTINCT c.ccode, COALESCE(cnt, 0) as numstudents\r\n" + 
			"FROM\r\n" + 
			"(\r\n" + 
			"  SELECT ccode, COUNT(*) as cnt\r\n" + 
			"  FROM enroll\r\n" + 
			"  WHERE term = 'winter 2018'\r\n" + 
			"  GROUP BY ccode\r\n" + 
			")e RIGHT OUTER JOIN courseoffer c\r\n" + 
			"  ON e.ccode = c.ccode\r\n" + 
			"WHERE c.term = 'winter 2018'\r\n" + 
			"ORDER BY c.ccode;",

			"SELECT AVG(cnt) AS avgenrollment\r\n" + 
			"FROM\r\n" + 
			"(\r\n" + 
			"  SELECT count(*) cnt\r\n" + 
			"  FROM enroll\r\n" + 
			"  WHERE term = 'winter 2018'\r\n" + 
			"  GROUP BY ccode\r\n" + 
			")t;",

			"SELECT ccode, COUNT(*) as numstudents\r\n" + 
			"FROM enroll\r\n" + 
			"WHERE term = 'winter 2018'\r\n" + 
			"GROUP BY ccode\r\n" + 
			"HAVING COUNT(*) >= ALL\r\n" + 
			"(\r\n" + 
			"  SELECT COUNT(*) cnt\r\n" + 
			"  FROM enroll\r\n" + 
			"  WHERE term = 'winter 2018'\r\n" + 
			"  GROUP BY ccode\r\n" + 
			")\r\n" + 
			"ORDER BY ccode;"			
	};
}