import sparksql.Covid19Sql;

public class CovidSparkSQL {

	public static void main(String[] args) {
		Covid19Sql csql = Covid19Sql.getInstance();
		
		String countySum = "SELECT county, SUM(cases) FROM " + Covid19Sql.TABLE_NAME + " GROUP BY county";
		csql.query(countySum);

	}

}
