
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StateCountryRfrncWhTest extends TableTest {

	@BeforeClass
	public  static void openResults(){
		wr = new WriteResults("StateCountryRfrncWhTest.html");
		wr.pageHeader();
	}

	@Test
	public void test1() {
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

		String sql1 = "Select count(*) " +
				"from DTSDM.STATE_COUNTRY_RFRNC_WH " +
				"where STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_WID = 0";

		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj1);
		wr.logSql(theSql);


		int number = 0;
		System.out.println("Starting StateCountryRfrncWhTest.test1,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						number =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//try {
			assertEquals(1, number);
		//} catch (Throwable t){
		//	System.out.println("Assertion rows returned failed ");
		//	t.printStackTrace();
        //    throw t;
		//}
		System.out.println("Test test_StateCountryCd Success " + "StateCountryCd = 'UNK' count = 1") ;
	}

	@Test
	public void test2() {
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

		// Select count distinct rows
		String sql1 = "Select count (distinct STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_WID) from DTSDM. STATE_COUNTRY_RFRNC_WH";

		// Select total distinct rows
		String sql2 = "Select count ( STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_WID) from DTSDM. STATE_COUNTRY_RFRNC_WH";

		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj1);
		SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj2);
		wr.logSql(theSql);


		// if the count the same no duplicates are found
		int distinctCount = 0;
		int totalCount = 0;

		System.out.println("Starting StateCountryRfrncWhTest.test2,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						distinctCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.stateCountryCount sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Starting StateCountryRfrncWhTest.test2,sql2");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						totalCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.stateCountryCount sql2 failed");
			e.printStackTrace();
		}

		try {
			assertEquals(totalCount, distinctCount);
		} catch (Throwable t){
			System.out.println("Assertion rows returned failed ");
			t.printStackTrace();
		}
		System.out.println("stateCountryCount  DistinctCount = TotalCount " + distinctCount + " = " + totalCount) ;
	}

	@Test
	public void test3() {
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

		// Select distinct country codes
		String sql1 = "Select count(distinct STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_CD ) \n" +
				      "from DTSDM. STATE_COUNTRY_RFRNC_WH \n";


		String sql2 = "select count(distinct U##STCODE) \n" +
				"from DTSDM_SRC_STG.STATE";

		String sql3 = "select distinct STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_CD \n" +
				"from DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
				"minus \n" +
				"select distinct U##STCODE \n" +
				"from DTSDM_SRC_STG.STATE  \n";

		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj1);
		SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj2);
		SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj3);
		wr.logSql(theSql);


		// if the count the same no duplicates are found
		int destCount = 0;
		int srcCount = 0;
		int minusCount = 0;

		System.out.println("Starting StateCountryRfrncWhTest.test3,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						destCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.test3 sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Starting StateCountryRfrncWhTest.test3,sql2");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						srcCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.test3 sql2 failed");
			e.printStackTrace();
		}

		System.out.println("Starting StateCountryRfrncWhTest.test3,sql3");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));

					while(rs.next()) {
						String row =  rs.getString(1);
						minusCount++;

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.test3 sql2 failed");
			e.printStackTrace();
		}


			System.out.println("stateCountryCount  Destination CD  expected 302 actual = " + destCount);
			assertEquals(302, destCount);



		System.out.println("stateCountryCount  Source CD  expected 301 actual = " + srcCount) ;
		assertEquals(301, srcCount);


		System.out.println("stateCountryCount  Destination minus Src  CD  expected 1 actual = " + minusCount) ;
		assertEquals(1, minusCount);



	}

	@Test
	public void test4() {
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


		// Select distinct country codes
		String sql1 = "Select count(distinct STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_NAME  ) \n" +
				"from DTSDM. STATE_COUNTRY_RFRNC_WH \n";


		String sql2 = "select count(distinct U##STNAME) \n" +
				"from DTSDM_SRC_STG.STATE\n";

		String sql3 = "select distinct STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_NAME \n" +
				"from DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
				"minus \n" +
				"select distinct STATE.U##STNAME \n" +
				"from DTSDM_SRC_STG.STATE  \n";

		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj1);
		SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj2);
		SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj3);
		wr.logSql(theSql);


		// if the count the same no duplicates are found
		int destCount = 0;
		int srcCount = 0;
		int minusCount = 0;

		System.out.println("Starting StateCountryRfrncWhTest.test4,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						destCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.test4 sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Starting StateCountryRfrncWhTest.test4,sql2");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						srcCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.test4 sql2 failed");
			e.printStackTrace();
		}

		System.out.println("Starting StateCountryRfrncWhTest.test4,sql3");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						String row =  rs.getString(1);
						minusCount++;

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.test4 sql2 failed");
			e.printStackTrace();
		}


			System.out.println("Test Four    Destination Name  expected 302 actual = " + destCount);
			assertEquals(302, destCount);


		System.out.println("Test Four    Source Name  expected 301 actual = " + srcCount) ;
		assertEquals(301, srcCount);


		System.out.println("Test Four  Destination minus Src  Name  expected 1 actual = " + minusCount) ;
		assertEquals(1, minusCount);


	}

	@Test
	public void test5() {
    	// Log the Class and method
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


		// Select distinct country codes
		String sql1 = "select STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_CD, STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_NAME,STATE_COUNTRY_RFRNC_WH.CONUS_IND, \n" +
				"STATE_COUNTRY_RFRNC_WH.HTL_TAX_EXMPT_CD, STATE_COUNTRY_RFRNC_WH.HTL_TAX_EXMPT_EXP_DT \n" +
				"from DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
				"minus \n" +
				"select STATE.U##STCODE, STATE.U##STNAME, \n" +
				"STATE.CONUS, \n" +
				"HTL_TAX_EXMPT_LOCATION1.TAX_EXMPT_TYPE, \n" +
				"HTL_TAX_EXMPT_LOCATION1.DATE_EXPIRY \n" +
				"from DTSDM_SRC_STG.STATE STATE  LEFT OUTER JOIN  DTSDM_SRC_STG.HTL_TAX_EXMPT_LOCATION HTL_TAX_EXMPT_LOCATION1  \n" +
				"    ON  HTL_TAX_EXMPT_LOCATION1.STATE_CODE = STATE.U##STCODE " ;


		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj1);
		wr.logSql(theSql);



		// if the count the same no duplicates are found
		int minusCount = 0;

		System.out.println("Starting StateCountryRfrncWhTest.test5,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						String stateCountryCd =  rs.getString("STATE_COUNTRY_CD");
						String stateCountryName =  rs.getString("STATE_COUNTRY_NAME");
						String conusInd =  rs.getString("CONUS_IND");
						String htlTaxExemptCd =  rs.getString("HTL_TAX_EXMPT_CD");
						Date htlTaxExemptdt =  rs.getDate("HTL_TAX_EXMPT_EXP_DT");
                        minusCount++;
					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.test5 sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Test Five  Destination minus Src  Name  expected 1 actual = " + minusCount) ;
		assertEquals(1, minusCount);

		}

	@Test
	public void test6() {
		// Log the Class and method
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		
		// Select distinct country codes
		String sql1 = "Select distinct STATE_COUNTRY_RFRNC_WH.CURR_SW, count(*) as table_rows_cnt \n" +
				"From DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
				"Where STATE_COUNTRY_RFRNC_WH.CURR_SW = 1 \n" +
				"group by STATE_COUNTRY_RFRNC_WH.CURR_SW " ;


		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj1);
		wr.logSql(theSql);



		// if the count the same no duplicates are found
		int tableRowsCount = 0;

		System.out.println("Starting StateCountryRfrncWhTest.test6,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						int currSw =  rs.getInt("CURR_SW");
						tableRowsCount =  rs.getInt("table_rows_cnt");

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.test6 sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Test Six  Destination minus Src  Name  expected 302 actual = " + tableRowsCount) ;
		assertEquals(302, tableRowsCount);

	}

	@Test
	/**
	 * Check the population of the AGNCY_WH.EFF_START_DT column
	 */
	public void test7() {
		// Log the Class and method
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

		// Select distinct EFF_START_DT
		String sql1 = "Select count (*) \n" +
				"From DTSDM.AGNCY_WH\n";


		String sql2 = "Select distinct trunc(AGNCY_WH.INSERT_DATE) INSERT_DATE, count (*) \n" +
				"From DTSDM.AGNCY_WH \n" +
				"Group by trunc(AGNCY_WH.INSERT_DATE) \n";


		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj1);
		SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj2);
		wr.logSql(theSql);


		// if the count the same
		int count = 0;
		String aDate = null;
		int runningCount = 0;


		System.out.println("Starting StateCountryRfrncWhTest.test7,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						count = rs.getInt("count(*)");
						

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test7 sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Starting StateCountryRfrncWhTest.test7,sql2");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						aDate = rs.getString("INSERT_DATE");
						runningCount = runningCount + rs.getInt("count(*)");

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test7 sql2 failed");
			e.printStackTrace();
		}



			System.out.println("Test Seven    distinctCount = " + count );


			System.out.println("Test Seven    runningCount = " + runningCount );
		assertEquals(count, runningCount);

	}

	@Test
	/**
	 * Check the population of the AGNCY_WH.UPDATE_DATE column
	 */
	public void test8() {
		// Log the Class and method
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

		// Select distinct EFF_START_DT
		String sql1 = "Select count (*) \n" +
				"From DTSDM.AGNCY_WH\n";


		String sql2 = "Select distinct trunc(AGNCY_WH.UPDATE_DATE) UPDATE_DATE, count (*) \n" +
				"From DTSDM.AGNCY_WH \n" +
				"Group by trunc(AGNCY_WH.UPDATE_DATE) \n";


		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj1);
		SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj2);
		wr.logSql(theSql);


		// if the count the same
		int count = 0;
		String aDate = null;
		int runningCount = 0;


		System.out.println("Starting StateCountryRfrncWhTest.test8,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						count = rs.getInt("count(*)");
						

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test8 sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Starting StateCountryRfrncWhTest.test8,sql2");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						aDate = rs.getString("UPDATE_DATE");
						runningCount = runningCount + rs.getInt("count(*)");

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test8 sql2 failed");
			e.printStackTrace();
		}



			System.out.println("Test Eight    distinctCount = " + count );


			System.out.println("Test Eight    runningCount = " + runningCount );
		assertEquals(count, runningCount);

	}

}