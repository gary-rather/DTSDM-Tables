import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AgncyWhTest extends TableTest {

	@BeforeClass
	public  static void openResults(){
		wr = new WriteResults("AgncyWhTest.html");
		wr.pageHeader();
	}


	@Test
	public void test1() {
		// Log the Class and method
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


		String sql = "Select count(*) \n" + "from DTSDM.AGNCY_WH \n" + " where AGNCY_WH. AGNCY_WID = 0 \n";

		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj);
		wr.logSql(theSql);

		int number = 0;

		System.out.println("Starting AgncyWhTest.test1,sql");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						number = rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		// Log the results before
		ArrayList<ResultObject> roList = new ArrayList<>();
		ResultObject ro1 = new ResultObject((1 == number),"(1 == number)");
		roList.add(ro1);
		wr.logTestResults(roList);

		System.out.println("Test AgncyWh  Row 0 1= " + number);
		assertEquals(1, number);

		System.out.println("Finish AgncyWhTest.test1");
		System.out.println();
	}

	@Test
	/**
	 * Check to ensure that all values in AGNCY_WID field are unique and taken from
	 * the external source file. Pass. '-- EXPECT to see all unique
	 * AGNCY_WH.AGNCY_WID provided by the external file and the count should not be
	 * greater then 1:
	 */
	public void test2() {
		// Log the Class and method
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


		// Select count distinct rows
		String sql1 = "select count(*) from " +
				"(" +
				"   Select distinct (AGNCY_WH.AGNCY_WID), count (*) \n" + "from DTSDM.AGNCY_WH \n"
				+ "group by AGNCY_WH.AGNCY_WID \n" + "having count(*) > 1 " +
				")\n";

		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj);
		wr.logSql(theSql);

		// if the count the same no duplicates are found
		int dupeCount = 0;

		System.out.println("Starting AgncyWhTest.test2,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						dupeCount = rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.stateCountryCount sql1 failed");
			e.printStackTrace();
		}

		// Log the results before
		ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
		ResultObject ro1 = new ResultObject((0 == dupeCount),"(0 == dupeCount)");
		roList.add(ro1);
		wr.logTestResults(roList);

		System.out.println("Agncy_WH   dupe wid =  " + dupeCount);
		assertEquals(0, dupeCount);

		System.out.println("Finish " +  this.getClass().getSimpleName() + ".test01");
		System.out.println();
	}

	@Test
	/**
	 * Check the value of AGNCY_WH. DPRTMNT_WID. Pass. DPRTMNT_WH table has only
	 * value for the departments and it’s ‘D’. The DPRTMNT_WH.DPRTMNT_WID field has
	 * value ‘1’
	 */
	public void test3() {
		// Log the Class and method
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


		// Select distinct country codes
		// All dprtmnt_id should be 1 expect the row 0;
		String sql1 = "Select distinct AGNCY_WH. DPRTMNT_WID from DTSDM.AGNCY_WH where AGNCY_WID != 0 ";

		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj);
		wr.logSql(theSql);

		// if the count the same no duplicates are found
		int dptWidCount = 0;
		int dptWid = 0;

		System.out.println("Starting AgncyWhTest.test3,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						dptWid = rs.getInt(1);
						dptWidCount++;
					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test3 sql1 failed");
			e.printStackTrace();
		}

		// Log the results before
		ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
		ResultObject ro1 = new ResultObject((1 == dptWidCount),"(1 == dptWidCount)");
		roList.add(ro1);
		ResultObject ro2 = new ResultObject((1 == dptWid),"(1 == dptWid)");
		roList.add(ro2);
		wr.logTestResults(roList);

		System.out.println("AgncyWh  DprtmntWid  count should be 1  actual = " + dptWidCount);
		assertEquals(1, dptWidCount);
		
		System.out.println("AgncyWh  DprtmntWid  should = " + dptWid);
		assertEquals(1, dptWid);



	}

	@Test
	/**
	 * Check the values of the AGNCY_WH. AGNCY_CD column
	 */
	public void test4() {
		// Log the Class and method
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


		// Select distinct country codes
		String sql1 = "Select count(distinct AGNCY_WH.AGNCY_CD) from DTSDM.AGNCY_WH ";

		String sql2 = "Select count(distinct u##agency) \n" + "from DTSDM_SRC_STG.adm_agency\n";

		String sql3 = "Select distinct AGNCY_WH.AGNCY_CD \n" + "from DTSDM.AGNCY_WH \n"
				+ "where AGNCY_WH.AGNCY_CD not in \n" + "(Select distinct u##agency \n"
				+ "  from DTSDM_SRC_STG.adm_agency )";

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
		String row = null;
		
		System.out.println("Starting AgncyWhTest.test4,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						destCount = rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.test4 sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Starting AgncyWhTest.test4,sql2");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						srcCount = rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.test4 sql2 failed");
			e.printStackTrace();
		}

		System.out.println("Starting AgncyWhTest.test4,sql3");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						row = rs.getString(1);
						minusCount++;

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test4 sql2 failed");
			e.printStackTrace();
		}


		// Log the results before
		ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
		ResultObject ro1 = new ResultObject((destCount > 0),"(destCount == 0)");
		roList.add(ro1);
		ResultObject ro2 = new ResultObject((srcCount > 0),"(srcCount > 0)");
		roList.add(ro2);
		ResultObject ro3 = new ResultObject((destCount == (srcCount + 1)),"(destCount == (srcCount + 1))");
		roList.add(ro3);
		ResultObject ro4 = new ResultObject((1 == minusCount),"(1 == minusCount)");
		roList.add(ro4);
		ResultObject ro5 = new ResultObject(("UNK".equalsIgnoreCase(row)),"('UNK' == row)");
		roList.add(ro5);

		wr.logTestResults(roList);


		System.out.println("AgncyWh Test Four    Destination count actual = " + destCount);
		assertTrue(destCount >= 0);

		System.out.println("AgncyWh Test Four    Source count actual = " + srcCount);
		assertTrue(srcCount >= 0);

		//include row 0 so dest count +1 over src
		System.out.println("Test Four    Destination count should equal Source Count " + destCount + " = " + srcCount + "row0 = 1" );
		assertEquals(destCount, (srcCount + 1));
		
		System.out.println("Test Four    rows not in Src" + minusCount );
		assertEquals(1, minusCount);

		System.out.println("Test Four   AgncyCd  not in Src" + row );
		assertEquals("UNK", row);

	}

	@Test
	/**
	 * Check the values of the AGNCY_WH.AGNCY_DESCR column
	 */
	public void test5() {
		// Log the Class and method
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


		// Select distinct country codes
		String sql1 = "Select distinct AGNCY_WH.AGNCY_DESCR \n" + "from DTSDM.AGNCY_WH\n";

		String sql2 = "select distinct agency_desc \n" + "from DTSDM_SRC_STG.adm_agency \n";

		String sql3 = "Select distinct AGNCY_WH.AGNCY_DESCR\n" + "from DTSDM.AGNCY_WH\n"
				+ "where AGNCY_WH.AGNCY_DESCR not in (Select distinct agency_desc\n"
				+ "from DTSDM_SRC_STG.adm_agency)\n";

		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj1);
		SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj2);
		SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj3);
		wr.logSql(theSql);


		int agncyDescrCount = 0;

		int srcAgncyDescrCount = 0;

		int minusCount = 0;
		String row = null;

		System.out.println("Starting AgncyWhTest.test5,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						agncyDescrCount++;
					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test5 sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Starting AgncyWhTest.test5,sql2");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						srcAgncyDescrCount++;

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test4 sql2 failed");
			e.printStackTrace();
		}

		System.out.println("Starting AgncyWhTest.test5,sql3");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						row = rs.getString(1);
						minusCount++;

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test4 sql2 failed");
			e.printStackTrace();
		}

		// Log the results before
		ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
		ResultObject ro1 = new ResultObject((agncyDescrCount > 0),"(agncyDescrCount > 0)");
		roList.add(ro1);
		ResultObject ro2 = new ResultObject((srcAgncyDescrCount > 0),"(srcAgncyDescrCount > 0)");
		roList.add(ro2);
		ResultObject ro3 = new ResultObject((agncyDescrCount == srcAgncyDescrCount),"(agncyDescrCount == srcAgncyDescrCount)");
		roList.add(ro3);
		ResultObject ro4 = new ResultObject((1 == minusCount),"(1 == minusCount)");
		roList.add(ro4);
		ResultObject ro5 = new ResultObject(("UNK".equalsIgnoreCase(row)),"('UNK' == row)");
		roList.add(ro5);
		wr.logTestResults(roList);

		System.out.println("AgncyWh Test Five  AGNCY_DESCR  Destination count actual = " + agncyDescrCount);
		assertTrue(agncyDescrCount >= 0);

		System.out.println("AgncyWh Test Five  AGNCY_DESCR  Source count actual = " + srcAgncyDescrCount);
		assertTrue(srcAgncyDescrCount >= 0);

		System.out.println("Test Five  AGNCY_DESCR  Destination count should be equal Source Count " + agncyDescrCount
				+ " = " + srcAgncyDescrCount);
		assertEquals(agncyDescrCount, srcAgncyDescrCount );

		System.out.println("Test Five  NOT IN  Destination count should equal 1 'UNK' " + minusCount);
		assertEquals(0, minusCount);

	}

	@Test
	/**
	 * Check the population of the AGNCY_WH.CURR_SW column
	 */
	public void test6() {
		// Log the Class and method
		System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
		wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


		// Select distinct country codes
		String sql1 = "Select distinct AGNCY_WH.CURR_SW, count(*) \n" + "From DTSDM.AGNCY_WH \n"
				+ "Group by AGNCY_WH.CURR_SW \n ";


		// log the Sql
		ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
		SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
		theSql.add(sqlObj1);
		wr.logSql(theSql);


		// if the count the same no duplicates are found
		int tableRowsCount = 0;
		int groupByCount = 0;
		String currSw = null;
		System.out.println("Starting AgncyWhTest.test6,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						currSw = rs.getString("CURR_SW");
						groupByCount = groupByCount + rs.getInt("count(*)");
						tableRowsCount++;
					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test6 sql1 failed");
			e.printStackTrace();
		}

		// Log the results before
		ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
		ResultObject ro1 = new ResultObject((1 == tableRowsCount),"(1 == tableRowsCount)");
		roList.add(ro1);
		ResultObject ro2 = new ResultObject(("1".equalsIgnoreCase(currSw)),"(1 == currSw)");
		roList.add(ro2);
		wr.logTestResults(roList);


		System.out.println("Test Six  All CURR_SW value = 1 = " + tableRowsCount);
		assertEquals(1, tableRowsCount);
		
		System.out.println("Test Six  All CURR_SW value = 1  = " + currSw);
		assertEquals("1", currSw);

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
		String sql1 = "Select count (*) \n" + "From DTSDM.AGNCY_WH\n";

		String sql2 = "Select distinct trunc(AGNCY_WH.INSERT_DATE) INSERT_DATE, count (*) \n" + "From DTSDM.AGNCY_WH \n"
				+ "Group by trunc(AGNCY_WH.INSERT_DATE) \n";

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

		System.out.println("Starting AgncyWhTest.test7,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						count = rs.getInt("count(*)");

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test7 sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Starting AgncyWhTest.test7,sql2");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						aDate = rs.getString("INSERT_DATE");
						runningCount = runningCount + rs.getInt("count(*)");

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test7 sql2 failed");
			e.printStackTrace();
		}

		// Log the results before
		ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
		ResultObject ro1 = new ResultObject((count == runningCount),"(count == runningCount)");
		roList.add(ro1);
		wr.logTestResults(roList);


		System.out.println("Test Seven    runningCount = " + runningCount);
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
		String sql1 = "Select count (*) \n" + "From DTSDM.AGNCY_WH\n";

		String sql2 = "Select distinct trunc(AGNCY_WH.UPDATE_DATE) UPDATE_DATE, count (*) \n" + "From DTSDM.AGNCY_WH \n"
				+ "Group by trunc(AGNCY_WH.UPDATE_DATE) \n";

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

		System.out.println("Starting AgncyWhTest.test8,sql1");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						count = rs.getInt("count(*)");

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test8 sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Starting AgncyWhTest.test8,sql2");
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				// ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					// System.out.println("Size of results = " + rs.getInt(1));
					while (rs.next()) {
						aDate = rs.getString("UPDATE_DATE");
						runningCount = runningCount + rs.getInt("count(*)");

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.test8 sql2 failed");
			e.printStackTrace();
		}

		// Log the results before
		ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
		ResultObject ro1 = new ResultObject((count == runningCount),"(count == runningCount)");
		roList.add(ro1);
		wr.logTestResults(roList);


		System.out.println("Test Eight    runningCount = " + runningCount);
		assertEquals(count, runningCount);

	}

}