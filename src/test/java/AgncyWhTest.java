import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AgncyWhTest {

	Connection conn = null;

	@Before
	public void getConnection() {
		Connection con = null;
		try {
			Conf config = new Conf();

			Properties props = new Properties();
			props.put("myConnectionURL", config.getMyConnectionURL());
			props.put("user", config.getUser());
			props.put("password", config.getPassword());
			// System.out.println("myConnectionURL " +
			// props.getProperty("myConnectionURL"));
			// System.out.println("user " + props.getProperty("user"));
			// System.out.println("password" + props.getProperty("password"));

			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(props.getProperty("myConnectionURL"), props);
			System.out.println("Connection Successful");
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.conn = con;
	}

	@Test
	public void test1() {
		System.out.println("Starting AgncyWhTest.test1");
		String sql = "Select count(*) \n" + "from DTSDM.AGNCY_WH \n" + " where AGNCY_WH. AGNCY_WID = 0 \n";
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
		System.out.println("Starting AgncyWhTest.test2");
		// Select count distinct rows
		String sql1 = "Select distinct (AGNCY_WH.AGNCY_WID), count (*) \n" + "from DTSDM.AGNCY_WH \n"
				+ "group by AGNCY_WH.AGNCY_WID \n" + "having count(*) > 1 \n";

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

		assertEquals(0, dupeCount);
		System.out.println("Agncy_WH   ddupe wid =  " + dupeCount);
	}

	@Test
	/**
	 * Check the value of AGNCY_WH. DPRTMNT_WID. Pass. DPRTMNT_WH table has only
	 * value for the departments and it’s ‘D’. The DPRTMNT_WH.DPRTMNT_WID field has
	 * value ‘1’
	 */
	public void test3() {
		System.out.println("Starting AgncyWhTest.test3");
		// Select distinct country codes
		// All dprtmnt_id should be 1 expect the row 0;
		String sql1 = "Select distinct AGNCY_WH. DPRTMNT_WID from DTSDM.AGNCY_WH where AGNCY_WID != 0 ";

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
		System.out.println("Starting AgncyWhTest.test4");
		// Select distinct country codes
		String sql1 = "Select count(distinct AGNCY_WH.AGNCY_CD) from DTSDM.AGNCY_WH ";

		String sql2 = "Select count(distinct u##agency) \n" + "from DTSDM_SRC_STG.adm_agency\n";

		String sql3 = "Select distinct AGNCY_WH.AGNCY_CD \n" + "from DTSDM.AGNCY_WH \n"
				+ "where AGNCY_WH.AGNCY_CD not in \n" + "(Select distinct u##agency \n"
				+ "  from DTSDM_SRC_STG.adm_agency )";

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

		System.out.println("AgncyWh Test Four    Destination count actual = " + destCount);
		assertTrue(destCount >= 0);

		System.out.println("AgncyWh Test Four    Source count actual = " + srcCount);
		assertTrue(srcCount >= 0);

		//include row 0 so dest count +1 over src
		System.out.println("Test Four    Destination count should equal Source Count " + destCount + " = " + srcCount + "row0 = 1" );
		assertEquals(destCount, srcCount + 1);
		
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
		System.out.println("Starting AgncyWhTest.test5");
		// Select distinct country codes
		String sql1 = "Select distinct AGNCY_WH.AGNCY_DESCR \n" + "from DTSDM.AGNCY_WH\n";

		String sql2 = "select distinct agency_desc \n" + "from DTSDM_SRC_STG.adm_agency \n";

		String sql3 = "Select distinct AGNCY_WH.AGNCY_DESCR\n" + "from DTSDM.AGNCY_WH\n"
				+ "where AGNCY_WH.AGNCY_DESCR not in (Select distinct agency_desc\n"
				+ "from DTSDM_SRC_STG.adm_agency)\n";

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
		System.out.println("Starting AgncyWhTest.test6");
		// Select distinct country codes
		String sql1 = "Select distinct AGNCY_WH.CURR_SW, count(*) \n" + "From DTSDM.AGNCY_WH \n"
				+ "Group by AGNCY_WH.CURR_SW \n ";

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
		System.out.println("Starting AgncyWhTest.test7");
		// Select distinct EFF_START_DT
		String sql1 = "Select count (*) \n" + "From DTSDM.AGNCY_WH\n";

		String sql2 = "Select distinct trunc(AGNCY_WH.INSERT_DATE) INSERT_DATE, count (*) \n" + "From DTSDM.AGNCY_WH \n"
				+ "Group by trunc(AGNCY_WH.INSERT_DATE) \n";

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

		System.out.println("Test Seven    distinctCount = " + count);

		System.out.println("Test Seven    runningCount = " + runningCount);
		assertEquals(count, runningCount);

	}

	@Test
	/**
	 * Check the population of the AGNCY_WH.UPDATE_DATE column
	 */
	public void test8() {
		System.out.println("Starting AgncyWhTest.test8");
		// Select distinct EFF_START_DT
		String sql1 = "Select count (*) \n" + "From DTSDM.AGNCY_WH\n";

		String sql2 = "Select distinct trunc(AGNCY_WH.UPDATE_DATE) UPDATE_DATE, count (*) \n" + "From DTSDM.AGNCY_WH \n"
				+ "Group by trunc(AGNCY_WH.UPDATE_DATE) \n";

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

		System.out.println("Test Eight    distinctCount = " + count);

		System.out.println("Test Eight    runningCount = " + runningCount);
		assertEquals(count, runningCount);

	}

}