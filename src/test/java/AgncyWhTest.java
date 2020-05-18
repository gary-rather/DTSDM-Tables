import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MyTestWatcher.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("StateCountryRfrncWhTest")
public class AgncyWhTest {

	Connection conn = null;
	//String myConnectionURL = "jdbc:oracle:thin:dtsdn/Gizmo900@10.1.10.201:1521:ORCLPDB";
	String myConnectionURL = "jdbc:oracle:thin:@10.1.10.201:1521:ORCLPDB";

	@BeforeAll
	private void getConnection(){
		Connection con = null;
		try {
			Properties props = new Properties();
			//props.put("DB_DRIVER","oracle.jdbc.OracleDriver");
			props.setProperty("user", "dtsdm");
			props.setProperty("password", "cL3ar#12");

			con = DriverManager.getConnection(myConnectionURL,props);
			System.out.println("Connection Successful");
		} catch (Exception e){
			e.printStackTrace();
		}
		this.conn = con;
	}


	@Test
	@Order(1)
	@DisplayName("testOne")
	void testOne() {
		String sql = "Select * \n" +
				"from DTSDM.AGNCY_WH \n" +
				" where AGNCY_WH. AGNCY_WID = 0\n";
		int number = 0;
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
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
		System.out.println("Test AgncyWh  Success " + "wid = '0' count = " + number) ;
	}

	@Test
	@Order(2)
	@DisplayName("testTwo")
	/**
	 * Check to ensure that all values in AGNCY_WID field are unique and taken from the external source file. Pass.
	 * '-- EXPECT to see all unique AGNCY_WH.AGNCY_WID provided by the external file  and the count should not be greater then 1:
	 */
	void testTwo() {
		// Select count distinct rows
		String sql1 = "Select distinct (AGNCY_WH.AGNCY_WID), count (*) \n" +
				"from DTSDM.AGNCY_WH \n" +
				"group by AGNCY_WH.AGNCY_WID \n" +
				"having count(*) > 1 \n";


		// if the count the same no duplicates are found
		int dupeCount = 0;

		// Get distinct count
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						dupeCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.stateCountryCount sql1 failed");
			e.printStackTrace();
		}

		assertEquals(0, dupeCount);
		System.out.println("Agncy_WH   ddupe wid =  " + dupeCount ) ;
	}

	@Test
	@Order(3)
	@DisplayName("testThree")
	/**
	 * Check the value of AGNCY_WH. DPRTMNT_WID. Pass.
	 * DPRTMNT_WH table has only value for the departments and it’s ‘D’.  The DPRTMNT_WH.DPRTMNT_WID field has value ‘1’
	 */
	void testThree() {

		// Select distinct country codes
		String sql1 = "Select distinct AGNCY_WH. DPRTMNT_WID \n" +
				"from DTSDM.AGNCY_WH \n";

		// if the count the same no duplicates are found
		int dptWidCount = 0;
		int dptWid = 0;

		// Get distinct count
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						dptWid =  rs.getInt(1);
						dptWidCount++;
					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.testThree sql1 failed");
			e.printStackTrace();
		}


		System.out.println("AgncyWh  DprtmntWid  count should be 1  actual = " + dptWidCount);
		assertEquals(1, dptWidCount);



		System.out.println("AgncyWh  DprtmntWid  value should be 1  actual = " + dptWidCount);
		assertEquals(1, dptWid);



	}

	@Test
	@Order(4)
	@DisplayName("testFour")
	/**
	 * Check the values of the AGNCY_WH. AGNCY_CD column
	 */
	void testFour() {

		// Select distinct country codes
		String sql1 = "Select count(distinct AGNCY_WH.AGNCY_CD) \n" +
				"from DTSDM.AGNCY_WH \n";


		String sql2 = "Select count(distinct u##agency) \n" +
				"from DTSDM_SRC_STG.adm_agency\n" ;

		String sql3 = "Select distinct AGNCY_WH.AGNCY_CD \n" +
				"from DTSDM.AGNCY_WH \n" +
				"where AGNCY_WH.AGNCY_CD not in \n" +
				"(Select distinct u##agency \n" +
				"  from DTSDM_SRC_STG.adm_agency )";

		// if the count the same no duplicates are found
		int destCount = 0;
		int srcCount = 0;
		int minusCount = 0;

		// Get dest count
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
			System.out.println("StateCountryRfrncWhTest.testFour sql1 failed");
			e.printStackTrace();
		}

		// Get total count
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
			System.out.println("StateCountryRfrncWhTest.testFour sql2 failed");
			e.printStackTrace();
		}

		// Get minus count
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
			System.out.println("AgncyWh.testFour sql2 failed");
			e.printStackTrace();
		}


		System.out.println("AgncyWh Test Four    Destination count actual = " + destCount);
	    assertTrue(destCount >= 0);

		System.out.println("AgncyWh Test Four    Source count actual = " + srcCount);
		assertTrue(srcCount >= 0);

		//try {
		System.out.println("Test Four    Destination count should equal Source Count " + destCount + " = " + srcCount) ;
		assertEquals(destCount, srcCount);
		//} catch (Throwable t){


	}

	@Test
	@Order(5)
	@DisplayName("testFive")
	/**
	 * Check the values of the AGNCY_WH.AGNCY_DESCR column
	 */
	void testFive() {
		// Select distinct country codes
		String sql1 = "Select distinct AGNCY_WH.AGNCY_DESCR \n" +
				"from DTSDM.AGNCY_WH\n" ;


		String sql2 = "select distinct agency_desc \n" +
				"from DTSDM_SRC_STG.adm_agency \n" ;

		String sql3 = "Select distinct AGNCY_WH.AGNCY_DESCR\n" +
				"from DTSDM.AGNCY_WH\n" +
				"where AGNCY_WH.AGNCY_DESCR not in (Select distinct agency_desc\n" +
				"from DTSDM_SRC_STG.adm_agency)\n";

		// if the count the same no duplicates are found
		int destCount = 0;
		int srcCount = 0;
		int minusCount = 0;

		// Get dest count
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
			System.out.println("AgncyWh.testFour sql1 failed");
			e.printStackTrace();
		}

		// Get total count
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
			System.out.println("AgncyWh.testFour sql2 failed");
			e.printStackTrace();
		}

		// Get minus count
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
			System.out.println("AgncyWh.testFour sql2 failed");
			e.printStackTrace();
		}


		System.out.println("AgncyWh Test Five  AGNCY_DESCR  Destination count actual = " + destCount);
		assertTrue(destCount >= 0);

		System.out.println("AgncyWh Test Five  AGNCY_DESCR  Source count actual = " + srcCount);
		assertTrue(srcCount >= 0);


		System.out.println("Test Five  AGNCY_DESCR  Destination count should equal Source Count " + destCount + " = " + srcCount) ;
		assertEquals(destCount, srcCount);
	}

	@Test
	@Order(6)
	@DisplayName("testSix")
	/**
	 * Check the population of the AGNCY_WH.CURR_SW column
	 */
	void testSix() {
		// Select distinct country codes
		String sql1 = "Select distinct AGNCY_WH.CURR_SW, count(*) \n" +
				"From DTSDM.AGNCY_WH \n" +
				"Group by AGNCY_WH.CURR_SW \n " ;



		// if the count the same no duplicates are found
		int tableRowsCount = 0;

		// Get dest count
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
			System.out.println("AgncyWh.testSix sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Test Six  Destination minus Src  Name  expected  actual = " + tableRowsCount) ;
		assertEquals(302, tableRowsCount);

	}

	@Test
	@Order(7)
	@DisplayName("testSeven")
	@Disabled
	/**
	 * Check the population of the AGNCY_WH.EFF_START_DT column
	 */
	void testSeven() {
		// Select distinct EFF_START_DT
		String sql1 = "Select count (*) \n" +
				"From DTSDM.AGNCY_WH\n";


		String sql2 = "Select distinct trunc(AGNCY_WH.EFF_START_DT), count (*) \n" +
				"From DTSDM.AGNCY_WH \n" +
				"Group by trunc(AGNCY_WH.EFF_START_DT) \n";


		// if the count the same
		int startCount = 0;
		int notNullCount = 0;


		// Get EFF_START_DT
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						startCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.testSeven sql1 failed");
			e.printStackTrace();
		}

		// Get total count
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						notNullCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.testSeven sql2 failed");
			e.printStackTrace();
		}



			System.out.println("Test Seven    Destination Name  expected 302 actual = " + startCount);
			assertEquals(302, startCount);


		System.out.println("Test Seven    Source Name  expected 301 actual = " + notNullCount) ;
		assertEquals(301, notNullCount);

	}

	@Test
	@Order(8)
	@DisplayName("testEight")
	@Disabled
	void testEight() {
		// Select distinct EFF_START_DT
		String sql1 = "Select distinct STATE_COUNTRY_RFRNC_WH.EFF_END_DT, count (*) \n" +
				"From DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
				"Group by STATE_COUNTRY_RFRNC_WH.EFF_END_DT";


		String sql2 = "Select distinct AGNCY_WH.EFF_END_DT, count (*) \n" +
				"From DTSDM.AGNCY_WH \n" +
				"Group by AGNCY_WH.EFF_END_DT \n";


		// if the count the same
		int endCount = 0;
		int notNullCount = 0;


		// Get EFF_START_DT
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						endCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.testEight sql1 failed");
			e.printStackTrace();
		}

		// Get total count
		try {
			try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
				//ps.setInt(1, userId);
				try (ResultSet rs = ps.executeQuery();) {
					//System.out.println("Size of results = " + rs.getInt(1));
					while(rs.next()) {
						notNullCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("AgncyWh.testEight sql2 failed");
			e.printStackTrace();
		}



		System.out.println("Test Eight    Destination Name  expected 302 actual = " + endCount);
		assertEquals(302, endCount);


		System.out.println("Test Eight    Source Name  expected 301 actual = " + notNullCount) ;
		assertEquals(301, notNullCount);

	}

}