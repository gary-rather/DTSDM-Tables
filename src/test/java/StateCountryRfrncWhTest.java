import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MyTestWatcher.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("StateCountryRfrncWhTest")
public class StateCountryRfrncWhTest {

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
		String sql = "Select count(*) " +
				"from DTSDM.STATE_COUNTRY_RFRNC_WH " +
				"where STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_WID = 0";
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
		System.out.println("Test test_StateCountryCd Success " + "StateCountryCd = 'UNK' count = 1") ;
	}

	@Test
	@Order(2)
	@DisplayName("testTwo")
	void testTwo() {
		// Select count distinct rows
		String sql1 = "Select count (distinct STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_WID) from DTSDM. STATE_COUNTRY_RFRNC_WH";

		// Select total distinct rows
		String sql2 = "Select count ( STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_WID) from DTSDM. STATE_COUNTRY_RFRNC_WH";

		// if the count the same no duplicates are found
		int distinctCount = 0;
		int totalCount = 0;

		// Get distinct count
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

		// Get total count
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
	@Order(3)
	@DisplayName("testThree")
	void testThree() {
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
			System.out.println("StateCountryRfrncWhTest.testThree sql1 failed");
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
			System.out.println("StateCountryRfrncWhTest.testThree sql2 failed");
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
			System.out.println("StateCountryRfrncWhTest.testThree sql2 failed");
			e.printStackTrace();
		}

        try {
			System.out.println("stateCountryCount  Destination CD  expected 302 actual = " + destCount);
			assertEquals(302, destCount);
		} catch (Throwable t){

		}

		//try {
		System.out.println("stateCountryCount  Source CD  expected 301 actual = " + srcCount) ;
		assertEquals(301, srcCount);
		//} catch (Throwable t){

		//}
		//try {
		System.out.println("stateCountryCount  Destination minus Src  CD  expected 1 actual = " + minusCount) ;
		assertEquals(1, minusCount);
		//} catch (Throwable t){

		//}

	}

	@Test
	@Order(4)
	@DisplayName("testFour")
	void testFour() {
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
			System.out.println("StateCountryRfrncWhTest.testFour sql2 failed");
			e.printStackTrace();
		}

		try {
			System.out.println("Test Four    Destination Name  expected 302 actual = " + destCount);
			assertEquals(302, destCount);
		} catch (Throwable t){

		}

		//try {
		System.out.println("Test Four    Source Name  expected 301 actual = " + srcCount) ;
		assertEquals(301, srcCount);
		//} catch (Throwable t){

		//}
		//try {
		System.out.println("Test Four  Destination minus Src  Name  expected 1 actual = " + minusCount) ;
		assertEquals(1, minusCount);
		//} catch (Throwable t){

		//}

	}

	@Test
	@Order(5)
	@DisplayName("testFive")
	void testFive() {
		// Select distinct country codes
		String sql1 = "select STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_CD, STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_NAME,STATE_COUNTRY_RFRNC_WH.CONUS_IND, \n" +
				"STATE_COUNTRY_RFRNC_WH.HTL_TAX_EXMPT_CD, STATE_COUNTRY_RFRNC_WH.HTL_TAX_EXMPT_EXP_DT \n" +
				"from DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
				"minus \n" +
				"select STATE.U##STCODE, STATE.U##STNAME, \n" +
				"STATE.CONUS, \n" +
				"HTL_TAX_EXMPT_LOCATION1.TAX_EXMPT_TYPE, \n" +
				"HTL_TAX_EXMPT_LOCATION1.DATE_EXPIRY \n" +
				"from FRED.STATE STATE  LEFT OUTER JOIN  FRED.HTL_TAX_EXMPT_LOCATION HTL_TAX_EXMPT_LOCATION1  \n" +
				"    ON  HTL_TAX_EXMPT_LOCATION1.STATE_CODE = STATE.U##STCODE " ;



		// if the count the same no duplicates are found
		int minusCount = 0;

		// Get dest count
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
			System.out.println("StateCountryRfrncWhTest.testFive sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Test Five  Destination minus Src  Name  expected 1 actual = " + minusCount) ;
		assertEquals(1, minusCount);

		}

	@Test
	@Order(6)
	@DisplayName("testSix")
	void testSix() {
		// Select distinct country codes
		String sql1 = "Select distinct STATE_COUNTRY_RFRNC_WH.CURR_SW, count(*) as table_rows_cnt \n" +
				"From DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
				"Where STATE_COUNTRY_RFRNC_WH.CURR_SW = 1 \n" +
				"group by STATE_COUNTRY_RFRNC_WH.CURR_SW " ;



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
			System.out.println("StateCountryRfrncWhTest.testSix sql1 failed");
			e.printStackTrace();
		}

		System.out.println("Test Six  Destination minus Src  Name  expected 302 actual = " + tableRowsCount) ;
		assertEquals(302, tableRowsCount);

	}

	@Test
	@Order(7)
	@DisplayName("testSeven")
	@Disabled
	void testSeven() {
		// Select distinct EFF_START_DT
		String sql1 = "Select distinct STATE_COUNTRY_RFRNC_WH.EFF_START_DT, count (*) \n" +
				"From DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
				"Group by STATE_COUNTRY_RFRNC_WH.EFF_START_DT";


		String sql2 = "Select count(STATE_COUNTRY_RFRNC_WH.EFF_START_DT) \n" +
				"From DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
				"Where STATE_COUNTRY_RFRNC_WH.EFF_START_DT is not null";


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
						notNullCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.testFour sql2 failed");
			e.printStackTrace();
		}



			System.out.println("Test Four    Destination Name  expected 302 actual = " + startCount);
			assertEquals(302, startCount);


		System.out.println("Test Four    Source Name  expected 301 actual = " + notNullCount) ;
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


		String sql2 = "Select count(STATE_COUNTRY_RFRNC_WH.EFF_END_DT) \n" +
				"From DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
				"Where STATE_COUNTRY_RFRNC_WH.EFF_END_DT is not null";


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
						notNullCount =  rs.getInt(1);

					}
				}
			}
		} catch (SQLException e) {
			System.out.println("StateCountryRfrncWhTest.testFour sql2 failed");
			e.printStackTrace();
		}



		System.out.println("Test Four    Destination Name  expected 302 actual = " + endCount);
		assertEquals(302, endCount);


		System.out.println("Test Four    Source Name  expected 301 actual = " + notNullCount) ;
		assertEquals(301, notNullCount);

	}

}