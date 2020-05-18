import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MyTestWatcher.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("TypeConsoldtdRfrncWhTest")
public class TypeConsoldtdRfrncWhTest {

    Connection conn = null;
    //String myConnectionURL = "jdbc:oracle:thin:dtsdn/Gizmo900@10.1.10.201:1521:ORCLPDB";
    String myConnectionURL = "jdbc:oracle:thin:@10.1.10.201:1521:ORCLPDB";

    @BeforeAll
    private void getConnection() {
        Connection con = null;
        try {
            Properties props = new Properties();
            //props.put("DB_DRIVER","oracle.jdbc.OracleDriver");
            props.setProperty("user", "dtsdm");
            props.setProperty("password", "cL3ar#12");

            con = DriverManager.getConnection(myConnectionURL, props);
            System.out.println("Connection Successful");
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.conn = con;
    }


    @Test
    @Order(1)
    @DisplayName("testOne")
    /**
     * Check that the "unknown" record 0 is populated. Pass
     * -- EXPECT –
     * TYPE_WID = 0; TYPE_NATRL_KEY = 'UNKNOWN', TYPE_CD = 'UNK'; TYPE_DESCR = 'UNKNOWN'; RCD_TYPE_CD =  'UNK';  RCD_TYPE_DESCR = 'UNKNOWN'; others NULL
     */
    void testOne() {
        String sql = "Select * from DTSDM. TYPE_CONSOLDTD_RFRNC_WH \n" +
                "where TYPE_CONSOLDTD_RFRNC_WH.TYPE_WID = 0\n";
        int number = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        number = rs.getInt(1);

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
        System.out.println("Test TypeConsoldtdRfrncWhTest Success " + "Row 0  count = 1");
    }

    @Test
    @Order(2)
    @DisplayName("TypeConsoldtdRfrncWhTest testTwo")
    /**
     * Check the population of the TYPE_CONSOLDTD_RFRNC_WH.TYPE_WID (PK) column
     */
    void testTwo() {
        // Select count distinct rows
        String sql1 = "Select count (distinct TYPE_CONSOLDTD_RFRNC_WH.TYPE_WID) \n" +
                "from DTSDM.TYPE_CONSOLDTD_RFRNC_WH\n";

        // Select total distinct rows
        String sql2 = "Select count(*)\n" +
                "from DTSDM.TYPE_CONSOLDTD_RFRNC_WH\n";

        String sql3 = "select distinct TYPE_CONSOLDTD_RFRNC_WH.TYPE_WID, count(*) \n" +
                "from DTSDM.TYPE_CONSOLDTD_RFRNC_WH \n" +
                "group by TYPE_CONSOLDTD_RFRNC_WH.TYPE_WID \n" +
                "having count(*) > 1 \n";

        // if the count the same no duplicates are found
        int distinctCount = 0;
        int totalCount = 0;
        int dupeCount = 0;

        // Get distinct count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TypeConsoldtdRfrncWhTest.status sql1 failed");
            e.printStackTrace();
        }

        // Get total count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        totalCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TypeConsoldtdRfrncWhTest. sql2 failed");
            e.printStackTrace();
        }

        // Get dupe count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));

                    while (rs.next()) {
                        String row = rs.getString(1);
                        dupeCount++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TypeConsoldtdRfrncWhTest.testThree sql2 failed");
            e.printStackTrace();
        }

        System.out.println("TypeConsoldtdRfrncWhTest  distinct / total  actual = " + distinctCount + " = " + totalCount);
        assertEquals(distinctCount, totalCount);


        System.out.println("TypeConsoldtdRfrncWhTest  duplicate actual = " + dupeCount);
        assertEquals(0, dupeCount);

    }

    @Test
    @Order(3)
    @DisplayName("TypeConsoldtdRfrncWhTest testThree")
    /**
     * Check the population of TYPE_CONSOLDTD_RFRNC_WH for DCMNT type
     */
    void testThree() {
        // Select distinct country codes
        String sql1 = "SELECT DISTINCT DOCSTAT.U##DOCTYPE AS TYPE_CD \n" +
                "FROM DTSDM_SRC_STG.DOCSTAT DOCSTAT \n";


        String sql2 = "SELECT DISTINCT DOCSTAT.U##DOCTYPE AS TYPE_CD , \n" +
                "case when DOCSTAT.U##DOCTYPE = 'AUTH' then 'Authorization' \n" +
                "when DOCSTAT.U##DOCTYPE = 'GAUTH' then 'Group Authorization' \n" +
                "when DOCSTAT.U##DOCTYPE = 'VCH' then 'Voucher' \n" +
                "when DOCSTAT.U##DOCTYPE = 'LVCH' then 'Local Voucher' \n" +
                "else null \n" +
                "end AS TYPE_DESCR , \n" +
                "case when DOCSTAT.U##DOCTYPE in ('AUTH', 'GAUTH', 'SAUTH') then 'AUTH' \n" +
                "when DOCSTAT.U##DOCTYPE in ('VCH', 'LVCH') then 'VCHR' \n" +
                "end AS ALT_TYPE_CD , \n" +
                "'DCMNT' AS RCD_TYPE_CD , \n" +
                "'Document' AS RCD_TYPE_DESCR , \n" +
                "DOCSTAT.U##DOCTYPE || 'DCMNT' AS TYPE_NATRL_KEY \n" +
                "FROM DTSDM_SRC_STG.DOCSTAT DOCSTAT";

        String sql3 = "select *  \n" +
                "from TYPE_CONSOLDTD_RFRNC_WH \n" +
                "where RCD_TYPE_CD = 'DCMNT'  \n";

        // if the count the same no duplicates are found
        ArrayList<String> srcDocstatList = new ArrayList<String>();
        ArrayList<String> destDocstatList = new ArrayList<String>();
        ArrayList<String> docList = new ArrayList<String>();
        int destCount = 0;
        int srcCount = 0;
        int docCount = 0;

        // Get dest count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String srcDocstat = rs.getString("TYPE_CD");
                        srcDocstatList.add(srcDocstat);
                        srcCount++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TypeConsoldtdRfrncWhTest.testThree sql1 failed");
            e.printStackTrace();
        }

        // Get total count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String destDocstat = rs.getString("TYPE_CD");
                        destDocstatList.add(destDocstat);
                        destCount++;


                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TypeConsoldtdRfrncWhTest.testThree sql2 failed");
            e.printStackTrace();
        }

        // Get dupe count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));

                    while (rs.next()) {
                        String doc = rs.getString("TYPE_CD");
                        docList.add(doc);
                        docCount++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TypeConsoldtdRfrncWhTest.testThree sql2 failed");
            e.printStackTrace();
        }

        System.out.println("stateCountryCount   src actual = " + destCount + " = " + srcCount);
        assertEquals(5, srcCount);

        System.out.println("stateCountryCount  dest  actual = " + destCount);
        assertEquals(5, destCount);

        System.out.println("stateCountryCount  doc  actual = " + docCount);
        assertEquals(5, docCount);


    }

    @Test
    @Order(4)
    @DisplayName("testFour")

    /**
     * - Check the population of TYPE_CONSOLDTD_RFRNC_WH for DEBT_TRNS type
     */
    void testFour() {
        // Select distinct country codes
        String sql1 = "SELECT DISTINCT PM_DEBT_HIST.TYPE, count(*) count \n" +
                "FROM DTSDM_SRC_STG.PM_DEBT_HIST PM_DEBT_HIST \n" +
                "group by PM_DEBT_HIST.TYPE\n";


        String sql2 = "SELECT DISTINCT \n" +
                "'DEBT_TRNS' AS TYPE_DESCR , \n" +
                "'DEBT_TRNS' AS RCD_TYPE_CD , \n" +
                "'Debt Transaction' AS RCD_TYPE_DESCR , \n" +
                "PM_DEBT_HIST.TYPE || 'DEBT_TRNS' AS TYPE_NATRL_KEY \n" +
                "FROM DTSDM_SRC_STG.PM_DEBT_HIST PM_DEBT_HIST \n";

        String sql3 = "select distinct STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_NAME \n" +
                "from DTSDM.STATE_COUNTRY_RFRNC_WH \n" +
                "minus \n" +
                "select distinct STATE.U##STNAME \n" +
                "from DTSDM_SRC_STG.STATE  \n";

        // if the count the same no duplicates are found
        ArrayList<String> typeList = new ArrayList<String>();
        ArrayList<Integer> typeCountList = new ArrayList<Integer>();
        int typeCount = 0;

        ArrayList<String> descrList = new ArrayList<String>();
        ArrayList<String> rcdTypeCdList = new ArrayList<String>();
        ArrayList<String> rcdTypeDescrList = new ArrayList<String>();
        ArrayList<String> typeNatrlKeyList = new ArrayList<String>();
        int descrCount = 0;

        int minusCount = 0;

        // Get dest count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String type = rs.getString("TYPE");
                        typeList.add(type);
                        Integer count = rs.getInt("COUNT");
                        typeCountList.add(count);
                        typeCount++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TypeConsoldtdRfrncWhTest.testFour sql1 failed");
            e.printStackTrace();
        }

        // Get total count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String typeDescr = rs.getString("TYPE_DESCR");
                        descrList.add(typeDescr);

                        String rcdTypeCd = rs.getString("RCD_TYPE_CD");
                        rcdTypeCdList.add(rcdTypeCd);

                        String rcdTypeDescr = rs.getString("RCD_TYPE_DESCR");
                        rcdTypeDescrList.add(rcdTypeDescr);

                        String typeNatrlKey = rs.getString("TYPE_NATRL_KEY");
                        typeNatrlKeyList.add(typeNatrlKey);
                        descrCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TypeConsoldtdRfrncWhTest.testFour sql2 failed");
            e.printStackTrace();
        }

        // Get minus count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String row = rs.getString(1);
                        minusCount++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TypeConsoldtdRfrncWhTest.testFour sql3 failed");
            e.printStackTrace();
        }


        System.out.println("Test Four    Destination Name  expected 302 actual = " + typeCount);
        assertEquals(302, typeCount);


        System.out.println("Test Four    Source Name  expected 301 actual = " + descrCount);
        assertEquals(301, descrCount);


        System.out.println("Test Four  Destination minus Src  Name  expected 1 actual = " + minusCount);
        assertEquals(1, minusCount);

    }


}
