import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MyTestWatcher.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("StatusConsoldtdRfrncWhTest")
public class StatusConsoldtdRfrncWhTest {

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
    /**
     * Check that the "unknown" record 0 is populated. Pass  ???
     * -- EXPECT - STATUS_WID = 0; STATUS_MASTER_WID = 0; STATUS_CD = 'UN'; STATUS_DESCR = 'UNKNOWN'; others NULL
     */
    void testOne() {
        String sql = "Select * from DTSDM.STATUS_CONSOLDTD_RFRNC_WH where STATUS_WID=0";
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

        System.out.println("Test StatusConsoldtdRfrncWhTest Success " + "Row 0  count = " + number) ;
        assertEquals(1, number);

        System.out.println("Test StatusConsoldtdRfrncWhTest Success " + "Row 0  count = 1") ;
    }

    @Test
    @Order(2)
    @DisplayName("testTwo")
    /**
     *
     */
    void testTwo() {
        // Select count distinct rows
        String sql1 = "Select distinct count(STATUS_WID) from DTSDM.STATUS_CONSOLDTD_RFRNC_WH";

        // Select total distinct rows
        String sql2 = "Select count(*) from DTSDM.STATUS_CONSOLDTD_RFRNC_WH";

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
            System.out.println("StatusConsoldtdRfrncWhTest.status sql1 failed");
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
            System.out.println("StatusConsoldtdRfrncWhTest. sql2 failed");
            e.printStackTrace();
        }

        assertEquals(totalCount, distinctCount);
        System.out.println("StatusConsoldtdRfrncWhTest  DistinctCount = TotalCount " + distinctCount + " = " + totalCount) ;
    }

    @Test
    @Order(3)
    @DisplayName("testThree")
    /**
     * Check to ensure that all distinct DCMNT records are being populated.
     * -- EXPECT count of [Select count(distinct(ds.cur_status)) from FRED.DOCSTAT ds;]
     */
    void testThree() {
        // Select distinct country codes
        String sql1 = "Select count(distinct(ds.cur_status)) from DTSDM_SRC_STG.DOCSTAT ds";


        String sql2 = "Select count(*) from DTSDM.STATUS_CONSOLDTD_RFRNC_WH scr \n" +
                "where scr.status_descr in (Select distinct (cur_status) from DTSDM_SRC_STG.DOCSTAT) and scr.RCD_TYPE_CD = \n" +
                "'DCMNT'\n";

        String sql3 = "select distinct STATUS_WID,STATUS_CD,STATUS_DESCR,STATUS_TYPE_DESCR,RCD_TYPE_CD,RCD_TYPE_DESCR, count(*)  \n" +
                "from status_consoldtd_rfrnc_wh \n" +
                "where rcd_type_cd = 'DCMNT' \n" +
                "group by STATUS_WID,STATUS_CD,STATUS_DESCR,STATUS_TYPE_DESCR,RCD_TYPE_CD,RCD_TYPE_DESCR \n" +
                "having count(*) > 1 \n";

        // if the count the same no duplicates are found
        int destCount = 0;
        int srcCount = 0;
        int dupeCount = 0;

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
            System.out.println("StatusConsoldtdRfrncWhTest.testThree sql1 failed");
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
            System.out.println("StatusConsoldtdRfrncWhTest.testThree sql2 failed");
            e.printStackTrace();
        }

        // Get dupe count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));

                    while(rs.next()) {
                        String row =  rs.getString(1);
                        dupeCount++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.testThree sql2 failed");
            e.printStackTrace();
        }

            System.out.println("StatusConsoldtdRfrncWhTest  dest / src actual = " + destCount + " = " + srcCount);
            assertEquals(destCount, srcCount);




        System.out.println("stateCountryCount  Destination duplicate actual = " + dupeCount) ;
        assertEquals(0, dupeCount);
        System.out.println();


    }

    @Test
    @Order(5)
    @DisplayName("testFour")
    void testFour() {
        // Select distinct country codes
        String sql1 = "select * from STATUS_CONSOLDTD_RFRNC_WH where RCD_TYPE_CD = 'DEBT_TRNS'";


        String sql2 = "select distinct status_descr from STATUS_CONSOLDTD_RFRNC_WH \n" +
                "where RCD_TYPE_CD = 'DEBT_TRNS'\n";

        String sql3 = "Select distinct pdh.status from DTSDM_SRC_STG.pm_debt_hist pdh";

        String sql4 = "select distinct status_type_descr from STATUS_CONSOLDTD_RFRNC_WH \n" +
                "where RCD_TYPE_CD = 'DEBT_TRNS'\n";

        String sql5 = "Select distinct pdh.type from DTSDM_SRC_STG.pm_debt_hist pdh";

        // if the count the same no duplicates are found
        int rowCountSql1 = 0;

        int rowCountSql2 = 0;
        int rowCountSql3 = 0;
        int rowCountSql4 = 0;
        int rowCountSql5 = 0;

        // Get dest count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        String statusWid =  rs.getString("STATUS_WID");
                        String statusCd =  rs.getString("STATUS_CD");
                        String statusDescr =  rs.getString("STATUS_Descr");
                        String statusTyeDescr =  rs.getString("STATUS_TYPE_DESCR");
                        String rcdTypeCd =  rs.getString("RCD_TYPE_CD");
                        String rcdTypeDescr =  rs.getString("RCD_TYPE_DESCR");
                        rowCountSql1++;


                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.testFour sql1 failed");
            e.printStackTrace();
        }

        // Get total count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        String StatusDescr =  rs.getString("STATUS_DESCR");
                        rowCountSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.testFour sql2 failed");
            e.printStackTrace();
        }

        // Get minus count
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        String status =  rs.getString("STATUS");
                        rowCountSql3++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.testFour sql3 failed");
            e.printStackTrace();
        }

        // GetSql 4
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        String statusTypeDescr =  rs.getString("STATUS_TYPE_DESCR");
                        rowCountSql4++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.testFour sql4 failed");
            e.printStackTrace();
        }

        // GetSql 5
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql5)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {

                    while(rs.next()) {
                        String type =  rs.getString("TYPE");
                        rowCountSql5++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.testFour sql5 failed");
            e.printStackTrace();
        }


        System.out.println("Test Five   Sql 1 rowcount " + rowCountSql1) ;
        //assertEquals(301, rowCountSql1);

        System.out.println("Test Five  rowCount2 should equal rowCount3 " + rowCountSql2 + " = " + rowCountSql3) ;
        assertEquals(rowCountSql2, rowCountSql3);

        System.out.println("Test Five  rowCount4 should equal rowCount5 " + rowCountSql4 + " = " + rowCountSql5) ;
        assertEquals(rowCountSql2, rowCountSql3);

    }


}
