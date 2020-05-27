
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StatusConsoldtdRfrncWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("StatusConsoldtdRfrncWhTest.html");
        wr.pageHeader();
    }

    @Test
    /**
     * Check that the "unknown" record 0 is populated. Pass  ???
     * -- EXPECT - STATUS_WID = 0; STATUS_MASTER_WID = 0; STATUS_CD = 'UN'; STATUS_DESCR = 'UNKNOWN'; others NULL
     */
    public void test1() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "Select * from DTSDM.STATUS_CONSOLDTD_RFRNC_WH where STATUS_WID=0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        wr.logSql(theSql);


        int number = 0;

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test1,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((1 == number), "(1 == number)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test StatusConsoldtdRfrncWhTest Success " + "Row 0  count = " + number);
        assertEquals(1, number);

        System.out.println("Test StatusConsoldtdRfrncWhTest Success " + "Row 0  count = 1");
    }

    @Test
    /**
     *
     */
    public void test2() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        // Select count distinct rows
        String sql1 = "Select distinct count(STATUS_WID) from DTSDM.STATUS_CONSOLDTD_RFRNC_WH";

        // Select total distinct rows
        String sql2 = "Select count(*) from DTSDM.STATUS_CONSOLDTD_RFRNC_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);


        // if the count the same no duplicates are found
        int distinctCount = 0;
        int totalCount = 0;

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test2,sql1");
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
            System.out.println("StatusConsoldtdRfrncWhTest.status sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test2,sql2");
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
            System.out.println("StatusConsoldtdRfrncWhTest. sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((totalCount == distinctCount), "(totalCount == distinctCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        assertEquals(totalCount, distinctCount);
        System.out.println("StatusConsoldtdRfrncWhTest  DistinctCount = TotalCount " + distinctCount + " = " + totalCount);
    }

    @Test
    /**
     * Check to ensure that all distinct DCMNT records are being populated.
     * -- EXPECT count of [Select count(distinct(ds.cur_status)) from FRED.DOCSTAT ds;]
     */
    public void test3() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

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

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
        wr.logSql(theSql);


        // if the count the same no duplicates are found
        int destCount = 0;
        int srcCount = 0;
        int dupeCount = 0;

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test3,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        destCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.test3 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test3,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        srcCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.test3 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test3,sql3");
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
            System.out.println("StatusConsoldtdRfrncWhTest.test3 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((destCount == srcCount), "(destCount == srcCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("StatusConsoldtdRfrncWhTest  dest / src actual = " + destCount + " = " + srcCount);
        assertEquals(destCount, srcCount);


        System.out.println("stateCountryCount  Destination duplicate actual = " + dupeCount);
        assertEquals(0, dupeCount);
        System.out.println();


    }

    @Test

    public void test4() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        // Select distinct country codes
        String sql1 = "select * from STATUS_CONSOLDTD_RFRNC_WH where RCD_TYPE_CD = 'DEBT_TRNS'";


        String sql2 = "select distinct status_descr from STATUS_CONSOLDTD_RFRNC_WH \n" +
                "where RCD_TYPE_CD = 'DEBT_TRNS'\n";

        String sql3 = "Select distinct pdh.status from DTSDM_SRC_STG.pm_debt_hist pdh";

        String sql4 = "select distinct status_type_descr from STATUS_CONSOLDTD_RFRNC_WH \n" +
                "where RCD_TYPE_CD = 'DEBT_TRNS'\n";

        String sql5 = "Select distinct pdh.type from DTSDM_SRC_STG.pm_debt_hist pdh";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
        SqlObject sqlObj4 = new SqlObject("sql4", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj4);
        SqlObject sqlObj5 = new SqlObject("sql5", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj5);

        wr.logSql(theSql);


        // if the count the same no duplicates are found
        int rowCountSql1 = 0;

        int rowCountSql2 = 0;
        int rowCountSql3 = 0;
        int rowCountSql4 = 0;
        int rowCountSql5 = 0;

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test4,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String statusWid = rs.getString("STATUS_WID");
                        String statusCd = rs.getString("STATUS_CD");
                        String statusDescr = rs.getString("STATUS_Descr");
                        String statusTyeDescr = rs.getString("STATUS_TYPE_DESCR");
                        String rcdTypeCd = rs.getString("RCD_TYPE_CD");
                        String rcdTypeDescr = rs.getString("RCD_TYPE_DESCR");
                        rowCountSql1++;


                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.test4 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test4,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String StatusDescr = rs.getString("STATUS_DESCR");
                        rowCountSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.test4 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test4,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String status = rs.getString("STATUS");
                        rowCountSql3++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.test4 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test4,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String statusTypeDescr = rs.getString("STATUS_TYPE_DESCR");
                        rowCountSql4++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.test4 sql4 failed");
            e.printStackTrace();
        }

        System.out.println("Starting StatusConsoldtdRfrncWhTest.test4,sql5");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql5)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {

                    while (rs.next()) {
                        String type = rs.getString("TYPE");
                        rowCountSql5++;

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("StatusConsoldtdRfrncWhTest.test4 sql5 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((rowCountSql2 == rowCountSql3), "(rowCountSql2 == rowCountSql3)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((rowCountSql4 == rowCountSql5), "(rowCountSql4 == rowCountSql5)");
        roList.add(ro2);
        wr.logTestResults(roList);


        System.out.println("Test Five  rowCount2 should equal rowCount3 " + rowCountSql2 + " = " + rowCountSql3);
        assertEquals(rowCountSql2, rowCountSql3);

        System.out.println("Test Five  rowCount4 should equal rowCount5 " + rowCountSql4 + " = " + rowCountSql5);
        assertEquals(rowCountSql2, rowCountSql3);

    }


}
