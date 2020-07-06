import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TripWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("TripWhTest.html");
        wr.pageHeader();
    }

    @Test
    /*
     */
    public void test01() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment(" ");

        String sql1 = "Select count(*) from DTSDM.TRIP_WH \n" +
                "where TRIP_WH.TRIP_WID = 0\n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        wr.logSql(theSql);

        int row0Count = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        row0Count = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((1 == row0Count), "(1 == row0Count)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (1 == row0Count) " + row0Count);
        assertEquals(1, row0Count);

        System.out.println("Finish Trip_wh.test1");
    }

    @Test
    /*
     */
    public void test02() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment(" ");

        String sql1 = "Select count (distinct TRIP_WH.TRIP_WID) \n" +
                "from DTSDM. TRIP_WH";

        String sql2 = "Select count(*) \n" +
                "from DTSDM.TRIP_WH";

        String sql3 = "select distinct TRIP_WH.TRIP_WID, count(*) \n" +
                "from DTSDM. TRIP_WH \n" +
                "group by TRIP_WH.TRIP_WID \n" +
                "having count(*) > 1  ";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;
        int minusCount = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        totalCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String wid = rs.getString(1);
                        int rCont = rs.getInt(1);
                         minusCount++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((0 == minusCount), "(0 == minusCount)");
        roList.add(ro2);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Test assert  (0 == minusCount) " + minusCount  );
        assertEquals(0 ,minusCount);

        System.out.println("Finish Trip_wh.test2");
    }

    @Test
    /*
     */
    public void test03() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment(" ");

        String sql1 = "select count(*) from\n" +
                "(\n" +
                "select distinct person_wid, count(*)\n" +
                "from DTSDM.mnt_wh\n" +
                "where CURR_TRVL_DCMNT_SW = 1\n" +
                "group by person_wid\n" +
                ") \n";

        String sql2 = "select count(*) from\n" +
                "(select distinct person_wid\n" +
                "from DTSDM.Lrip_wh)\n";

        String sql3 = "select distinct person_wid\n" +
                "from DTSDM.mnt_wh\n" +
                "where CURR_TRVL_DCMNT_SW = 1\n" +
                "MINUS\n" +
                "select distinct person_wid\n" +
                "from DTSDM.Lrip_wh\n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;
        int minusCount = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        totalCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String wid = rs.getString(1);
                        int rCont = rs.getInt(1);
                        minusCount++;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((0 == minusCount), "(0 == minusCount)");
        roList.add(ro2);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Test assert  (0 == minusCount) " + minusCount  );
        assertEquals(0 ,minusCount);

        System.out.println("Finish Trip_wh.test3");
    }

    @Test
    /*
     */
    public void test04() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printYellowDiv("CHECK SQL AND COUNTS - Incorrect field in DB");

        String sql1 = "Select distinct VNDR_WH.VNCR_TYPE_WID, count (*)\n" +
                "From DTSDM.VNDR_WH\n" +
                "Group by VNDR_WH.VNCR_TYPE_WID\n";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        wr.logSql(theSql);

        int sql1Count = 0;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String vndrTypeWid = rs.getString(1);
                        sql1Count++;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }



        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((0 == sql1Count), "(0 == sql1Count)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (0 == sql1Count) " + 0  + " == " + sql1Count);
        assertEquals(0 ,sql1Count);


        System.out.println("Finish Trip_wh.test4");
    }

    @Test
    /*
     */
    @Ignore
    public void test05() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        totalCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish Trip_wh.test5");
    }

    @Test
    /*
     */
    public void test06() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printYellowDiv("SQL NEED REVIEW");

        String sql1 = "Select distinct VNDR_WH.CURR_SW, count(*)\n" +
                "From DTSDM.VNDR_WH\n" +
                "Group by VNDR_WH.CURR_SW";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        wr.logSql(theSql);

        int sql1Count = 0;


        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String curr_sw = rs.getString(1);
                        sql1Count++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((0 == sql1Count), "(0 == sql1Count)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (0 == sql1Count) " + 0  + " == " + sql1Count);
        assertEquals(0 ,sql1Count);

        System.out.println("Finish Trip_wh.test6");
    }

    @Test
    /*
     */
    @Ignore
    public void test07() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printYellowDiv("NO EFF_START_DT");

        String sql1 ="Select count (*) \n" +
                      "From DTSDM.VNDR_WH";

        String sql2 = "Select distinct VNDR_WH.EFF_START_DT, count (*)\n" +
                "        From DTSDM. VNDR_WH\n" +
                "        Group by VNDR_WH.EFF_START_DT";


        String sql3 = "Select count(VNDR_WH.EFF_START_DT)\n" +
                "                From DTSDM. VNDR_WH\n" +
                "        And VNDR_WH.EFF_START_DT is not null";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
        wr.logSql(theSql);

        int distinctCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((0 == distinctCount), "(0 == distinctCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (0 == distinctCount) " + 0  + " == " + distinctCount);
        assertEquals(0 ,distinctCount);

        System.out.println("Finish Trip_wh.test7");
    }

    @Test
    /*
     */
    @Ignore
    public void test08() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";

        String sql3 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((0 == distinctCount), "(0 == distinctCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (0 == distinctCount) " + 0  + " == " + distinctCount);
        assertEquals(0 ,distinctCount);

        System.out.println("Finish Trip_wh.test8");
    }

    @Test
    /**
     * --TRIP_WH ROW COUNT
     */
    public void test09() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "select src.src_cnt - trgt.trgt_cnt as rcd_cnt_discrepancy\n" +
                "from\n" +
                "    (\n" +
                "        select count(*) src_cnt\n" +
                "        from\n" +
                "            (\n" +
                "                select distinct\n" +
                "                dcmnt_base_name,\n" +
                "                src_ssn\n" +
                "                from DTSDM.mnt_wh\n" +
                "                where adjstmt_lvl = 0\n" +
                "                and dcmnt_actv_ind = 'Y'\n" +
                "                and dcmnt_wid != 0\n" +
                "            )\n" +
                "        ) src,\n" +
                "    (\n" +
                "        select count(trip_wid) trgt_cnt\n" +
                "        from DTSDM.Lrip_wh\n" +
                "        where trip_wid != 0\n" +
                "    ) trgt";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting TripWhTest.test09,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
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
        ResultObject ro1 = new ResultObject((0 == number),"(0 == number)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test DebtWh   0 == " + number);
        assertEquals(0, number);

        System.out.println("Finish TripWhTest.test09");
        System.out.println();
    }

}
