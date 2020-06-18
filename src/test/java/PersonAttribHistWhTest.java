import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PersonAttribHistWhTest  extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults(" PersonAttribHistWhTest.html");
        wr.pageHeader();
    }

    @Test
    /*
     */
    public void test1() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment("Check that the \"unknown\" record 0 is populated.");

        String sql1 = "Select count(*) from DTSDM.PERSON_ATTRIB_HIST_WH \n" +
                "where PERSON_ATTRIB_HIST_WH.PERSON_ATTRIB_WID = 0";

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

        System.out.println("Finish Person_attrib_Wh.test1");
    }

    @Test
    /*
     */
    public void test2() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment("Check that the \"unknown\" record 0 is populated.");

        String sql1 = "Select count (distinct PERSON_ATTRIB_HIST_WH.PERSON_ATTRIB_WID)\n" +
                "        from DTSDM. PERSON_ATTRIB_HIST_WH";

        String sql2 = "Select count(*)\n" +
                "        from DTSDM.PERSON_ATTRIB_HIST_WH";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
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

        System.out.println("Finish Person_attrib_Wh.test2");
    }
    @Test
    /*
     */
    public void test3() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment("Check that the \"unknown\" record 0 is populated.");

        String sql1 = "select distinct PERSON_WID\n" +
                "from DTSDM.PERSON_WH\n";

        String sql2 = "select distinct PERSON_WID, count(*)\n" +
                "from DTSDM.PERSON_ATTRIB_HIST_WH\n" +
                "group by PERSON_WID  \n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int row1Count = 0;
        int row2Count = 0;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        int wid = rs.getInt(1);
                        row1Count++;

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
                        String  wid = rs.getString(1);
                        int grpWidCount = rs.getInt(2);
                         row2Count++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((row1Count == row2Count), "(row1Count == row2Count)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (row1Count == row2Count) " + row1Count + " " +row2Count);
        assertEquals(row1Count , row2Count);

        System.out.println("Finish Person_attrib_Wh.test3");
    }

    @Test
    /*
     */
    public void test4() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment("Check that the \"unknown\" record 0 is populated.");

        String sql1 = "select distinct a.agncy_org_wid\n" +
                "from dtsdm.agncy_org_wh a,\n" +
                "     dtsdm.suborg_wh b,\n" +
                "     dtsdm_src_stg.person c\n" +
                "where c.org = b.full_org_cd \n" +
                "and a.agncy_org_wid = b.agncy_org_wid\n";

        String sql2 = "select distinct a.agncy_org_wid\n" +
                "from dtsdm.person_attrib_hist_wh a\n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);
        int row0Count = -1;

        ArrayList<Integer> sql1List = new ArrayList<>();
        ArrayList<Integer> sql2List = new ArrayList<>();

        int sql1RowCount = 0;
        int sql2RowCount = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        Integer i1 = rs.getInt(1);
                        sql1List.add(i1);
                        sql1RowCount++;
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
                        Integer i2 = rs.getInt(1);
                        sql2List.add(i2);
                        sql2RowCount++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Boolean  res = sql1List.equals(sql2List);
        System.out.println("Woo Hoo" + res);
        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((sql1RowCount == sql2RowCount), "(sql1RowCount == sql2RowCount)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((sql1List.equals(sql2List)), "(sql1List.equals(sql2List))");
        roList.add(ro2);
        wr.logTestResults(roList);

        System.out.println("Test assert  (sql1RowCount == sql2RowCount) " + sql1RowCount + " == " + sql2RowCount);
        assertEquals(sql1RowCount, sql2RowCount);
        System.out.println("Test assert  (sql1List.equals(sql2List)) " + sql1List.equals(sql2List));
        assertEquals(true, res);

        System.out.println("Finish Person_attrib_Wh.test4");
    }

    @Test
    /*
     */
    public void test5() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment("Check that the \"unknown\" record 0 is populated.");

        String sql1 = "select distinct a.suborg_wid\n" +
                "from DTSDM.person_attrib_hist_wh a " +
                "where suborg_wid != 0 \n" +
                "order by a.suborg_wid";

        String sql2 = "select distinct a.suborg_wid\n" +
                "from dtsdm.suborg_wh a,\n" +
                "     DTSDM_SRC_STG.person b\n" +
                "where b.org = a.full_org_cd  \n" +
                "order by a.suborg_wid";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        ArrayList<Integer> sql1List = new ArrayList<>();
        ArrayList<Integer> sql2List = new ArrayList<>();

        int sql1RowCount = 0;
        int sql2RowCount = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        Integer i1 = rs.getInt(1);
                        sql1List.add(i1);
                        sql1RowCount++;
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
                        Integer i2 = rs.getInt(1);
                        sql2List.add(i2);
                        sql2RowCount++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Boolean  res = sql1List.equals(sql2List);


        System.out.println("The two list are equal " + res);
        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((sql1RowCount == sql2RowCount), "(sql1RowCount == sql2RowCount)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((sql1List.equals(sql2List)), "(sql1List.equals(sql2List))");
        roList.add(ro2);
        wr.logTestResults(roList);

        System.out.println("Test assert  (sql1RowCount == sql2RowCount) " + sql1RowCount + " == " + sql2RowCount);
        assertEquals(sql1RowCount, sql2RowCount);
        System.out.println("Test assert  (sql1List.equals(sql2List)) " + sql1List.equals(sql2List));
        assertEquals(true, res);

        System.out.println("Finish Person_attrib_Wh.test5");
    }

    @Test
    /*
     */
    @Ignore
    public void test6() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printComment("Check that the \"unknown\" record 0 is populated.");

        String sql1 = "select distinct a.ssn_full\n" +
                "from DTSDM.person_attrib_hist_wh a  \n";

        String sql2 = "select distinct u##ssn\n" +
                "from DTSDM_SRC_STG.person  \n";

        String sql3 = "select distinct u##ssn\n" +
                "from DTSDM_SRC_STG.person \n" +
                "minus\n" +
                "select distinct a.ssn_full\n" +
                "from DTSDM.person_attrib_hist_wh a   ";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
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

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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

        System.out.println("Finish Person_attrib_Wh.test2");
    }

 }
