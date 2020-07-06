import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class VndrWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("VndrWhTest.html");
        wr.pageHeader();
    }


    @Test
    public void test01() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check that the \"unknown\" record 0 is populated";
        String reason = " Provides the ability to traverse through the VNDR_WH when no value is matched";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql = "Select count(*) \n" + "from DTSDM.VNDR_WH \n" + " where VNDR_WH.VNDR_WID = 0 \n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting VndrWhTest.test1,sql");
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

        System.out.println("Test VndrWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish VndrWhTest.test1");
        System.out.println();
    }

    @Test
    /**
     *Check the population of the VNDR_WH.VNDR_WID  (PK) column.
     */
    public void test02() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the VNDR_WH.VNDR_WID  (PK) column";
        String reason = " Unique identifier of the record. Sequential ID";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        // Select count distinct rows
        String sql1 = "Select count (distinct VNDR_WH.VNDR_WID) \n" +
                "from DTSDM. VNDR_WH \n";

         // "-- Should equal this count:\n" ;
         String sql2 =       "Select count(*)\n" +
                "from DTSDM. VNDR_WH\n" ;

                //"-- EXPECT 0 records return from the query below:\n" +
         String sql3 =       "select distinct VNDR_WH.VNDR_WID, count(*)\n" +
                "from DTSDM. VNDR_WH\n" +
                "group by VNDR_WH.VNDR_WID\n" +
                "having count(*) > 1\n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        // if the count the same no duplicates are found
        int dupeCount = 0;
        int totalCount = 0;
        int distinctCount = 0;

        System.out.println("Starting VndrWhTest.test2,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("VndrWhTest.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting VndrWhTest.test2,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        totalCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("VndrWhTest.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting VndrWhTest.test2,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        dupeCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("VndrWhTest.test2 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((totalCount== distinctCount),"(totalCount == distinctCount)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((0 == dupeCount),"(0 == dupeCount)");
        roList.add(ro2);

        wr.logTestResults(roList);
        System.out.println("VndrWhTest.test2 totalCount  " +  totalCount );
        System.out.println("VndrWhTest.test2 distinctCount  " +  distinctCount );
        System.out.println("VndrWhTest.test2 dupeCount  " +  dupeCount );
        System.out.println("VndrWhTest.test2 distinctCount == totalCount " + (distinctCount == totalCount) );
        assertEquals(distinctCount , totalCount);

        System.out.println("VndrWhTest.test2 0 == dupeCount " + (0 == dupeCount) );
        assertEquals(0, dupeCount);
        System.out.println("Finish VndrWhTest.test2");
    }

    @Test
    /**
     * Check the population of the VNDR_WH.VNDR_NAME columns’
     */
    public void test03() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the VNDR_WH.VNDR_NAME column";
        String reason = " Get distinct vendor name/vendor address";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printYellowDiv("Need to review SQLS");

        // Select distinct country codes
        // All dprtmnt_id should be 1 expect the row 0;
        String sql1 = "select distinct VNDR_WH.VNDR_NAME, count(*)\n" +
                "from DTSDM.VNDR_WH\n" +
                "group by VNDR_WH.VNDR_NAME\n" +
                "order by VNDR_WH.VNDR_NAME \n";

        String sql2 = "select distinct PNRTOUCH.CTO\n" +
                "from DTSDM_SRC_STG.PNRTOUCH\n" +
                "inner join TICKSUB\n" +
                "on CTO = VENDOR\n";

        String sql3 = "select distinct PNRTOUCH.CTO, count(*)\n" +
                "from DTSDM_SRC_STG.PNRTOUCH\n" +
                "group by PNRTOUCH.CTO\n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        // if the count the same no duplicates are found
        int sql1Count = 0;
        int sql2Count = 0;
        int sql3Count = 0;

        System.out.println("Starting VndrWhTest.test3,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String vndrName = rs.getString(1);
                        sql1Count++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("VndrWh.test3 sql1 failed");
            e.printStackTrace();
        }
        System.out.println("Starting VndrWhTest.test3,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String cto = rs.getString(1);
                        sql2Count++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("VndrWh.test3 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting VndrWhTest.test3,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String cto = rs.getString(1);
                        sql3Count++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("VndrWh.test3 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((1 == sql1Count),"(1 == sql1Count)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((1 == sql2Count),"(1 == sql2Count)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == sql3Count),"(1 == sql3Count)");
        roList.add(ro3);
        wr.logTestResults(roList);

        //System.out.println("VndrWh  DprtmntWid  count should be 1  actual = " + dptWidCount);
        //assertEquals(1, dptWidCount);

        //System.out.println("VndrWh  DprtmntWid  should = " + dptWid);
        //assertEquals(1, dptWid);

        System.out.println("Finish VndrWhTest.test3");

    }

    @Test
    /**
     * Check the population of the VNDR_WH.VNDR_TYPE_WID column.
     */
    public void test04() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the VNDR_WH.VNDR_TYPE_WID column";
        String reason = " Look up TYPE_WID in TYPE_CONSOLDTD_RFRNC_WH. For vendors sourced from DTSDM.LICKSUB, get ID for record having type code 'PRPTY', or Property. For vendors sourced from VCHPNR, get ID for record having  type 'CTO'";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);




        String sql1 = "Select distinct VNDR_WH.VNDR_TYPE_WID, count (*)\n" +
                "From DTSDM.VNDR_WH\n" +
                "Group by VNDR_WH.VNDR_TYPE_WID\n ";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);

        wr.logSql(theSql);

        // if the count the same no duplicates are found
        int destCount = 0;
        int srcCount = 0;
        int minusCount = 0;
        String row = null;

        System.out.println("Starting VndrWhTest.test4,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        destCount = rs.getInt(2);

                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("VndrWhTest.test4 sql1 failed");
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


        System.out.println("VndrWh Test Four    Destination count actual = " + destCount);
        assertTrue(destCount >= 0);

        System.out.println("VndrWh Test Four    Source count actual = " + srcCount);
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
     * Check the values of the VNDR_WH.AGNCY_DESCR column
     */
    @Ignore
    public void test05() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the values of the VNDR_WH.AGNCY_DESCR column";
        String reason = " No business rules for this column! But looks like this column represents the sum(CTO_FEE) for CTO’s in DTSDM_SRC_STG.PNRTOUCH";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        // Select distinct country codes
        String sql1 = "";

        String sql2 = "";

        String sql3 = "";

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

        System.out.println("Starting VndrWhTest.test5,sql1");
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
            System.out.println("VndrWh.test5 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting VndrWhTest.test5,sql2");
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
            System.out.println("VndrWh.test4 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting VndrWhTest.test5,sql3");
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
            System.out.println("VndrWh.test4 sql2 failed");
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

        System.out.println("VndrWh Test Five  AGNCY_DESCR  Destination count actual = " + agncyDescrCount);
        assertTrue(agncyDescrCount >= 0);

        System.out.println("VndrWh Test Five  AGNCY_DESCR  Source count actual = " + srcAgncyDescrCount);
        assertTrue(srcAgncyDescrCount >= 0);

        System.out.println("Test Five  AGNCY_DESCR  Destination count should be equal Source Count " + agncyDescrCount
                + " = " + srcAgncyDescrCount);
        assertEquals(agncyDescrCount, srcAgncyDescrCount );

        System.out.println("Test Five  NOT IN  Destination count should equal 1 'UNK' " + minusCount);
        assertEquals(0, minusCount);

    }

    @Test
    /**
     * Check the population of the VNDR_WH.CURR_SW column
     */
    public void test06() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the VNDR_WH.CURR_SW column";
        String reason = " Indicates whether the record is the current record for the agency.  value = 1 if current, 0 if not current";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        // Select distinct country codes
        String sql1 = "Select distinct VNDR_WH.CURR_SW, count(*) \n" +
                "                From DTSDM. VNDR_WH\n" +
                "                Group by VNDR_WH.CURR_SW";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        wr.logSql(theSql);


        // if the count the same no duplicates are found
        int tableRowsCount = 0;
        int groupByCount = 0;
        String currSw = null;
        System.out.println("Starting VndrWhTest.test6,sql1");
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
            System.out.println("VndrWh.test6 sql1 failed");
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
     * Check the population of the VNDR_WH.EFF_START_DT column
     */
    @Ignore
    public void test07() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the VNDR_WH.EFF_START_DT column";
        String reason = " Default EFF_STRT_DT = sysdate";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        // Select distinct EFF_START_DT
        String sql1 = "Select count (*)\n" +
                "From DTSDM. VNDR_WH\n";

        String sql2 = "Select distinct VNDR_WH.EFF_START_DT, count (*)\n" +
                "From DTSDM. VNDR_WH\n" +
                "Group by VNDR_WH.EFF_START_DT\n";

        String sql3 = "Select count(VNDR_WH.EFF_START_DT)\n" +
                "From DTSDM. VNDR_WH\n" +
                "And VNDR_WH.EFF_START_DT is not null\n";

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

        System.out.println("Starting VndrWhTest.test7,sql1");
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
            System.out.println("VndrWh.test7 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting VndrWhTest.test7,sql2");
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
            System.out.println("VndrWh.test7 sql2 failed");
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
     * Check the population of the VNDR_WH.UPDATE_DATE column
     */
    @Ignore
    public void test08() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the VNDR_WH.UPDATE_DATE column";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        // Select distinct EFF_START_DT
        String sql1 = "Select count (*)\n" +
                "From DTSDM. VNDR_WH\n";

        String sql2 = "Select distinct VNDR_WH.EFF_END_DT, count (*)\n" +
                "From DTSDM. VNDR_WH\n" +
                "Group by VNDR_WH.EFF_END_DT\n";

        String sql3 = "Select count(VNDR_WH.EFF_END_DT)\n" +
                "From DTSDM. VNDR_WH\n" +
                "And VNDR_WH.EFF_END_DT is not null\n";

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

        System.out.println("Starting VndrWhTest.test8,sql1");
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
            System.out.println("VndrWh.test8 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting VndrWhTest.test8,sql2");
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
            System.out.println("VndrWh.test8 sql2 failed");
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

    @Test
    /**
     * --VNDR_WH  ROW COUNT Althea
     */
    public void test09() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " VNDR_WH  ROW COUNT";
        String reason = " VNDR_WH  ROW COUNT";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "--VNDR_WH  ROW COUNT\n" +
                "select src.src_cnt - trgt.trgt_cnt rcd_cnt_discrepancy\n" +
                "from\n" +
                "    (\n" +
                "        select count(*) src_cnt\n" +
                "        from\n" +
                "            (\n" +
                "                select distinct vendor\n" +
                "                from dtsdm_src_stg.ticksub\n" +
                "                union\n" +
                "                select distinct vendor\n" +
                "                from dtsdm_src_stg.itinry\n" +
                "                union\n" +
                "                select distinct exp_vendor\n" +
                "                from dtsdm_src_stg.lodge\n" +
                "                union\n" +
                "                select distinct ldg_vendor\n" +
                "                from dtsdm_src_stg.lodge\n" +
                "                union\n" +
                "                select distinct mie_vendor\n" +
                "                from dtsdm_src_stg.lodge\n" +
                "                union\n" +
                "                select distinct cto\n" +
                "                from dtsdm_src_stg.pnrtouch\n" +
                "            )\n" +
                "        where vendor != ' '\n" +
                "        )src,\n" +
                "        (\n" +
                "            select count(*) trgt_cnt\n" +
                "            from dtsdm.vndr_wh\n" +
                "            where vndr_wid != 0\n" +
                "        ) trgt";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting "+ this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName() + " sql1" );
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

        System.out.println("Test VndrWh 0 == " + number);
        assertEquals(0, number);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test09");
        System.out.println();
    }

}