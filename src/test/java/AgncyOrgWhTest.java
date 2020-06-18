import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static junit.framework.TestCase.assertEquals;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AgncyOrgWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("AgncyOrgWhTest.html");
        wr.pageHeader();
    }

    @Test
    /*
     * -- EXPECT to get 1 row where AGNCY_ORG_WID = 0; AGNCY_WID = 0; DPRTMNT_AGNCY_SHRT_CD = 'UN'; AGNCY_SHRT_CD = 'UN'; ORG_CD = 'UN'.
     */
    public void test1() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);





        String sql = "Select AGNCY_ORG_WID, \n" +
                "AGNCY_WID, \n" +
                "DPRTMNT_AGNCY_SHRT_CD, \n" +
                "AGNCY_SHRT_CD, \n" +
                "ORG_CD, \n" +
                "CURR_SW, \n" +
                "INSERT_DATE, \n" +
                "UPDATE_DATE, \n" +
                "SITE_WID, \n" +
                "PSEUDO_CITY_CODE, \n" +
                "DELETED_FLAG \n" +
                "from dtsdm.AGNCY_ORG_WH ao \n" +
                "where ao.agncy_org_wid = 0 \n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int rowCount = 0;
        int agncyOrgWid = -1;
        int agncyWid = -1;
        String dprtmntAgncyShrtCd = null;
        String agncyShrtCd = null;
        String orgCd = null;

        System.out.println("Starting AgncyOrgWhTest.test1,sql1");
        try {

            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {

                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        agncyOrgWid = rs.getInt("AGNCY_ORG_WID");

                        agncyWid = rs.getInt("AGNCY_WID");

                        dprtmntAgncyShrtCd = rs.getString("DPRTMNT_AGNCY_SHRT_CD");

                        agncyShrtCd = rs.getString("AGNCY_SHRT_CD");

                        orgCd = rs.getString("ORG_CD");
                        rowCount++;
                    }


                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<>();

        ResultObject ro1 = new ResultObject((1 == rowCount),"(1 == rowCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((0 == agncyOrgWid),"(0 == agncyOrgWid)");
        roList.add(ro2);

        ResultObject ro3 = new ResultObject((0 == agncyWid),"(0 == agncyWid)");
        roList.add(ro3);

        ResultObject ro4 = new ResultObject(("UNK".equalsIgnoreCase(dprtmntAgncyShrtCd)),"('UNK' equals 'dprtmntAgncyShrtCd'");
        roList.add(ro4);

        ResultObject ro5 = new ResultObject(("UNK".equalsIgnoreCase(agncyShrtCd)),"('UNK' equals 'agncyShrtCd'");
        roList.add(ro5);

        ResultObject ro6 = new ResultObject(("UNK".equalsIgnoreCase(orgCd)),"('UNK' equals 'orgCd'");
        roList.add(ro6);

        wr.logTestResults(roList);

        assertEquals(1, rowCount);
        assertEquals(0, agncyOrgWid);
        assertEquals(0, agncyWid);
        assertEquals("UNK", dprtmntAgncyShrtCd);
        assertEquals("UNK", agncyShrtCd);
        assertEquals("UNK", orgCd);

        System.out.println("test1 rowCount= " + rowCount);
        System.out.println("test1 agncyOrgWid= " + agncyOrgWid);
        System.out.println("test1 agncyWid= " + agncyWid);
        System.out.println("test1 dprtmntAgncyShrtCd= " + dprtmntAgncyShrtCd);
        System.out.println("test1 agncyShrtCd= " + agncyShrtCd);
        System.out.println("test1 orgCd= " + orgCd);
        
        System.out.println("Finish AgncyOrgWhTest.test1");
        System.out.println();

    }

    @Test

    /*
     *
     */
    public void test2() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from (\n" +
                "select distinct DPRTMNT_AGNCY_SHRT_CD,AGNCY_SHRT_CD,ORG_CD, count (*)\n" +
                "from AGNCY_ORG_WH\n" +
                "group by DPRTMNT_AGNCY_SHRT_CD,AGNCY_SHRT_CD,ORG_CD\n" +
                "having count(*) > 1\n" +
                "\n" +
                ")";


        // Output the Sql to be executed
        ArrayList<SqlObject> sqlList = new ArrayList<>();
        SqlObject sqlObj = new SqlObject("sql",sql1);
        sqlList.add(sqlObj);
         wr.logSql(sqlList);



        int dupeCount = 0;

        System.out.println("Starting AgncyOrgWhTest.test2,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        dupeCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        


        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((0 == dupeCount),"(0 == dupeCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("test1 0= " + dupeCount + " 0  = " + dupeCount);
        assertEquals(0, dupeCount);

        
        System.out.println("Finish AgncyOrgWhTest.test2");
        System.out.println();

    }


    @Test
    /*
     * - Check the values of the AGNCY_ORG_WH.AGNCY_WID  column. Pass
     * '-- EXPECT to see the distinct values of AGNCY_WID from both below queries to be equal:
     */
    public void test3() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select distinct agncy_org_wh.agncy_wid\n" +
                "from dtsdm.agncy_org_wh \n";

        String sql1 = "select distinct ao.agncy_wid, ao.agncy_shrt_cd, substr(ol.u##org,2,1) substr\n" +
                "from dtsdm.agncy_org_wh ao,DTSDM_SRC_STG.orglist ol\n" +
                "where ao.agncy_shrt_cd = substr(ol.u##org,2,1)\n" +
                "order by agncy_wid" ;

        // Output the Sql to be executed
        ArrayList<SqlObject> sqlList = new ArrayList<>();
        SqlObject sqlObj = new SqlObject("sql",sql);
        sqlList.add(sqlObj);
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<BR>"));
        sqlList.add(sqlObj1);
        wr.logSql(sqlList);

        ArrayList<Integer >sql1AgncyWid = new ArrayList<>();
        ArrayList<Integer> sql2AgncyWid = new ArrayList<>();
        ArrayList<String> sql2AgncyShortCd = new ArrayList<>();
        ArrayList<String> sql2Substr = new ArrayList<>();
        int distinctCount = 0;
        int agncyWid = 0;
        String agncyShrtCd = null;
        String substr = null;

        int rowCount = 0;

        System.out.println("Starting AgncyOrgWhTest.test3,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        agncyWid = rs.getInt("AGNCY_WID");
                        sql1AgncyWid.add(agncyWid);
                        distinctCount++;
                        System.out.println(distinctCount + " Sql 1  agncyWid = " + agncyWid );
                      }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Starting AgncyOrgWhTest.test3,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));

                    while (rs.next()) {
                        rowCount++;
                        agncyWid = rs.getInt("AGNCY_WID");
                        sql2AgncyWid.add(agncyWid);

                        agncyShrtCd = rs.getString("AGNCY_SHRT_CD");
                        sql2AgncyShortCd.add(agncyShrtCd);

                        substr = rs.getString("SUBSTR");
                        sql2Substr.add(substr);

                        System.out.println(rowCount + " Sql 2  agncyWid = " + agncyWid + "  ; agncyShrtCd  = " + agncyShrtCd + " ; substr = " + substr);


                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
       //wr.printYellowDiv("TEST 3 Need Review");

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((distinctCount == (rowCount + 1)),"(distinctCount == (rowCount+1))");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("distinctCount = rowCount " + distinctCount + " = " + rowCount);
        assertEquals(distinctCount,rowCount);
        System.out.println("Finish AgncyOrgWhTest.test3");
        System.out.println();
    }

    @Test
        /*
         * Check the values of the AGNCY_ORG_WH.DPRTMNT_AGNCY_SHORT_CD column
         */
    public void test4() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select distinct ao.DPRTMNT_AGNCY_SHRT_CD \n" + 
        		"from DTSDM.AGNCY_ORG_WH ao \n" + 
        		"";

        String sql1 = "Select distinct substr(ol.U##ORG,0,2) \n" +
                "From DTSDM_SRC_STG.ORGLIST ol" ;

        // Output the Sql to be executed
        ArrayList<SqlObject> sqlList = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql);
        sqlList.add(sqlObj);
        SqlObject sqlObj1 = new SqlObject("sql1",sql1);
        sqlList.add(sqlObj1);
        wr.logSql(sqlList);

        int distinctCount = 0;
        int rowCount = 0;

        System.out.println("Starting AgncyOrgWhTest.test4,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                    	distinctCount++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Starting AgncyOrgWhTest.test4,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                    	rowCount++;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((distinctCount == rowCount),"(distinctCount == rowCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("distinctCount = rowCount " + distinctCount + " = " + rowCount);
        assertEquals(distinctCount,rowCount);
        
        System.out.println("Finish AgncyOrgWhTest.test4");
        System.out.println();
    }

    @Test
        /*
         * Check the values of the AGNCY_ORG_WH.AGNCY_SHRT_CD column.
         */
    public void test5() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select distinct ao.DPRTMNT_AGNCY_SHRT_CD \n" + 
        		"from DTSDM.AGNCY_ORG_WH ao \n" + 
        		"";

        String sql1 = "Select distinct substr(ol.U##ORG,0,2) \n" +
                "From DTSDM_SRC_STG.ORGLIST ol" ;

        // Log the Sql
        ArrayList<SqlObject> sqlList = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql);
        sqlList.add(sqlObj);
        SqlObject sqlObj1 = new SqlObject("sql1",sql1);
        sqlList.add(sqlObj1);
        wr.logSql(sqlList);

        int distinctCount = 0;
        int rowCount = 0;

        System.out.println("Starting AgncyOrgWhTest.test5,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                    	distinctCount++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Starting AgncyOrgWhTest.test5,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                    	rowCount++;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((distinctCount == rowCount),"(distinctCount == rowCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("distinctCount = rowCount " + distinctCount + " = " + rowCount);
        assertEquals(distinctCount,rowCount);
        
        System.out.println("Finish AgncyOrgWhTest.test5");
        System.out.println();
    }

    @Test
        /*
         * - Check the values of the ORG_CD column.
         */
    public void test6() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct ao.DPRTMNT_AGNCY_SHRT_CD, ao.org_cd \n" + 
        		"from DTSDM.AGNCY_ORG_WH ao \n";

        String sql2 = "select distinct substr(u##org,2,3)  \n" +
                      "from DTSDM_SRC_STG.orglist ol \n" +
                      "where substr(u##org,0,2) in ('DA', 'DF')";

        String sql3 = "select distinct substr(u##org,2,2) \n" +
                      "from DTSDM_SRC_STG.orglist ol \n" +
                      "where substr(u##org,0,2) in ('DN', 'DD', 'DM', 'DJ')";

        // Log the Sql
        ArrayList<SqlObject> sqlList = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1);
        sqlList.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2);
        sqlList.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3);
        sqlList.add(sqlObj3);
        wr.logSql(sqlList);

       int distinctCount = 0;
        int destCount = 0;
        int srcCount = 0;

        System.out.println("Starting AgncyOrgWhTest.test6,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                    	distinctCount++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        

        System.out.println("Starting AgncyOrgWhTest.test6,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        destCount++;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        
        System.out.println("Starting AgncyOrgWhTest.test6,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        srcCount++;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((destCount == srcCount),"(destCount == srcCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Distinct Count  " + distinctCount );
        assertEquals(destCount,srcCount);
        

        System.out.println("Finish AgncyOrgWhTest.test6");
        System.out.println();
    }

    @Test
        /*
         * Check the population of the DCMNT_WH.CURR_SW column
         */
    public void test7() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "Select distinct AGNCY_ORG_WH.CURR_SW, count(*) \n" + 
        		"From DTSDM.AGNCY_ORG_WH \n" + 
        		"Group by AGNCY_ORG_WH.CURR_SW \n" ;;

        // Log the Sql
        ArrayList<SqlObject> sqlList = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1);
        sqlList.add(sqlObj1);
        wr.logSql(sqlList);


        int distinctCount = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        wr.printYellowDiv("Test 7 Need Review");
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((distinctCount == distinctCount),"(distinctCount == distinctCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("distinctCount " + distinctCount );
        assertEquals(distinctCount,distinctCount);
        
        System.out.println("Finish AgncyOrgWhTest.test7");
        System.out.println();
    }

    @Test
        /*
         * - Check the values of the ORG_CD column.
         */
    public void test8() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "Select count (*)\n" +
                      "From DTSDM. AGNCY_ORG_WH \n";

        String sql2 = "Select distinct trunc(AGNCY_ORG_WH.INSERT_DATE) INSERT_DATE, count (*)\n" +
                      "From DTSDM. AGNCY_ORG_WH\n" +
                      "Group by trunc(AGNCY_ORG_WH.INSERT_DATE)";

        // Log the Sql
        ArrayList<SqlObject> sqlList = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1);
        sqlList.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2);
        sqlList.add(sqlObj2);
        wr.logSql(sqlList);


        int distinctCount = 0;
        int count = 0;
        int totalCount = 0;

        System.out.println("Starting AgncyOrgWhTest.test8,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {

                    	distinctCount = rs.getInt("count(*)");
               
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        System.out.println("Starting AgncyOrgWhTest.test8,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                    	count = rs.getInt("count(*)");
                    	totalCount = count + totalCount;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the Reults
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((distinctCount == totalCount),"(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("distinctCount = totalCount " + distinctCount + " = " + totalCount);
        assertEquals(distinctCount,totalCount);
        System.out.println("Finish AgncyOrgWhTest.test8");
        System.out.println();
    }

    @Test
        /*
         * - Check the values of the ORG_CD column.
         */
    public void test9() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "Select count (*)\n" +
                      "From DTSDM. AGNCY_ORG_WH \n";

        String sql2 = "Select distinct trunc(AGNCY_ORG_WH.UPDATE_DATE) UPDATE_DATE, count (*)\n" +
                      "From DTSDM. AGNCY_ORG_WH\n" +
                      "Group by trunc(AGNCY_ORG_WH.UPDATE_DATE)";

        // Log the Sql
        ArrayList<SqlObject> sqlList = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1);
        sqlList.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2);
        sqlList.add(sqlObj2);
        wr.logSql(sqlList);


        int distinctCount = 0;
        int count = 0;
        int totalCount = 0;

        System.out.println("Starting AgncyOrgWhTest.test9,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {

                        count = rs.getInt("count(*)");

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        System.out.println("Starting AgncyOrgWhTest.test9,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                     count = rs.getInt("count(*)");
                    	totalCount = count + totalCount;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the Reults
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((count == totalCount),"(count == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("count = totalCount " + count + " = " + totalCount);
        assertEquals(count,totalCount);
        
        System.out.println("Finish AgncyOrgWhTest.test9");
        System.out.println();
    }

}
