import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PersonWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("PersonWhTest.html");
        wr.pageHeader();
    }

    @Test
    /*
     */
    public void test1() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment("Check that the \"unknown\" record 0 is populated.");

        String sql1 = "Select count(*) from DTSDM.PERSON_WH \n" +
                "where PERSON_WH.PERSON_WID = 0";

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

        System.out.println("Test assert  (1 == row0Count) " + row0Count );
        assertEquals(1, row0Count);

        System.out.println("Test PersonWHTest Success " );
    }

    @Test
    /*
     */
    public void test2() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment("- Check the population of the PERSON_WH.PERSON_WID (PK) column");

        String sql1 = "Select count (distinct PERSON_WH.PERSON_WID) \n" +
                "from DTSDM. PERSON_WH\n";

        String sql2 = "Select count(*) \n" +
                "from DTSDM.PERSON_WH";

        String sql3 = "select distinct PERSON_WH.PERSON_WID, count(*) \n" +
                "from DTSDM. PERSON_WH \n" +
                "group by PERSON_WH.PERSON_WID \n" +
                "having count(*) > 1                                                                \n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
        wr.logSql(theSql);

        int sql1Count = 0;
        int sql2Count = 0;
        int sql3Count = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                       sql1Count = rs.getInt(1);

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
                        sql2Count = rs.getInt(1);

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
                        String  wid = rs.getString(1);
                        int  count = rs.getInt(2);
                        sql3Count++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((sql1Count == sql2Count), "(sql1Count == sql2Count)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((0 == sql3Count), "(0 == sql3Count)");
        roList.add(ro2);
        wr.logTestResults(roList);

        assertEquals(sql1Count ,sql2Count);
        assertEquals(0 ,sql3Count);

        System.out.println("Test PersonWHTest Success " + "  count = " + 1);
    }

    @Test
    /*
     */
    public void test3() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment("Check the population of the PERSON_WH.SUBORG_WID column. ");

        wr.printYellowDiv("SKIPPING QUERY PERSON_WH.SUBORG_WID doesn’t exist in the table as of 08/19/19");
        String sql1 = "Select p.suborg_wid as etl_suborg_wid, s.suborg_wid as test_suborg wid \n" +
                "From dtsdm.person_wh p, dtsdm.suborg_wh s, DTSDM_SRC_STG.person fp \n" +
                "Where fp.org = s.full_org_cd \n" +
                "And p.ssn_full = fp.u##ssn";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        wr.logSql(theSql);

/*
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {


                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((2 == 2), "(2 == 2)");
        roList.add(ro1);
        wr.logTestResults(roList);

        assertEquals(1, 1);
*/
        System.out.println("Test PersonWHTest Success " + "  count = " + 1);
    }

    @Test
    /*
     */
    public void test4() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment("Check the population of the PERSON_WH.SSN_FULL column");

        String sql1 = "select distinct ssn_full, count(*) \n" +
                "from dtsdm.person_wh \n" +
                "group by ssn_full  \n";

        String sql2 = "select distinct PERSON.U##SSN --, count(*) \n" +
                "From DTSDM_SRC_STG.PERSON,DTSDM_SRC_STG.DOCSTAT \n" +
                "where PERSON.U##SSN = DOCSTAT.U##SSN \n" +
                "and PERSON.U##VCHNUM = DOCSTAT.U##VCHNUM \n" +
                "and PERSON.U##DOCTYPE = DOCSTAT.U##DOCTYPE \n" +
                "and PERSON.ADJ_LEVEL = DOCSTAT.ADJ_LEVEL \n" +
                "group by PERSON.U##SSN \n  \n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int destCount = 0;
        int srcCount = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    int rowCountSql1 = 0;
                    while (rs.next()) {
                        String ssn_full = rs.getString(1);
                        String singleCount = rs.getString(2);
                        destCount++;

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
                    int rowCountSql2 = 0;
                    while (rs.next()) {
                        String srcSSN = rs.getString(1);
                        srcCount++;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject(((srcCount + 1) == destCount), "((srcCount+1) == destCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("PersonWh.Test4 (srcCount + 1) == destCount " + (srcCount + 1)+ " == " + destCount);
        assertEquals(srcCount+1, destCount);

        System.out.println("Test PersonWHTest Success ");
    }

    @Test
    /*
     */
    public void test5() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "SELECT \n" +
                "  DISTINCT \n" +
                "  PERSON.\"U##SSN\" AS U##SSN ,\n" +
                "  last_value(PERSON.\"U##LNAME\") over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS U##LNAME ,\n" +
                "  last_value(PERSON.\"U##FNAME\") over \n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS U##FNAME ,\n" +
                "  last_Value(PERSON.\"U##MNAME\") over \n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS U##MNAME ,\n" +
                "   last_value(PERSON.PHONE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS PHONE ,\n" +
                "  last_value(PERSON.MOBILE_PHONE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MOBILE_PHONE ,\n" +
                "  last_value(PERSON.OFF_PHONE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS OFF_PHONE ,\n" +
                "  last_value(PERSON.OFF_FAX) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS OFF_FAX ,\n" +
                "  last_value(PERSON.RES_FAX)over \n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS RES_FAX ,\n" +
                "  last_value(PERSON.EMAIL_ADDR) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS EMAIL_ADDR ,\n" +
                "  last_value(PERSON.CSA_FLAG) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS CSA_FLAG ,\n" +
                "  last_value(PERSON.USER_DATA2) over \n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS AIR_CREW_STATUS ,\n" +
                "  last_value(PERSON.EM_NAME) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS EM_NAME ,\n" +
                "  last_value(PERSON.EM_PHONE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS EM_PHONE ,\n" +
                "  last_value(PERSON.CORRES_CODE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MAIL_CD ,\n" +
                "  last_value(PERSON.PROFNAME) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ROUTING_LIST_NAME ,\n" +
                "  last_value(PERSON.USER_DATA1) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS TECH_STATUS ,\n" +
                "  last_value(PERSON.WHOURS) over \n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS WORK_HRS_QTY ,\n" +
                "  last_value(PERSON.UNIT_ASSIGN) over \n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS UNIT_ASSIGN ,\n" +
                " last_value(DOCSTAT.CREATE_DATE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LAST_CREATE_DATE   \n" +
                "FROM \n" +
                "  DTSDM_SRC_STG.PERSON PERSON  INNER JOIN  DTSDM_SRC_STG.DOCSTAT DOCSTAT  \n" +
                "    ON  PERSON.\"U##SSN\" = DOCSTAT.\"U##SSN\"\n" +
                "and PERSON.\"U##VCHNUM\" = DOCSTAT.\"U##VCHNUM\"\n" +
                "and PERSON.\"U##DOCTYPE\" = DOCSTAT.\"U##DOCTYPE\"\n" +
                "and PERSON.ADJ_LEVEL = DOCSTAT.ADJ_LEVEL";

        String sql2 = "select * from person_wh";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        wr.logSql(theSql);

        int srcCount = 0;
        ArrayList<Person_src> personSrcList = new ArrayList<>();
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        Person_src personSrc = new Person_src();
                        personSrc.U_SSN = rs.getString("U##SSN");
                        personSrc.U_LNAME = rs.getString("U##LNAME");
                        personSrc.U_FNAME = rs.getString("U##FNAME");
                        personSrc.U_MNAME = rs.getString("U##MNAME");
                        personSrc.PHONE = rs.getString("PHONE");
                        personSrc.MOBILE_PHONE= rs.getString("MOBILE_PHONE");
                        personSrc.OFF_PHONE = rs.getString("OFF_PHONE");
                        personSrc.OFF_FAX = rs.getString("OFF_FAX");
                        personSrc.RES_FAX = rs.getString("RES_FAX");
                        personSrc.EMAIL_ADDR = rs.getString("EMAIL_ADDR");
                        personSrc.CSA_FLAG = rs.getString("CSA_FLAG");
                        personSrc.AIR_CREW_STATUS = rs.getString("AIR_CREW_STATUS");
                        personSrc.EM_NAME = rs.getString("EM_NAME");
                        personSrc.EM_PHONE = rs.getString("EM_PHONE");
                        personSrc.MAIL_CD = rs.getString("MAIL_CD");
                        personSrc.ROUTING_LIST_NAME = rs.getString("ROUTING_LIST_NAME");
                        personSrc.TECH_STATUS = rs.getString("TECH_STATUS");
                        personSrc.WORK_HRS_QTY = rs.getInt("WORK_HRS_QTY");
                        personSrc.UNIT_ASSIGN = rs.getString("UNIT_ASSIGN");
                        personSrc.LAST_CREATE_DATE = rs.getDate("LAST_CREATE_DATE");
                        personSrcList.add(personSrc);
                        srcCount++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        int destCount = 0;
        List<Person_dest> personDestList = new ArrayList<>();
        Map<String,Person_dest> personDestBySsnMap = new HashMap<>();
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {

                    while (rs.next()) {
                        Person_dest personDest = new Person_dest();
                        personDest.PERSON_WID   = rs.getInt("PERSON_WID");
                        personDest.SSN_FULL  = rs.getString("SSN_FULL");
                        personDest.SSN_PRTL  = rs.getString("SSN_PRTL");
                        personDest.FNAME  = rs.getString("FNAME");
                        personDest.MINIT   = rs.getString("MINIT");
                        personDest.LNAME  = rs.getString("LNAME");
                        personDest.HOME_PHONE_NUM   = rs.getString("HOME_PHONE_NUM");
                        personDest.MOBILE_PHONE_NUM  = rs.getString("MOBILE_PHONE_NUM");
                        personDest.OFF_PHONE_NUM  = rs.getString("OFF_PHONE_NUM");
                        personDest.OFF_FAX_NUM  = rs.getString("OFF_FAX_NUM");
                        personDest.RSDNTL_FAX_NUM   = rs.getString("RSDNTL_FAX_NUM");
                        personDest.EMAIL_ADDR   = rs.getString("EMAIL_ADDR");
                        personDest.AIR_CREW_STATUS   = rs.getString("AIR_CREW_STATUS");
                        personDest.CSA_FLAG   = rs.getString("CSA_FLAG");
                        personDest.EMGNCY_CNTCT_NAME   = rs.getString("EMGNCY_CNTCT_NAME");
                        personDest.EMGNCY_CNTCT_PHONE_NUM  = rs.getString("EMGNCY_CNTCT_PHONE_NUM");
                        personDest.MAIL_CD   = rs.getString("MAIL_CD");
                        personDest.ROUTNG_LIST_NAME   = rs.getString("ROUTNG_LIST_NAME");
                        personDest.TECH_STATUS   = rs.getString("TECH_STATUS");
                        personDest.WORK_HRS_QTY = rs.getInt("WORK_HRS_QTY");
                        personDest.ASSGND_UNIT_DESCR   = rs.getString("ASSGND_UNIT_DESCR");
                        personDest.DFLT_ORG_ACCNT_WID    = rs.getString("DFLT_ORG_ACCNT_WID");
                        personDest.CURR_SW         = rs.getInt("CURR_SW");
                        personDest.INSERT_DATE      = rs.getDate("INSERT_DATE");
                        personDest.UPDATE_DATE      = rs.getDate("UPDATE_DATE");
                        personDest.DELETED_FLAG   = rs.getString("DELETED_FLAG");
                        personDest.EFF_START_DT    = rs.getDate("EFF_START_DT");
                        personDest.EFF_END_DT        = rs.getDate("EFF_END_DT");
                        personDest.APPRV_OVERRIDE_FLG   = rs.getString("APPRV_OVERRIDE_FLG");
                        personDest.ADV_AUTHRZD_TXT    = rs.getString("ADV_AUTHRZD_TXT");
                        personDestList.add(personDest);
                        personDestBySsnMap.put(personDest.SSN_FULL,personDest);
                        destCount++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Loop through check each dest to the src
        int compared = 0;
        System.out.println("Src Size = " + srcCount);
        System.out.println("Dest Size = " + destCount);

        // Loop through using the Src (No Need to check Row )
        for(Person_src aPersonSrc: personSrcList){
            Person_dest aPersonDest = personDestBySsnMap.get(aPersonSrc.U_SSN);
            if (aPersonDest.SSN_FULL != null )
            if (!aPersonDest.SSN_FULL.equalsIgnoreCase(aPersonSrc.U_SSN)) break;

            if (aPersonDest.FNAME != null )
            if (!aPersonDest.FNAME.equalsIgnoreCase(aPersonSrc.U_FNAME)) break;

            if (aPersonDest.LNAME != null )
            if (!aPersonDest.LNAME.equalsIgnoreCase(aPersonSrc.U_LNAME)) break;

            if (aPersonDest.MINIT != null )
            if (!aPersonDest.MINIT.equalsIgnoreCase(aPersonSrc.U_MNAME)) break;

            if (aPersonDest.HOME_PHONE_NUM != null )
            if (!aPersonDest.HOME_PHONE_NUM.equalsIgnoreCase(aPersonSrc.PHONE)) break;

            if (aPersonDest.MOBILE_PHONE_NUM != null )
            if (!aPersonDest.MOBILE_PHONE_NUM.equalsIgnoreCase(aPersonSrc.MOBILE_PHONE)) break;

            if (aPersonDest.OFF_PHONE_NUM != null )
            if (!aPersonDest.OFF_PHONE_NUM.equalsIgnoreCase(aPersonSrc.OFF_PHONE)) break;

            if (aPersonDest.OFF_FAX_NUM != null )
            if (!aPersonDest.OFF_FAX_NUM.equalsIgnoreCase(aPersonSrc.OFF_FAX)) break;

            if (aPersonDest.RSDNTL_FAX_NUM != null )
            if (!aPersonDest.RSDNTL_FAX_NUM.equalsIgnoreCase(aPersonSrc.RES_FAX)) break;

            if (aPersonDest.EMAIL_ADDR != null )
            if (!aPersonDest.EMAIL_ADDR.equalsIgnoreCase(aPersonSrc.EMAIL_ADDR)) break;

            if (aPersonDest.CSA_FLAG != null)
            if (!aPersonDest.CSA_FLAG.equalsIgnoreCase(aPersonSrc.CSA_FLAG)) break;

            if (aPersonDest.AIR_CREW_STATUS != null )
            if (!aPersonDest.AIR_CREW_STATUS.equalsIgnoreCase(aPersonSrc.AIR_CREW_STATUS )) break;

            if (aPersonDest.EMGNCY_CNTCT_NAME != null )
            if (!aPersonDest.EMGNCY_CNTCT_NAME.equalsIgnoreCase(aPersonSrc.EM_NAME )) break;

            if (aPersonDest.EMGNCY_CNTCT_PHONE_NUM != null )
            if (!aPersonDest.EMGNCY_CNTCT_PHONE_NUM.equalsIgnoreCase(aPersonSrc.EM_PHONE )) break;

            if (aPersonDest.MAIL_CD != null )
            if (!aPersonDest.MAIL_CD.equalsIgnoreCase(aPersonSrc.MAIL_CD )) break;

            if (aPersonDest.ROUTNG_LIST_NAME != null )
            if (!aPersonDest.ROUTNG_LIST_NAME.equalsIgnoreCase(aPersonSrc.ROUTING_LIST_NAME )) break;

            if (aPersonDest.TECH_STATUS != null )
            if (!aPersonDest.TECH_STATUS.equalsIgnoreCase(aPersonSrc.TECH_STATUS)) break;

            if ( aPersonDest.WORK_HRS_QTY != aPersonSrc.WORK_HRS_QTY ) break;

            if (aPersonDest.ASSGND_UNIT_DESCR != null )
            if (!aPersonDest.ASSGND_UNIT_DESCR.equalsIgnoreCase(aPersonSrc.UNIT_ASSIGN )) break;

            compared++;

        }
        System.out.println("Test 5 compared src object  to dest object" + compared );
        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject(((srcCount + 1)== destCount), "((srcCount+1) == destCount)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((srcCount == compared), "(srcCount == compared)");
        roList.add(ro1);
        wr.logTestResults(roList);

        assertEquals((srcCount+1), destCount);
        assertEquals(srcCount, compared);



        System.out.println("Test PersonWHTest Success " + "  count = " + 1);
    }

    class Person_dest {
        int PERSON_WID   = -1;
        String SSN_FULL  = null;
        String SSN_PRTL  = null;
        String FNAME  = null;
        String MINIT   = null;
        String LNAME  = null;
        String HOME_PHONE_NUM   = null;
        String MOBILE_PHONE_NUM  = null;
        String OFF_PHONE_NUM  = null;
        String OFF_FAX_NUM  = null;
        String RSDNTL_FAX_NUM   = null;
        String EMAIL_ADDR   = null;
        String AIR_CREW_STATUS   = null;
        String CSA_FLAG   = null;
        String EMGNCY_CNTCT_NAME   = null;
        String EMGNCY_CNTCT_PHONE_NUM  = null;
        String MAIL_CD   = null;
        String ROUTNG_LIST_NAME   = null;
        String TECH_STATUS   = null;
        int WORK_HRS_QTY =  0;
        String ASSGND_UNIT_DESCR   = null;
        String DFLT_ORG_ACCNT_WID    = null;
        int CURR_SW         = 0;
        Date INSERT_DATE      = null;
        Date UPDATE_DATE      = null;
        String DELETED_FLAG   = null;
        Date EFF_START_DT    = null;
        Date EFF_END_DT        = null;
        String APPRV_OVERRIDE_FLG   = null;
        String ADV_AUTHRZD_TXT    = null;

    }

    class Person_src {
        String U_SSN = null;
        String U_LNAME = null;
        String U_FNAME = null;
        String U_MNAME = null;
        String PHONE = null;
        String MOBILE_PHONE= null;
        String OFF_PHONE = null;
        String OFF_FAX = null;
        String RES_FAX = null;
        String EMAIL_ADDR =  null;
        String CSA_FLAG = null;
        String AIR_CREW_STATUS =  null;
        String EM_NAME = null;
        String EM_PHONE = null;
        String MAIL_CD = null;
        String ROUTING_LIST_NAME =  null;
        String TECH_STATUS =  null;
        int WORK_HRS_QTY = 0;
        String UNIT_ASSIGN =  null;
        Date LAST_CREATE_DATE = null;
    }
}