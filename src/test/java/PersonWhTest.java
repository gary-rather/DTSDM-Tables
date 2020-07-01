import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
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
public class PersonWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("PersonWhTest.html");
        wr.pageHeader();
    }

    @Test
    /*
     */
    public void test01() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check that the \"unknown\" record 0 is populated";
        String reason = " Initial load must add  unspecified data row.  Provides the ability to traverse through the DTSDM. PERSON_WH  table when no value is matched";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

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

        System.out.println("Finish PersonWh.test1");
    }

    @Test
    /*
     */
    public void test02() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the PERSON_WH.PERSON_WID  (PK) column";
        String reason = " Sequential ID (PK)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

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

        System.out.println("Finish PersonWh.test2");
    }

    @Test
    /*
     */
    public void test03() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the PERSON_WH.SUBORG_WID column";
        String reason = " FK to SUBORG_WH; lookup on SUBORG_WH.FULL_ORG_CD";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

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
        System.out.println("Finish PersonWh.test3");
    }

    @Test
    /*
     */
    public void test04() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the PERSON_WH.SSN_FULL column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

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

        System.out.println("Finish PersonWh.test4");
    }

    @Test
    /*
     */
    public void test05() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the all other columns in PERSON_WH";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "SELECT\n" +
                "  DISTINCT\n" +
                "  PERSON.\"U##SSN\" AS U##SSN ,\n" +
                "  trgt.ssn_full trgt_ssn_full,\n" +
                "  last_value(PERSON.\"U##LNAME\") over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS U##LNAME ,\n" +
                "trgt.lname as trgt_lname,\n" +
                "  last_value(PERSON.\"U##FNAME\") over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS U##FNAME ,\n" +
                "trgt.fname as trgt_fname,\n" +
                "  last_Value(PERSON.\"U##MNAME\") over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS U##MNAME ,\n" +
                "trgt.minit as trgt_minit,\n" +
                "   last_value(PERSON.PHONE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS PHONE ,\n" +
                "trgt.home_phone_num as trgt_home_phone_num,\n" +
                "  last_value(PERSON.MOBILE_PHONE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MOBILE_PHONE ,\n" +
                "trgt.mobile_phone_num as trgt_mobile_phone_num,\n" +
                "  last_value(PERSON.OFF_PHONE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS OFF_PHONE ,\n" +
                "trgt.off_phone_num as trgt_off_phone_num,\n" +
                "  last_value(PERSON.OFF_FAX) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS OFF_FAX ,\n" +
                "trgt.off_fax_num as trgt_off_fax_num,\n" +
                "  last_value(PERSON.RES_FAX)over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS RES_FAX ,\n" +
                "trgt.rsdntl_fax_num as trgt_rsdntl_fax_num,\n" +
                "  last_value(PERSON.EMAIL_ADDR) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS EMAIL_ADDR ,\n" +
                "trgt.email_addr as trgt_email_addr,\n" +
                "  last_value(PERSON.CSA_FLAG) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS CSA_FLAG ,\n" +
                "trgt.csa_flag as trgt_csa_flag,\n" +
                "  last_value(PERSON.USER_DATA2) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS AIR_CREW_STATUS ,\n" +
                "trgt.air_crew_status as trgt_air_crew_status,\n" +
                "  last_value(PERSON.EM_NAME) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS EM_NAME ,\n" +
                "trgt.emgncy_cntct_name as trgt_emgncy_cntct_name,\n" +
                "  last_value(PERSON.EM_PHONE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS EM_PHONE ,\n" +
                "trgt.emgncy_cntct_phone_num as trgt_emgncy_cntct_phone_num,\n" +
                "  last_value(PERSON.CORRES_CODE) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MAIL_CD ,\n" +
                "trgt.mail_cd as trgt_mail_cd,\n" +
                "  last_value(PERSON.PROFNAME) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ROUTING_LIST_NAME ,\n" +
                "trgt.routng_list_name as trgt_routng_list_name,\n" +
                "  last_value(PERSON.USER_DATA1) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS TECH_STATUS ,\n" +
                "trgt.tech_status as trgt_tech_status,\n" +
                "  last_value(PERSON.WHOURS) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS WORK_HRS_QTY ,\n" +
                "trgt.work_hrs_qty as trgt_work_hrs_qty,\n" +
                "  last_value(PERSON.UNIT_ASSIGN) over\n" +
                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS UNIT_ASSIGN ,\n" +
                "trgt.assgnd_unit_descr as trgt_assgnd_unit_descr\n" +
                "\n" +
//               "/*\n" +
//                "\n" +
//                "last_value(DOCSTAT.CREATE_DATE) over\n" +
//                "\n" +
//                "(partition by PERSON.\"U##SSN\" order by DOCSTAT.CREATE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LAST_CREATE_DATE\n" +
//                "\n" +
//                "*/\n" +
//                "\n" +
                "FROM\n" +
                "  DTSDM_SRC_STG.PERSON PERSON \n" +
                "    left outer join dtsdm.person_wh trgt\n" +
                "        on trgt.ssn_full = person.u##ssn,\n" +
                "DTSDM_SRC_STG.DOCSTAT DOCSTAT \n" +
                "where  PERSON.\"U##SSN\" = DOCSTAT.\"U##SSN\"\n" +
                "and PERSON.\"U##VCHNUM\" = DOCSTAT.\"U##VCHNUM\"\n" +
                "and PERSON.\"U##DOCTYPE\" = DOCSTAT.\"U##DOCTYPE\"\n" +
                "and PERSON.ADJ_LEVEL = DOCSTAT.ADJ_LEVEL\n" +
                "order by 1\n" +
                "\n" +
                "fetch first 10000 rows only";

        int rowComparePass = 0;
        int rowCompareFail = 0;
        List<String> failedPersonSsn = new ArrayList<>();
        int rowCount = 0;

        System.out.println("Starting PersonWh.test5,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    ResultSetMetaData rsmd=rs.getMetaData();
                    int numberColumns = rsmd.getColumnCount();

                    // Loop through the results
                    while (rs.next()) {
                        int c = 1;
                        Boolean rowStatus = true;
                        rowCount++;

                        // Loop through each two columns
                        // Compare the colulns
                        while (c < (numberColumns)) {

                            // Get the next two coulmns
                            Object o1 = rs.getObject(c++);
                            Object o2 = rs.getObject(c++);

                            Boolean columnCheck = false;
                            // Check the two columns
                            if (o1 != null) {
                                if (o1 instanceof String) {
                                    columnCheck = ((String) o1).equalsIgnoreCase((String) o2);
                                    if (!columnCheck) {
                                        rowStatus = false;
                                    }

                                }
                                if (o1 instanceof Integer) {
                                    columnCheck = ((Integer) o1).intValue() == ((Integer) o2).intValue();
                                    if (!columnCheck) {
                                        rowStatus = false;
                                    }

                                }
                                if (o1 instanceof BigDecimal) {
                                    BigDecimal bd1 = (BigDecimal) o1;
                                    BigDecimal bd2 = (BigDecimal) o2;
                                    int res = bd1.compareTo(bd2);
                                    if (res == 0 ) columnCheck = true;
                                    if (!columnCheck) {
                                        rowStatus = false;
                                    }

                                }
                             } else {
                                columnCheck = (o1 == null) && (o2 == null);
                                if (!columnCheck)  {
                                    rowStatus = false;
                                }
                            }
                        }
                        // Check to see if any column failed. If one fails the row fails
                        if (rowStatus) {
                            rowComparePass++;
                        }
                        else {
                            rowCompareFail++;
                        }
                     }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Row Read " + rowCount);
        System.out.println("Rows Compare Success = " + rowComparePass);
        System.out.println("Rows Compare Failed = " + rowCompareFail);

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject(((rowComparePass )== rowCount), "(successRowComapare == rowCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        assertEquals(rowComparePass, rowCount);

        System.out.println("Finish PersonWh.test5");
    }

    @Test
    /**
     * --PERSON_WH ROW COUNT
     */
    public void test06() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "select src.src_cnt - trgt.trgt_cnt rcd_cnt_discrepancy\n" +
                "from\n" +
                "    (\n" +
                "        select count(distinct u##ssn) src_cnt\n" +
                "        from dtsdm_src_stg.person\n" +
                "    ) src,\n" +
                "    (\n" +
                "        select count(*) trgt_cnt\n" +
                "        from dtsdm.person_wh\n" +
                "        where person_wid != 0\n" +
                "    ) trgt\n";

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
        ResultObject ro1 = new ResultObject((1 == number),"(0 == number)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test PersonWh  0 == " + number);
        assertEquals(0, number);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test06");
        System.out.println();
    }

    @Test
    /**
     * --Identify missing data
     */
    public void test07() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "select a.*\n" +
                "from dtsdm_src_stg.person a\n" +
                "where not exists\n" +
                "    (\n" +
                "        select b.person_wid\n" +
                "        from dtsdm.person_wh b\n" +
                "        where b.ssn_full = a.u##ssn\n" +
                "    )\n" +
                "order by a.u##ssn";

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
                        String ssn = rs.getString("ssn");
                        number++;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((1 == number),"(0 == number)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test PersonWh 0 == " + number);
        assertEquals(0, number);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test07");
        System.out.println();
    }

}