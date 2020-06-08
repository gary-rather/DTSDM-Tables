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
public class ExpnsSmryWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("ExpnsSmryWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        //check that the unknown record 0 is populated
        //EXPECT: Unspecified data row: Initial load must add the 'Unspecified' row to the table.
        //
        //      EXPNS_SMRY_WID = 0; EXPNS_TYPE_WID = 0; ORG_ACCNT_WID = 0; DCMNT_WID = 0; EXPNS_CTGRY_TYPE_WID = 0;
        //      SDN_TYPE_CODE = 'Unspecified'; others NULL.
        //      As of 05/20 SDN_TYPE_CODE = ‘UNKNOWN’, SRC_VCHNUM = ‘UNKNOWN’ and SRC_EXPENSE_TYPE = ‘UNKNOWN

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select * from DTSDM.EXPNS_SMRY_WH where EXPNS_SMRY_WH.EXPNS_SMRY_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting ExpnsSmryWhTest.test1,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        number++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro = new ResultObject((number == 1), "(number == 1)");
        roList.add(ro);
        wr.logTestResults(roList);

        System.out.println("Test ExpnsSmryWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish ExpnsSmryWhTest.test1");

    }

    @Test
    public void test02() {

        //Check the population of the unique identifier (EXPNS_SMRY_WH.EXPNS_SMRY_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct EXPNS_SMRY_WH. EXPNS_SMRY_WID, count(*)\n from DTSDM.EXPNS_SMRY_WH\n" +
                "group by EXPNS_SMRY_WH. EXPNS_SMRY_WID having count(*) > 1";

        String sql2 = "select count (distinct EXPNS_SMRY_WH. EXPNS_SMRY_WID) from DTSDM.EXPNS_SMRY_WH";
        String sql3 = "select count(*) from DTSDM.EXPNS_SMRY_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int count = 0;
        int distinctCount = 0;
        int duplicateCount = 0;

        System.out.println("Starting ExpnsSmryTicketWhTest.test2,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        duplicateCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test2,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test2,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test2 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((distinctCount == count), "(distinctCount == count)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((duplicateCount == 0), "(duplicateCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 2: Count = " + count);
        System.out.println("Test 2: Distinct Count = " + distinctCount);
        System.out.println("Test 2: Duplicate Count =  " + duplicateCount);

        assertEquals(count, distinctCount);
        assertEquals(0, duplicateCount);

        System.out.println("Finish ExpnsSmryWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the population of the EXPNS_SMRY_WH.EXPNS_TYPE_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.EXPNS_SMRY_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t select es.expns_type_wid as etl_expns_type_wid, \n" +
                        "\t\t\t tcr.type_wid as test_expns_type_wid \n" +
                        "\n" +
                        "\t from dtsdm.expns_smry_wh es, dtsdm.dcmnt_wh dc, \n" +
                        "\t\t\t dtsdm.type_consoldtd_rfrnc_wh tcr, \n" +
                        "\t\t\t fred.lodge l, fred.acctotals at \n" +
                        "\n" +
                        "\t where es.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and dc.dcmnt_type_wid = tcr.type_wid \n" +
                        "\t and dc.dcmnt_name = l.u##vchnum \n" +
                        "\t and dc.src_doctype = l.u##doctype \n" +
                        "\t and dc.src_ssn = l.u##ssn \n" +
                        "\t and l.u##vchnum = at.u##vchnum \n" +
                        "\t and l.u##doctype = at.u##doctype \n" +
                        "\t and l.u##ssn = at.u##ssn \n" +
                        "\t and tcr.type_cd = upper(l.expenses) \n" +
                        "\t and upper(l.expenses) in ('HOTEL TAX%', 'GAS%') \n" +
                        "\t and tcr.type_cd = at.u##sublabel \n" +
                        "\t and at.u##sublabel in ('LODGING', 'COM. CARR.-I', \n" +
                                                "'RENTAL CAR', 'M'||'&'||'IE', 'OTHER') \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test03,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test03,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test03 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 03: Comparison Count = " + comparisonCount);
        System.out.println("Test 03: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04(){

        // Check the population of the EXPNS_SMRY_WH.ORG_ACCNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.EXPNS_SMRY_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t select es.org_accnt_wid as etl_org_accnt_wid, \n" +
                        "\t\t\t oa.org_accnt_wid as test_org_accnt_wid \n" +
                        "\n" +
                        "\t from dtsdm.expns_smry_wh es, dtsdm.dcmnt_wh dc, \n" +
                        "\t\t\t dtsdm.org_accnt_wh oa, fred.acctotals a \n" +
                        "\n" +
                        "\t where es.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and a.acclabel = oa.accnt_label \n" +
                        "\t and upper(a.org) = oa.full_org_cd \n" +
                        "\t and dc.dcmnt_name = a.u##vchnum \n" +
                        "\t and dc.src_doctype = a.u##doctype \n" +
                        "\t and dc.src_ssn = a.u##ssn \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test04,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test04 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test04,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test04 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 04: Comparison Count = " + comparisonCount);
        System.out.println("Test 04: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05(){

        // Check the population of the EXPNS_SMRY_WH.DCMNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.EXPNS_SMRY_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t select es.dcmnt_wid as etl_dcmnt_wid, dc.dcmnt_wid as test_dcmnt_wid \n" +
                        "\n" +
                        "\t from dtsdm.expns_smry_wh es, dtsdm.dcmnt_wh dc, fred.acctsex ats \n" +
                        "\n" +
                        "\t where es.src_vchnum = dc.dcmnt_name \n" +
                        "\t and es.src_ssn = dc.src_ssn \n" +
                        "\t and es.src_doctype = dc.src_doctype \n" +
                        "\t and dc.dcmnt_name = ats.u##vchnum \n" +
                        "\t and dc.src_doctype = ats.u##doctype \n" +
                        "\t and dc.src_ssn = ats.u##ssn \n" +
                        "\t and dc.adjstmt_lvl = ats.adj_level \n" +
                        "\t and ats.adj_level = 0 \n" +
                        "\t and es.dcmnt_wid = dc.dcmnt_wid \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test05,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test05 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test05,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test05 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 05: Comparison Count = " + comparisonCount);
        System.out.println("Test 05: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06(){

        // Check the population of the EXPNS_SMRY_WH.SDN column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.EXPNS_SMRY_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "select es.sdn as etl_sdn, ats.acc2 as test_sdn \n" +
                        "\n" +
                        "from dtsdm.expns_smry_wh es, fred.acctsex ats \n" +
                        "\n" +
                        "where es.src_vchnum = ats.u##vchnum \n" +
                        "and es.src_ssn = ats.u##ssn \n" +
                        "and es.src_doctype = ats.u##doctype \n" +
                        "and es.sdn = ats.acc2 \n" +
                        "and es.sdn != ' ' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test06,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test06 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test06,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test06 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 06: Comparison Count = " + comparisonCount);
        System.out.println("Test 06: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test06");
        System.out.println();

    }

    @Ignore
    @Test
    public void test07(){
        // no test right now (need more information)
    }

    @Test
    public void test08() {

        // Check the population of the EXPNS_SMRY_WH.NOREIMB_COST_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select es.nonreimb_cost_amt as etl_nonreimb_cost_amt, \n" +
                        "\t\t\t a.noreimb_cost as test_nonreimb_cost_amt \n" +
                        "\n" +
                        "\t from dtsdm.expns_smry_wh es, fred.acctotals a \n" +
                        "\n" +
                        "\t where es.src_vchnum = a.u##vchnum \n" +
                        "\t and es.src_doctype = a.u##doctype \n" +
                        "\t and es.src_ssn = a.u##ssn \n" +
                        "\t and es.nonreimb_cost_amt !=0 \n" +
                        "\t and es.nonreimb_cost_amt != a.noreimb_cost \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test08,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test08 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 08: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test08");
        System.out.println();

    }

    @Ignore
    @Test
    public void test09(){
        // no test right now (need more information)
    }

    @Ignore
    @Test
    public void test10(){
        // no test right now (column not in table)
    }

    @Ignore
    @Test
    public void test11(){
        // no test right now (column not in table)
    }

    @Ignore
    @Test
    public void test12(){
        // no test right now (column not in table)
    }

    @Ignore
    @Test
    public void test13(){
        // no test right now (column not in table)
    }

    @Ignore
    @Test
    public void test14(){
        // no test right now (column not in table)
    }

    @Test
    public void test15() {

        // Check the population of the EXPNS_SMRY_WH.SRC_VCHNUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select es.src_vchnum as etl_src_vchnum, l.u##vchnum as test_src_vchnum \n" +
                        "\n" +
                        "\t from dtsdm.expns_smry_wh es, dtsdm.dcmnt_wh dc, fred.lodge l, fred.acctotals a \n" +
                        "\n" +
                        "\t where es.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and dc.dcmnt_name = l.u##vchnum \n" +
                        "\t and dc.src_doctype = l.u##doctype \n" +
                        "\t and dc.src_ssn = l.u##ssn \n" +
                        "\t and l.u##vchnum = a.u##vchnum \n" +
                        "\t and l.u##doctype = a.u##doctype \n" +
                        "\t and l.u##ssn = a.u##ssn \n" +
                        "\t and es.src_vchnum != l.u##vchnum \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test15,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test15 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 15: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test15");
        System.out.println();

    }

    @Test
    public void test16() {

        // Check the population of the EXPNS_SMRY_WH.SRC_DOCTYPE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                "( \n" +
                "\t select es.src_doctype as etl_src_doctype, l.u##doctype as test_src_doctype \n" +
                "\n" +
                "\t from dtsdm.expns_smry_wh es, dtsdm.dcmnt_wh dc, fred.lodge l, fred.acctotals at \n" +
                "\n" +
                "\t where es.dcmnt_wid = dc.dcmnt_wid \n" +
                "\t and dc.dcmnt_name = l.u##vchnum \n" +
                "\t and dc.src_doctype = l.u##doctype \n" +
                "\t and dc.src_ssn = l.u##ssn \n" +
                "\t and l.u##vchnum = at.u##vchnum \n" +
                "\t and l.u##doctype = at.u##doctype \n" +
                "\t and l.u##ssn = at.u##ssn \n" +
                "\t and es.src_doctype != l.u##doctype \n" +
                ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test16,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test16 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 16: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test16");
        System.out.println();

    }

    @Test
    public void test17() {

        // Check the population of the EXPNS_SMRY_WH.SRC_SSN column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select es.src_ssn as etl_src_doctype, l.u##ssn as test_src_doctype \n" +
                        "\n" +
                        "\t from dtsdm.expns_smry_wh es, dtsdm.dcmnt_wh dc, fred.lodge l, fred.acctotals a \n" +
                        "\n" +
                        "\t where es.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and dc.dcmnt_name = l.u##vchnum \n" +
                        "\t and dc.src_doctype = l.u##doctype \n" +
                        "\t and dc.src_ssn = l.u##ssn \n" +
                        "\t and l.u##vchnum = a.u##vchnum \n" +
                        "\t and l.u##doctype = a.u##doctype \n" +
                        "\t and l.u##ssn = a.u##ssn \n" +
                        "\t and es.src_ssn != l.u##ssn \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test17,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test17 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 17: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test17");
        System.out.println();

    }

    @Test
    public void test18(){

        // Check the population of the EXPNS_SMRY_WH.SRC_EXPNS_TYPE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.EXPNS_SMRY_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t select es.src_expense_type as etl_src_expense_type, \n" +
                        "\t\t\t a.u##sublabel as test_src_expense_type \n" +
                        "\n" +
                        "\t from dtsdm.expns_smry_wh es, dtsdm.dcmnt_wh dc, \n" +
                        "\t\t\t fred.lodge l, fred.acctotals a \n" +
                        "\n" +
                        "\t where es.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and dc.dcmnt_name = l.u##vchnum \n" +
                        "\t and dc.src_doctype = l.u##doctype \n" +
                        "\t and dc.src_ssn = l.u##ssn \n" +
                        "\t and l.u##vchnum = a.u##vchnum \n" +
                        "\t and l.u##doctype = a.u##doctype \n" +
                        "\t and l.u##ssn = a.u##ssn \n" +
                        "\t and a.u##sublabel = upper(expenses) \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test18,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test18 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test18,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test18 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 18: Comparison Count = " + comparisonCount);
        System.out.println("Test 18: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test18");
        System.out.println();

    }

    @Test
    public void test19() {

        // Check the population of the EXPNS_SMRY_WH.CURR_SW column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.EXPNS_SMRY_WH";
        String sql2 = "select EXPNS_SMRY_WH.CURR_SW from DTSDM.EXPNS_SMRY_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test19,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test19 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test19,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test19 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 19: Comparison Count = " + comparisonCount);
        System.out.println("Test 19: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test19");
        System.out.println();

    }

    @Ignore
    @Test
    public void test20() {

        // Check the population of the EXPNS_SMRY_WH.EFF_START_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.EXPNS_SMRY_WH";
        String sql2 = "select trunc(EXPNS_SMRY_WH.EFF_START_DT) from DTSDM.EXPNS_SMRY_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test20,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test20 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test20,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test20 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 20: Comparison Count = " + comparisonCount);
        System.out.println("Test 20: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test20");
        System.out.println();

    }

    @Ignore
    @Test
    public void test21() {

        // Check the population of the EXPNS_SMRY_WH.EFF_END_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.EXPNS_SMRY_WH";
        String sql2 = "select EXPNS_SMRY_WH.EFF_END_DT from DTSDM.EXPNS_SMRY_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test21,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test21 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test21,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("ExpnsSmryWh.test21 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 21: Comparison Count = " + comparisonCount);
        System.out.println("Test 21: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test21");
        System.out.println();

    }

}