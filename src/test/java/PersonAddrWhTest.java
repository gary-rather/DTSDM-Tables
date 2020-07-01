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
public class PersonAddrWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("PersonAddrWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        // Check that the unknown record 0 is populated

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql = "Select count(*) from DTSDM.PERSON_ADDR_WH where PERSON_ADDR_WID = 0 \n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting "+ this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName() + " sql" );
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
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
        ResultObject ro1 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test PersonAddrWhTest  Row 0 = " + number);
        assertEquals(1, number);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test01");
        System.out.println();

    }

    @Test
    public void test02() {

        // Check the population of the unique identifier (PERSON_ADDR_WH.PERSON_ADDR_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select count(*) from \n" +
                        "(\n" +
                        "select distinct PERSON_WID,ADDR_TYPE_DESCR, count (*) \n" +
                        "from PERSON_ADDR_WH \n" +
                        "group by PERSON_WID,ADDR_TYPE_DESCR \n" +
                        "having count(*) > 1 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);

        wr.logSql(theSql);

        int dupeCount = 0;

        System.out.println("Starting "+ this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName() + " sql1" );
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
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

        System.out.println("Test DebtWh  dupeCount= " + dupeCount);
        assertEquals(0, dupeCount);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test02");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the population of the PERSON_ADDR_WH.PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A where A.PERSON_ADDR_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.PERSON_WID, B.PERSON_WID \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, DTSDM.PERSON_WH B \n" +
                        "\t WHERE A.PERSON_WID = B.PERSON_WID \n" +
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

        System.out.println("Starting PersonAddrWhTest.test03,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.tes03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test03,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test03 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 3: Test Count = " + testCount);
        System.out.println("Test 3: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04_Mail(){

        // Check the population of the PERSON_ADDR_WH.ADDR1 column (for mailing addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'MAILING'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.ADDR1, B.ADDRESS1 \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.ADDR1 = B.ADDRESS1 \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'MAILING' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test04_Mail,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test04_Mail sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test04_Mail,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test04_Mail sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test04_Mail: Test Count = " + testCount);
        System.out.println("Test04_Mail: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test04_Mail");
        System.out.println();

    }

    @Test
    public void test05_Mail(){

        // Check the population of the PERSON_ADDR_WH.ADDR2 column (for mailing addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'MAILING'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.ADDR2, B.ADDRESS2 \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.ADDR2 = B.ADDRESS2 \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'MAILING' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test05_Mail,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test05_Mail sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test05_Mail,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test05_Mail sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test05_Mail: Test Count = " + testCount);
        System.out.println("Test05_Mail: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test05_Mail");
        System.out.println();

    }

    @Test
    public void test06_Mail(){

        // Check the population of the PERSON_ADDR_WH.CITY_NAME column (for mailing addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'MAILING' \n";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.CITY_NAME, B.CITY \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.CITY_NAME = B.CITY \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'MAILING' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test06_Mail,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test06_Mail sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test06_Mail,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test06_Mail sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test06_Mail: Test Count = " + testCount);
        System.out.println("Test06_Mail: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test06_Mail");
        System.out.println();

    }

    @Test
    public void test07_Mail(){

        // Check the population of the PERSON_ADDR_WH.STATE_COUNTRY_CD column (for mailing addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'MAILING'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.PERSON_ADDR_WID, A.STATE_COUNTRY_CD, B.STATE_COUNTRY_CD \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, DTSDM.STATE_COUNTRY_RFRNC_WH B, FRED.TPERSON C \n" +
                        "\t WHERE A.STATE_COUNTRY_WID = B.STATE_COUNTRY_WID \n" +
                        "\t AND A.STATE_COUNTRY_CD = B.STATE_COUNTRY_CD" +
                        "\t AND B.STATE_COUNTRY_CD = C.STATE \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'MAILING' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test07_Mail,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test07_Mail sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test07_Mail,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test07_Mail sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test07_Mail: Test Count = " + testCount);
        System.out.println("Test07_Mail: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test07_Mail");
        System.out.println();

    }

    @Test
    public void test08_Mail(){

        // Check the population of the PERSON_ADDR_WH.STATE_COUNTRY_WID column (for mailing addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'MAILING'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.PERSON_ADDR_WID, A.STATE_COUNTRY_WID, B.STATE_COUNTRY_WID\n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, DTSDM.STATE_COUNTRY_RFRNC_WH B, FRED.TPERSON C\n" +
                        "\t WHERE A.STATE_COUNTRY_WID = B.STATE_COUNTRY_WID\n" +
                        "\t AND A.STATE_COUNTRY_CD = B.STATE_COUNTRY_CD" +
                        "\t AND B.STATE_COUNTRY_CD = C.STATE \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'MAILING' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test08_Mail,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test08_Mail sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test08_Mail,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test08_Mail sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test08_Mail: Test Count = " + testCount);
        System.out.println("Test08_Mail: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test08_Mail");
        System.out.println();

    }

    @Test
    public void test09_Mail(){

        // Check the population of the PERSON_ADDR_WH.ZIP_CODE column (for mailing addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'MAILING'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.ZIP_CODE, B.ZIP \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.ZIP_CODE = B.ZIP \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'MAILING' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test09_Mail,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test09_Mail sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test09_Mail,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test09_Mail sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test09_Mail: Test Count = " + testCount);
        System.out.println("Test09_Mail: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test09_Mail");
        System.out.println();

    }

    @Test
    public void test10_Mail(){

        // Check the population of the PERSON_ADDR_WH.STATE_TYPE_CD column (for mailing addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'MAILING'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.STATE_TYPE_CD, B.STATE_TYPE \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.STATE_TYPE_CD = B.STATE_TYPE \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'MAILING' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test10_Mail,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test10_Mail sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test10_Mail,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test10_Mail sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test10_Mail: Test Count = " + testCount);
        System.out.println("Test10_Mail: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test10_Mail");
        System.out.println();

    }

    @Test
    public void test04_Resd(){

        // Check the population of the PERSON_ADDR_WH.ADDR1 column (for residential addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'RESIDENTIAL'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.ADDR1, B.RES_ADDR1 \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.ADDR1 = B.RES_ADDR1 \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'RESIDENTIAL' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test04_Resd,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test04_Resd sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test04_Resd,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test04_Resd sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test04_Resd: Test Count = " + testCount);
        System.out.println("Test04_Resd: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test04_Resd");
        System.out.println();

    }

    @Test
    public void test05_Resd(){

        // Check the population of the PERSON_ADDR_WH.ADDR2 column (for residential addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'RESIDENTIAL' \n";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.ADDR2, B.RES_ADDR2 \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.ADDR2 = B.RES_ADDR2 \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'RESIDENTIAL' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test05_Resd,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test05_Resd sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test05_Resd,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test05_Resd sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test05_Resd: Test Count = " + testCount);
        System.out.println("Test05_Resd: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test05_Resd");
        System.out.println();

    }

    @Test
    public void test06_Resd(){

        // Check the population of the PERSON_ADDR_WH.CITY_NAME column (for residential addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'RESIDENTIAL'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.CITY_NAME, B.RCITY \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.CITY_NAME = B.RCITY \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'RESIDENTIAL' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test06_Resd,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test06_Resd sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test06_Resd,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test06_Resd sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test06_Resd: Test Count = " + testCount);
        System.out.println("Test06_Resd: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test06_Resd");
        System.out.println();

    }

    @Test
    public void test07_Resd(){

        // Check the population of the PERSON_ADDR_WH.STATE_COUNTRY_CD column (for residential addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'RESIDENTIAL'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.PERSON_ADDR_WID, A.STATE_COUNTRY_CD, C.RSTATE \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, DTSDM.STATE_COUNTRY_RFRNC_WH B, FRED.TPERSON C \n" +
                        "\t WHERE A.STATE_COUNTRY_WID = B.STATE_COUNTRY_WID \n" +
                        "\t AND A.STATE_COUNTRY_CD = B.STATE_COUNTRY_CD" +
                        "\t AND B.STATE_COUNTRY_CD = C.RSTATE \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'RESIDENTIAL' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test07_Resd,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test07_Resd sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test07_Resd,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test07_Resd sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test07_Resd: Test Count = " + testCount);
        System.out.println("Test07_Resd: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test07_Resd");
        System.out.println();

    }

    @Test
    public void test08_Resd(){

        // Check the population of the PERSON_ADDR_WH.STATE_COUNTRY_WID column (for residential addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'RESIDENTIAL'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.PERSON_ADDR_WID, A.STATE_COUNTRY_WID, B.STATE_COUNTRY_WID\n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, DTSDM.STATE_COUNTRY_RFRNC_WH B, FRED.TPERSON C\n" +
                        "\t WHERE A.STATE_COUNTRY_WID = B.STATE_COUNTRY_WID\n" +
                        "\t AND A.STATE_COUNTRY_CD = B.STATE_COUNTRY_CD" +
                        "\t AND B.STATE_COUNTRY_CD = C.RSTATE \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'RESIDENTIAL' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test08_Resd,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test08_Resd sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test08_Resd,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test08_Resd sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test08_Resd: Test Count = " + testCount);
        System.out.println("Test08_Resd: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test08_Resd");
        System.out.println();

    }

    @Test
    public void test09_Resd(){

        // Check the population of the PERSON_ADDR_WH.ZIP_CODE column (for residential addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'RESIDENTIAL'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.ZIP_CODE, B.RES_ZIP \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.ZIP_CODE = B.RES_ZIP \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'RESIDENTIAL' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test09_Resd,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test09_Resd sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test09_Resd,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test09_Resd sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test09_Resd: Test Count = " + testCount);
        System.out.println("Test09_Resd: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test09_Resd");
        System.out.println();

    }

    @Test
    public void test10_Resd(){

        // Check the population of the PERSON_ADDR_WH.RES_STATE_TYPE_CD column (for residential addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'RESIDENTIAL'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.STATE_TYPE_CD, B.RES_STATE_TYPE \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.STATE_TYPE_CD = B.RES_STATE_TYPE \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'RESIDENTIAL' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test10_Resd,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test10_Resd sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test10_Resd,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test10_Resd sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test10_Resd: Test Count = " + testCount);
        System.out.println("Test10_Resd: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test10_Resd");
        System.out.println();

    }

    @Test
    public void test04_Offc(){

        // Check the population of the PERSON_ADDR_WH.ADDR1 column (for duty station/office addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'OFFICE'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.ADDR1, B.OFF_ADDR1 \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.ADDR1 = B.OFF_ADDR1 \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'OFFICE' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test04_Offc,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test04_Offc sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test04_Offc,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test04_Offc sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test04_Offc: Test Count = " + testCount);
        System.out.println("Test04_Offc: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test04_Offc");
        System.out.println();

    }

    @Test
    public void test05_Offc(){

        // Check the population of the PERSON_ADDR_WH.ADDR2 column (for duty station/office addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'OFFICE' \n";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.ADDR2, B.OFF_ADDR2 \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.ADDR2 = B.OFF_ADDR2 \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'OFFICE' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test05_Offc,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test05_Offc sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test05_Offc,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test05_Offc sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test05_Offc: Test Count = " + testCount);
        System.out.println("Test05_Offc: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test05_Offc");
        System.out.println();

    }

    @Test
    public void test06_Offc(){

        // Check the population of the PERSON_ADDR_WH.CITY_NAME column (for duty station/office addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'OFFICE'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.CITY_NAME, B.OFF_CITY \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.CITY_NAME = B.OFF_CITY \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'OFFICE' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test06_Offc,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test06_Offc sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test06_Offc,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test06_Offc sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test06_Offc: Test Count = " + testCount);
        System.out.println("Test06_Offc: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test06_Offc");
        System.out.println();

    }

    @Test
    public void test07_Offc(){

        // Check the population of the PERSON_ADDR_WH.STATE_COUNTRY_CD column (for duty station/office addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'OFFICE'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.PERSON_ADDR_WID, A.STATE_COUNTRY_CD, C.OFF_STATE \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, DTSDM.STATE_COUNTRY_RFRNC_WH B, FRED.TPERSON C \n" +
                        "\t WHERE A.STATE_COUNTRY_WID = B.STATE_COUNTRY_WID \n" +
                        "\t AND A.STATE_COUNTRY_CD = B.STATE_COUNTRY_CD" +
                        "\t AND B.STATE_COUNTRY_CD = C.OFF_STATE \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'OFFICE' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test07_Offc,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test07_Offc sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test07_Offc,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test07_Offc sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test07_Offc: Test Count = " + testCount);
        System.out.println("Test07_Offc: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test07_Offc");
        System.out.println();

    }

    @Test
    public void test08_Offc(){

        // Check the population of the PERSON_ADDR_WH.STATE_COUNTRY_WID column (for duty station/office addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'OFFICE'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.PERSON_ADDR_WID, A.STATE_COUNTRY_WID, B.STATE_COUNTRY_WID\n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, DTSDM.STATE_COUNTRY_RFRNC_WH B, FRED.TPERSON C\n" +
                        "\t WHERE A.STATE_COUNTRY_WID = B.STATE_COUNTRY_WID\n" +
                        "\t AND A.STATE_COUNTRY_CD = B.STATE_COUNTRY_CD" +
                        "\t AND B.STATE_COUNTRY_CD = C.OFF_STATE \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'OFFICE' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test08_Offc,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test08_Offc sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test08_Offc,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test08_Offc sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test08_Offc: Test Count = " + testCount);
        System.out.println("Test08_Offc: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test08_Offc");
        System.out.println();

    }

    @Test
    public void test09_Offc(){

        // Check the population of the PERSON_ADDR_WH.ZIP_CODE column (for duty station/office addresses)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'OFFICE'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.ZIP_CODE, B.OFF_ZIP \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.ZIP_CODE = B.OFF_ZIP \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'OFFICE' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test09_Offc,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test09_Offc sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test09_Offc,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test09_Offc sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test09_Offc: Test Count = " + testCount);
        System.out.println("Test09_Offc: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test09_Offc");
        System.out.println();

    }

    @Test
    public void test10_Offc(){

        // Check the population of the PERSON_ADDR_WH.RES_STATE_TYPE_CD column (for duty station/office address)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A \n" +
                        "where A.PERSON_ADDR_WID != 0 \n" +
                        "and A.ADDR_TYPE_DESCR = 'OFFICE'";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSON_ADDR_WID, A.STATE_TYPE_CD, B.OFF_STATE_TYPE \n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.TPERSON B \n" +
                        "\t WHERE A.STATE_TYPE_CD = B.OFF_STATE_TYPE \n" +
                        "\t AND A.ADDR_TYPE_DESCR = 'OFFICE' \n" +
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

        System.out.println("Starting PersonAddrWhTest.test10_Offc,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test10_Offc sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test10_Offc,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test10_Offc sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test10_Offc: Test Count = " + testCount);
        System.out.println("Test10_Offc: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test10_Offc");
        System.out.println();

    }

    @Ignore
    @Test
    public void test11(){

    }

    @Test
    public void test12(){

        // Check the population of the PERSON_ADDR_WH.PHONE_NBR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A where A.PERSON_ADDR_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.PERSON_ADDR_WID, A.PHONE_NBR, B.OFF_PHONE\n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.PERSON B\n" +
                        "\t WHERE A.PHONE_NBR = B.OFF_PHONE \n" +
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

        System.out.println("Starting PersonAddrWhTest.test12,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test12 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test12,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test12 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 12: Test Count = " + testCount);
        System.out.println("Test 12: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test12");
        System.out.println();

    }

    @Test
    public void test13(){

        // Check the population of the PERSON_ADDR_WH.FAX_NBR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.PERSON_ADDR_WH A where A.PERSON_ADDR_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.PERSON_ADDR_WID, A.FAX_NBR, B.OFF_FAX\n" +
                        "\t FROM DTSDM.PERSON_ADDR_WH A, FRED.PERSON B\n" +
                        "\t WHERE A.FAX_NBR = B.OFF_FAX \n" +
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

        System.out.println("Starting PersonAddrWhTest.test13,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test13 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonAddrWhTest.test13,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PersonAddrWh.test13 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 13: Test Count = " + testCount);
        System.out.println("Test 13: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish PersonAddrWhTest.test13");
        System.out.println();

    }

}
