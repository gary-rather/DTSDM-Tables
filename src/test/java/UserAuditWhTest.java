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
public class UserAuditWhTest extends TableTest {

    @BeforeClass
    public static void openResults(){
        wr = new WriteResults("UserAuditWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        //check that the unknown record 0 is populated

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql = "Select count(*) from DTSDM.USER_AUDIT_WH A where A.USER_AUDIT_RFRNC_WID = 0 \n";

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

        System.out.println("Test UserAuditWhTest  Row 0 = " + number);
        assertEquals(1, number);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test01");
        System.out.println();

    }

    @Test
    public void test02(){

        //Check the population of the unique identifier (USER_AUDIT_WH.USER_AUDIT_WH_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct A.USER_AUDIT_WID, count(*) from DTSDM.USER_AUDIT_WH A \n" +
                "group by A.USER_AUDIT_WID having count(*) > 1";

        String sql2 = "select count (distinct A.USER_AUDIT_WID) from USER_AUDIT_WH A";
        String sql3 = "select count(*) from DTSDM.USER_AUDIT_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int count = 0;
        int distinctCount = 0;
        int duplicateCount = 0;

        System.out.println("Starting UserAuditWhTest.test02,sql1");
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
            System.out.println("UserAuditWh.test02 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test02,sql2");
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
            System.out.println("UserAuditWh.test02 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test02,sql3");
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
            System.out.println("UserAuditWh.test02 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((distinctCount == count),"(distinctCount == count)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((duplicateCount == 0),"(duplicateCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 02: Count = " + count);
        System.out.println("Test 02: Distinct Count = " + distinctCount);
        System.out.println("Test 02: Duplicate Count =  " + duplicateCount);

        assertEquals(count, distinctCount);
        assertEquals(0, duplicateCount);

        System.out.println("Finish UserAuditWhTest.test02");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the population of the USER_AUDIT_WH.CHNG_PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_PERSON_WID, B.PERSON_WID, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, DTSDM.PERSON_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##PASSWD C, \n" +
                        "\n" +
                        "\t WHERE A.CHNG_PERSON_WID = B.PERSON_WID \n" +
                        "\t AND B.SSN_FULL = C.AUDIT_SSN \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_PERSON_WID, B.PERSON_WID, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, DTSDM.PERSON_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##SIGNATURE C \n" +
                        "\n" +
                        "\t WHERE A.CHNG_PERSON_WID = B.PERSON_WID \n" +
                        "\t AND B.SSN_FULL = C.AUDIT_SSN \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test03,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test03,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test03 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test03,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test03 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test03,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test03 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 03: Test Count Psw = " + testCountPsw);
        System.out.println("Test 03: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 03: Test Count Sig = " + testCountSig);
        System.out.println("Test 03: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04(){

        // Check the population of the USER_AUDIT_WH.USER_PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.USER_PERSON_WID, B.PERSON_WID, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, DTSDM.PERSON_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##PASSWD C, \n" +
                        "\n" +
                        "\t WHERE A.USER_PERSON_WID = B.PERSON_WID \n" +
                        "\t AND B.SSN_FULL = C.SSN \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.USER_PERSON_WID, B.PERSON_WID, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, DTSDM.PERSON_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##SIGNATURE C \n" +
                        "\n" +
                        "\t WHERE A.USER_PERSON_WID = B.PERSON_WID \n" +
                        "\t AND B.SSN_FULL = C.SSN \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test04,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test04 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test04,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test04 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test04,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test04 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test04,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test04 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 04: Test Count Psw = " + testCountPsw);
        System.out.println("Test 04: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 04: Test Count Sig = " + testCountSig);
        System.out.println("Test 04: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05(){

        // Check the population of the USER_AUDIT_WH.CHNG_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_DT, B.AUDIT_DATE, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##PASSWD B, \n" +
                        "\n" +
                        "\t WHERE A.CHNG_DT = B.AUDIT_DT \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_DT, B.AUDIT_DATE, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##SIGNATURE B, \n" +
                        "\n" +
                        "\t WHERE A.CHNG_DT = B.AUDIT_DT \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test05,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test05 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test05,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test05 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test05,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test05 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test05,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test05 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 05: Test Count Psw = " + testCountPsw);
        System.out.println("Test 05: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 05: Test Count Sig = " + testCountSig);
        System.out.println("Test 05: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06(){

        // Check the population of the USER_AUDIT_WH.CHNG_TYPE_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_TYPE_CD, B.AUDIT_DML_TYPE, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##PASSWD B, \n" +
                        "\n" +
                        "\t WHERE A.CHNG_TYPE_CD = B.AUDIT_DML_TYPE \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_TYPE_CD, B.AUDIT_DML_TYPE, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##SIGNATURE B, \n" +
                        "\n" +
                        "\t WHERE A.CHNG_TYPE_CD = B.AUDIT_DML_TYPE \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test06,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test06 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test06,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test06 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test06,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test06 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test06,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test06 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 06: Test Count Psw = " + testCountPsw);
        System.out.println("Test 06: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 06: Test Count Sig = " + testCountSig);
        System.out.println("Test 06: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test06");
        System.out.println();

    }

    @Test
    public void test07(){

        // Check the population of the USER_AUDIT_WH.CHNG_DB_USER_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_DB_USER_NAME, B.AUDIT_DB_USER, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##PASSWD B, \n" +
                        "\n" +
                        "\t WHERE A.CHNG_DB_USER_NAME = B.AUDIT_DB_USER \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_DB_USER_NAME, B.AUDIT_DB_USER, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##SIGNATURE B, \n" +
                        "\n" +
                        "\t WHERE A.CHNG_DB_USER_NAME = B.AUDIT_DB_USER \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test07,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test07 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test07,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test07 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test07,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test07 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test07,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test07 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 07: Test Count Psw = " + testCountPsw);
        System.out.println("Test 07: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 07: Test Count Sig = " + testCountSig);
        System.out.println("Test 07: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test07");
        System.out.println();

    }

    @Test
    public void test08(){

        // Check the population of the USER_AUDIT_WH.USER_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.USER_WID, B.USER_WID \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, DTSDM.USER_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD C \n" +
                        "\n" +
                        "\t WHERE A.USER_WID = B.USER_WID \n" +
                        "\t AND B.DTS_USER_ID = C.USER_ID \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.USER_WID, B.USER_WID \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, DTSDM.USER_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE C \n" +
                        "\n" +
                        "\t WHERE A.USER_WID = B.USER_WID \n" +
                        "\t AND B.DTS_USER_ID = C.USER_ID \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test08,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test08 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test08,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test08 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test08,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test08 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test08,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test08 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 08: Test Count Psw = " + testCountPsw);
        System.out.println("Test 08: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 08: Test Count Sig = " + testCountSig);
        System.out.println("Test 08: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test08");
        System.out.println();

    }

    @Test
    public void test09(){

        // Check the population of the USER_AUDIT_WH.UPDT_USER_FNAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_FNAME, B.FNAME \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A,\n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_FNAME = B.FNAME \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int countSig = 0;
        int etlCountPsw = 0;

        System.out.println("Starting UserAuditWhTest.test09,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test09 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test09,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test09 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test09,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test09 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw + countSig == testCountPsw + countSig),
                "(etlCountPsw + countSig == testCountPsw + countSig)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 09: Test Count Psw = " + testCountPsw);
        System.out.println("Test 09: Etl Count Psw = " + etlCountPsw);
        System.out.println("Test 09: Test Count Sig = " + countSig);

        assertEquals(testCountPsw + countSig, etlCountPsw + countSig);

        System.out.println("Finish UserAuditWhTest.test09");
        System.out.println();

    }

    @Test
    public void test10(){

        // Check the population of the USER_AUDIT_WH.UPDT_USER_LNAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_LNAME, B.LNAME, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED##PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_LNAME = B.LNAME \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_LNAME, B.NAME, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_LNAME = B.NAME \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test10,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test10 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test10,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test10 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test10,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test10 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test10,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test10 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 10: Test Count Psw = " + testCountPsw);
        System.out.println("Test 10: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 10: Test Count Sig = " + testCountSig);
        System.out.println("Test 10: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test10");
        System.out.println();

    }

    @Test
    public void test11(){

        // Check the population of the USER_AUDIT_WH.UPDT_USER_MINIT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_MINIT, B.MNAME \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A,\n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_MINIT = B.MNAME \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int countSig = 0;
        int etlCountPsw = 0;

        System.out.println("Starting UserAuditWhTest.test11,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test11 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test11,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test11 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test11,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test11 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw + countSig == testCountPsw + countSig),
                "(etlCountPsw + countSig == testCountPsw + countSig)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 11: Test Count Psw = " + testCountPsw);
        System.out.println("Test 11: Etl Count Psw = " + etlCountPsw);
        System.out.println("Test 11: Test Count Sig = " + countSig);

        assertEquals(testCountPsw + countSig, etlCountPsw + countSig);

        System.out.println("Finish UserAuditWhTest.test11");
        System.out.println();

    }

    @Test
    public void test12(){

        // Check the population of the USER_AUDIT_WH.UPDT_USER_EMAIL_ADDR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_EMAIL_ADDR, B.EMAIL_ADDR \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A,\n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_EMAIL_ADDR = B.EMAIL_ADDR \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int countSig = 0;
        int etlCountPsw = 0;

        System.out.println("Starting UserAuditWhTest.test12,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test12 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test12,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test12 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test12,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test12 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw + countSig == testCountPsw + countSig),
                "(etlCountPsw + countSig == testCountPsw + countSig)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 12: Test Count Psw = " + testCountPsw);
        System.out.println("Test 12: Etl Count Psw = " + etlCountPsw);
        System.out.println("Test 12: Test Count Sig = " + countSig);

        assertEquals(testCountPsw + countSig, etlCountPsw + countSig);

        System.out.println("Finish UserAuditWhTest.test12");
        System.out.println();

    }

    @Ignore
    @Test
    public void test13(){
        // no test
    }

    @Test
    public void test14(){

        // Check the population of the USER_AUDIT_WH.UPDT_GRP_OWNR_SUBORG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_GRP_OWNR_SUBORG_WID, B.GROUPORG \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A,\n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_GRP_OWNR_SUBORG_WID = B.GROUPORG \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int countSig = 0;
        int etlCountPsw = 0;

        System.out.println("Starting UserAuditWhTest.test14,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test14 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test14,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test14 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test14,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test14 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw + countSig == testCountPsw + countSig),
                "(etlCountPsw + countSig == testCountPsw + countSig)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 14: Test Count Psw = " + testCountPsw);
        System.out.println("Test 14: Etl Count Psw = " + etlCountPsw);
        System.out.println("Test 14: Test Count Sig = " + countSig);

        assertEquals(testCountPsw + countSig, etlCountPsw + countSig);

        System.out.println("Finish UserAuditWhTest.test14");
        System.out.println();

    }

    @Test
    public void test15(){

        // Check the population of the USER_AUDIT_WH.UPDT_PRMSN_LVL column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_PRMSN_LVL, B.LEVEL_ \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A,\n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_PRMSN_LVL = B.LEVEL_ \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int countSig = 0;
        int etlCountPsw = 0;

        System.out.println("Starting UserAuditWhTest.test15,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test15 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test15,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test15 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test15,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test15 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw + countSig == testCountPsw + countSig),
                "(etlCountPsw + countSig == testCountPsw + countSig)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 15: Test Count Psw = " + testCountPsw);
        System.out.println("Test 15: Etl Count Psw = " + etlCountPsw);
        System.out.println("Test 15: Test Count Sig = " + countSig);

        assertEquals(testCountPsw + countSig, etlCountPsw + countSig);

        System.out.println("Finish UserAuditWhTest.test15");
        System.out.println();

    }

    @Test
    public void test16(){

        // Check the population of the USER_AUDIT_WH.UPDT_USER_SUBORG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_SUBORG_WID, B.SUBORG_WID, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM.SUBORG_WID B, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD C \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_SUBORG_WID = B.SUBORG_WID \n" +
                        "\t and B.FULL_ORG_CD = C.ORG \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_SUBORG_WID, B.SUBORG_WID, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM.SUBORG_WID B, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE C \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_SUBORG_WID = B.SUBORG_WID \n" +
                        "\t and B.FULL_ORG_CD = C.ORG \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test16,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test16 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test16,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test16 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test16,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test16 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test16,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test16 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 16: Test Count Psw = " + testCountPsw);
        System.out.println("Test 16: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 16: Test Count Sig = " + testCountSig);
        System.out.println("Test 16: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test16");
        System.out.println();

    }

    @Test
    public void test17(){

        // Check the population of the USER_AUDIT_WH.UPDT_ACCSS_SUBORG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_ACCSS_SUBORG_WID, B.SUBORG_WID \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM.SUBORG_WH B," +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD C \n" +
                        "\n" +
                        "\t WHERE A.UPDT_ACCSS_SUBORG_WID = B.SUBORG_WID \n" +
                        "\t and B.FULL_ORG_CD = C.ORG_ACCESS" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int countSig = 0;
        int etlCountPsw = 0;

        System.out.println("Starting UserAuditWhTest.test17,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test17 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test17,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test17 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test17,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test17 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw + countSig == testCountPsw + countSig),
                "(etlCountPsw + countSig == testCountPsw + countSig)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 17: Test Count Psw = " + testCountPsw);
        System.out.println("Test 17: Etl Count Psw = " + etlCountPsw);
        System.out.println("Test 17: Test Count Sig = " + countSig);

        assertEquals(testCountPsw + countSig, etlCountPsw + countSig);

        System.out.println("Finish UserAuditWhTest.test17");
        System.out.println();

    }

    @Test
    public void test18(){

        // Check the population of the USER_AUDIT_WH.UPDT_USER_GRP_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_GRP_NAME, B.USER_GROUP \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_GRP_NAME = B.USER_GROUP \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int countSig = 0;
        int etlCountPsw = 0;

        System.out.println("Starting UserAuditWhTest.test18,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test18 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test18,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test18 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test18,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test18 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw + countSig == testCountPsw + countSig),
                "(etlCountPsw + countSig == testCountPsw + countSig)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 18: Test Count Psw = " + testCountPsw);
        System.out.println("Test 18: Etl Count Psw = " + etlCountPsw);
        System.out.println("Test 18: Test Count Sig = " + countSig);

        assertEquals(testCountPsw + countSig, etlCountPsw + countSig);

        System.out.println("Finish UserAuditWhTest.test18");
        System.out.println();

    }

    @Test
    public void test19(){

        // Check the population of the USER_AUDIT_WH.UPDT_DEBT_MGT_MONITOR_FLAG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_DEBT_MGT_MONITOR_FLAG, " +
                        "\t\t\t B.DEBT_MGT_MONITOR_FLAG \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_DEBT_MGT_MONITOR_FLAG = B.DEBT_MGT_MONITOR_FLAG \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int countSig = 0;
        int etlCountPsw = 0;

        System.out.println("Starting UserAuditWhTest.test19,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test19 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test19,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test19 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test19,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test19 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw + countSig == testCountPsw + countSig),
                "(etlCountPsw + countSig == testCountPsw + countSig)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 19: Test Count Psw = " + testCountPsw);
        System.out.println("Test 19: Etl Count Psw = " + etlCountPsw);
        System.out.println("Test 19: Test Count Sig = " + countSig);

        assertEquals(testCountPsw + countSig, etlCountPsw + countSig);

        System.out.println("Finish UserAuditWhTest.test19");
        System.out.println();

    }

    @Test
    public void test20(){

        // Check the population of the USER_AUDIT_WH.UPDT_APPRV_OVRD_SW column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_APPRV_OVRD_SW, B.APPROVERIDE \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_APPRV_OVRD_SW = B.APPROVERIDE \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int countSig = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test20,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test20 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test20,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test20 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test20,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test20 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountSig + countSig == testCountPsw + countSig),
                "(etlCountPsw + countSig == testCountPsw + countSig)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 20: Test Count Psw = " + testCountPsw);
        System.out.println("Test 20: Etl Count Psw = " + etlCountSig);
        System.out.println("Test 20: Test Count Sig = " + countSig);

        assertEquals(testCountPsw + countSig, etlCountSig + countSig);

        System.out.println("Finish UserAuditWhTest.test20");
        System.out.println();

    }

    @Test
    public void test21(){

        // Check the population of the USER_AUDIT_WH.NDEA_FLAG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.NDEA_FLAG, B.DGE_TENTERED \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE B \n" +
                        "\n" +
                        "\t WHERE A.NDEA_FLAG = B.DGE_TENTERED \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int countSig = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test21,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test21 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test21,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test21 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test21,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test21 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountSig + countSig == testCountPsw + countSig),
                "(etlCountPsw + countSig == testCountPsw + countSig)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 21: Test Count Psw = " + testCountPsw);
        System.out.println("Test 21: Etl Count Psw = " + etlCountSig);
        System.out.println("Test 21: Test Count Sig = " + countSig);

        assertEquals(testCountPsw + countSig, etlCountSig + countSig);

        System.out.println("Finish UserAuditWhTest.test21");
        System.out.println();

    }

    @Test
    public void test22(){

        // Check the population of the USER_AUDIT_WH.CHNG_DT_KEY column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_DT_KEY, TO_CHAR(B.AUDIT_DT, 'MMDDYYYY'), \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.CHNG_DT_KEY = TO_CHAR(B.AUDIT_DT, 'MMDDYYYY') \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_DT_KEY, TO_CHAR(B.AUDIT_DT, 'MMDDYYYY'), \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE B \n" +
                        "\n" +
                        "\t WHERE A.CHNG_DT_KEY = TO_CHAR(B.AUDIT_DT, 'MMDDYYYY') \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test22,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test22 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test22,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test22 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test22,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test22 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test22,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test22 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 22: Test Count Psw = " + testCountPsw);
        System.out.println("Test 22: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 22: Test Count Sig = " + testCountSig);
        System.out.println("Test 22: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test22");
        System.out.println();

    }

    @Test
    public void test23(){

        // Check the population of the USER_AUDIT_WH.CHNG_PERSON_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_PERSON_NAME, B.AUDIT_USER_NAME, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.CHNG_PERSON_NAME = B.AUDIT_USER_NAME \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_PERSON_NAME, B.AUDIT_USER_NAME, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE B \n" +
                        "\n" +
                        "\t WHERE A.CHNG_PERSON_NAME = B.AUDIT_USER_NAME \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test23,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test23 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test23,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test23 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test23,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test23 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test23,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test23 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 23: Test Count Psw = " + testCountPsw);
        System.out.println("Test 23: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 23: Test Count Sig = " + testCountSig);
        System.out.println("Test 23: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test23");
        System.out.println();

    }

    @Test
    public void test24(){

        // Check the population of the USER_AUDIT_WH.CHNG_PERSON_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int countPsw = 0;
        int countSig = 0;
        int countTotal = 0;

        System.out.println("Starting UserAuditWhTest.test24,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test24 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test24,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test24 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test24,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countTotal++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test24 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countPsw + countSig == countTotal),
                "(countPsw + countSig == countTotal)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 24: Psw Table Count = " + countPsw);
        System.out.println("Test 24: Sig Table Count = " + countSig);
        System.out.println("Test 24: Total Count = " + countTotal);

        assertEquals(countTotal, countPsw + countSig);

        System.out.println("Finish UserAuditWhTest.test24");
        System.out.println();

    }

    @Test
    public void test25(){

        // Check the population of the USER_AUDIT_WH.CHNG_PERSON_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_PERSON_SSN, B.SSN, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.CHNG_PERSON_SSN = B.SSN \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.CHNG_PERSON_SSN, B.SSN, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE B \n" +
                        "\n" +
                        "\t WHERE A.CHNG_PERSON_SSN = B.SSN \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test25,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test25 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test25,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test25 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test25,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test25 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test25,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test25 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 25: Test Count Psw = " + testCountPsw);
        System.out.println("Test 25: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 25: Test Count Sig = " + testCountSig);
        System.out.println("Test 25: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test25");
        System.out.println();

    }

    @Test
    public void test26(){

        // Check the population of the USER_AUDIT_WH.SRC_AUDIT_ID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.SRC_AUDIT_ID, B.AUDIT_ID, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.SRC_AUDIT_ID = B.AUDIT_ID \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.SRC_AUDIT_ID, B.AUDIT_ID, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE B \n" +
                        "\n" +
                        "\t WHERE A.SRC_AUDIT_ID = B.AUDIT_ID \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test26,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test26 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test26,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test26 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test26,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test26 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test26,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test26 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 26: Test Count Psw = " + testCountPsw);
        System.out.println("Test 26: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 26: Test Count Sig = " + testCountSig);
        System.out.println("Test 26: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test26");
        System.out.println();

    }

    @Test
    public void test27(){

        // Check the population of the USER_AUDIT_WH.UPDT_USER_FULL_ORG_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_FULL_ORG_CD, B.ORG, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_FULL_ORG_CD = B.ORG \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_FULL_ORG_CD, B.ORG, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_FULL_ORG_CD = B.ORG \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test27,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test27 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test27,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test27 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test27,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test27 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test27,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test27 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 27: Test Count Psw = " + testCountPsw);
        System.out.println("Test 27: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 27: Test Count Sig = " + testCountSig);
        System.out.println("Test 27: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test27");
        System.out.println();

    }

    @Test
    public void test28(){

        // Check the population of the USER_AUDIT_WH.UPDT_USER_SSN column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#PASSWD'";

        String sql2 = "select * from DTSDM.USER_AUDIT_WH A \n" +
                        "where A.USER_AUDIT_WID != 0 \n" +
                        "and A.SRC_TBL_NAME = 'FRED#SIGNATURE'";

        String sql3 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_SSN, B.SSN, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#PASSWD B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_SSN = B.SSN \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#PASSWD' \n" +
                        ")";

        String sql4 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.USER_AUDIT_WID, \n" +
                        "\t\t\t A.UPDT_USER_SSN, B.SSN, \n" +
                        "\t\t\t A.SRC_TBL_NAME \n" +
                        "\n" +
                        "\t FROM DTSDM.USER_AUDIT_WH A, \n" +
                        "\t\t\t DTSDM_SRC_STG.FRED#SIGNATURE B \n" +
                        "\n" +
                        "\t WHERE A.UPDT_USER_SSN = B.SSN \n" +
                        "\t and A.SRC_TBL_NAME = 'FRED#SIGNATURE' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int testCountPsw = 0;
        int testCountSig = 0;
        int etlCountPsw = 0;
        int etlCountSig = 0;

        System.out.println("Starting UserAuditWhTest.test28,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountPsw++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test28 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test28,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCountSig++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test28 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test28,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountPsw = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test28 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting UserAuditWhTest.test28,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        etlCountSig = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("UserAuditWh.test28 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((etlCountPsw == testCountPsw),"(etlCountPsw == testCountPsw)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((etlCountSig == testCountSig),"(etlCountSig == testCountSig)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 28: Test Count Psw = " + testCountPsw);
        System.out.println("Test 28: Etl Count Psw = " + etlCountPsw);

        System.out.println("Test 28: Test Count Sig = " + testCountSig);
        System.out.println("Test 28: Etl Count Sig = " + etlCountSig);

        assertEquals(testCountPsw, etlCountPsw);
        assertEquals(testCountSig, etlCountSig);

        System.out.println("Finish UserAuditWhTest.test28");
        System.out.println();

    }

}
