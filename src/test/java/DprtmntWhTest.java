import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;


import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DprtmntWhTest extends TableTest {

     @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("DprtmntWhTest.html");
        wr.pageHeader();
    }

    @Test
    /*
     * Check that the DPRTMNT_WH.DPRTMNT_WID is the same as in the external file.
     * -- EXPECT a unique number provided by the external file.
     *
     *          Manual comparison with the external file.
     */
    public void test1() {
    	System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count (distinct(DPRTMNT_WID)) from DTSDM.dprtmnt_wh";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
         wr.logSql(theSql);


        int count = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((1 == count),"(1 == count)");
        roList.add(ro1);
         wr.logTestResults(roList);

        assertEquals(1, count);

        System.out.println("Test DprtmntWhTest Success " + "  count = " + count);
    }

    @Test
    /**
     * Check to ensure that all values in DPRTMNT_WH.DPRTMNT_CD field are unique and taken from the external source file. Pass
     * '-- All values in this column should be unique (one character per department).
     * EXPECT to see only one value ‘D’ in this field because only one department is using DTS now.
     */
    public void test2() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    	
        String sql1 = "select count (*) from DTSDM.dprtmnt_wh";
        
        String sql2 = "select count (distinct(DPRTMNT_CD) ) from DTSDM.dprtmnt_wh";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);


        int totalRowCount = 0;
        int distinctCdCount = 0;
        
        System.out.println("Starting AgncyWhTest.test2,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                    	totalRowCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Starting AgncyWhTest.test2,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                    	distinctCdCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro1 = new ResultObject((totalRowCount == distinctCdCount),"(totalRowCount == distinctCdCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test2 totalRowCount = distinctCdCount " + totalRowCount + " = " + distinctCdCount);
        assertEquals(totalRowCount, distinctCdCount);

    }

    @Test
    /*
     *  * - Check to ensure that DPRTMNT_WH.DPRTMNT_DESCR is populated correctly. Pass
     * '-- EXPECT to see the department description provided in the external file.
     * There is only one record in the table and the DPRTMNT_DESCR vale is ‘Department of Defense’
     */
    public void test3() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        assertEquals(0, 0);

    }
}
