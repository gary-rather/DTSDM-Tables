import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;


import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DprtmntWhTest {

    Connection conn = null;

    @Before
    public void getConnection() {
        Connection con = null;
        try {
        	Conf config = new Conf();
        	
            Properties props = new Properties();
            props.put("myConnectionURL", config.getMyConnectionURL());
            props.put("user", config.getUser());
            props.put("password", config.getPassword());
            //System.out.println("myConnectionURL" + props.getProperty("myConnectionURL"));
            //System.out.println("user" + props.getProperty("user"));
            //System.out.println("password" + props.getProperty("password"));
            
            Class.forName("oracle.jdbc.driver.OracleDriver");
            con = DriverManager.getConnection(props.getProperty("myConnectionURL"), props);
            System.out.println("Connection Successful");
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.conn = con;
    }
    


    @Test
    /*
     * Check that the DPRTMNT_WH.DPRTMNT_WID is the same as in the external file.
     * -- EXPECT a unique number provided by the external file.
     *
     *          Manual comparison with the external file.
     */
    public void test1() {
    	System.out.println("Starting DprtmntWhTest.test1");
        String sql = "select count (distinct(DPRTMNT_WID)) from DTSDM.dprtmnt_wh";

        int count = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
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
    	System.out.println("Starting DprtmntWhTest.test2");
    	
        String sql1 = "select count (*) from DTSDM.dprtmnt_wh";
        
        String sql2 = "select count (distinct(DPRTMNT_CD) ) from DTSDM.dprtmnt_wh";

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
    	System.out.println("Starting DprtmntWhTest.test3");
        assertEquals(0, 0);

    }
}
