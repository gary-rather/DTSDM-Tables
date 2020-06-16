import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public abstract class TableTest {
    Connection conn = null;
    static WriteResults wr = null;

    /**
     * In Oracle to check number of sessions for grath
     * select count(*) from
     * (
     * SELECT sid, serial#, status, osuser ,seconds_in_wait
     * FROM v$session
     * where osuser = 'grath'
     * );
     */
    @Before
    public void getConnection() {
        Connection con = null;
        try {
            Conf config = new Conf();

            System.out.println("myConnectionURL " + config.getProps().getProperty("myConnectionURL"));
            System.out.println("user " + config.getProps().getProperty("user"));
            //System.out.println("password" + props.getProperty("password"));

            Class.forName("oracle.jdbc.driver.OracleDriver");
            con = DriverManager.getConnection(config.getProps().getProperty("myConnectionURL"), config.getProps());
            System.out.println("Connection Successful");
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Connection Should open " );
        this.conn = con;
    }
    @After
    public void closeConnection(){

        try {
            if (this.conn  != null) {
                this.conn.close();
                this.conn = null;
            }
        } catch (Exception e){}
    }

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("AgncyOrgWhTest.html");
        wr.pageHeader();
    }

    @AfterClass
    public static void closeResults() {


        wr.closePage();
        wr.printWriter.flush();
        wr.printWriter.close();
    }
}
