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

public class TripLeaveWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("TripLeaveWhTest.html");
        wr.pageHeader();
    }





}
