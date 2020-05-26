public class ResultObject {
    Boolean theResult = null;
    String theTest = "&nbsp;";

    public ResultObject(Boolean theResult, String theTest){
        this.theResult = theResult;
        this.theTest = theTest;
    }
    public Boolean getTheResult() {
        return theResult;
    }

    public void setTheResult(Boolean theResult) {
        this.theResult = theResult;
    }

    public String getTheTest() {
        return theTest;
    }

    public void setTheTest(String theTest) {
        this.theTest = theTest;
    }


}
