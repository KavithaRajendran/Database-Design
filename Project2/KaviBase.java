import java.io.*;
import java.util.*;
import java.util.regex.*;

/**
 *  @author Kavitha Rajendran 
 *  @version 1.0
 *
 */
public class KaviBase{

    static String prompt = "kavisql> ";
    static String version = "v1.0";
    static String copyright = "Â©2017 Kavitha Rajendran";
    static boolean isExit = false;
    static Map<String,String> columnDataMap = new LinkedHashMap<String,String>(); // Used while creating tables
    static Map<String,String> columnValueMap = new LinkedHashMap<String,String>(); //Used while inserting values
    static Map<String,String> columnIsNullableMap = new LinkedHashMap<String,String>(); //Used to check for null values
    
    /*
     * Page size for all files is 512 bytes by default.
     */
    static long pageSize; 
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");
    
    /** ***********************************************************************
     *  Main method
     */
    public static void main(String[] args) {
        Config conf = new Config();
        pageSize = conf.getPageSize();
        MetaDataHandler md = new MetaDataHandler();
        
        //creating directory structure
        md.initiateSchema();
        
        /* Display the welcome screen */
        splashScreen();

        /* Variable to collect user input from the prompt */
        String userCommand = ""; 

        while(!isExit) {
            System.out.print(prompt);
            /* toLowerCase() renders command case insensitive */
            userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
            // userCommand = userCommand.replace("\n", "").replace("\r", "");
            parseUserCommand(userCommand);
        }
        System.out.println("Bye");
    }

    /** ***********************************************************************
     *  Method definitions
     */

    /**
     *  Display the splash screen
     */
    public static void splashScreen() {
        System.out.println(printLine("-",100));
        System.out.println("Welcome to KaviBase"); // Display the string.
        System.out.println("Version " + getVersion());
        System.out.println(getCopyright());
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println("\nEvery command should end with semicolon ;");
        System.out.println(printLine("-",100));
    }
    
    public static String printLine(String s,int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }
        /**
         *  Help: Display supported commands
         */
        public static void help() {
            System.out.println(printLine("*",120));
            System.out.println("SUPPORTED COMMANDS");
            System.out.println("All commands below are case insensitive");
            System.out.println();
            System.out.println("\tSHOW TABLES;                                                                  Display all tables in the schema.");
            //System.out.println("\tSELECT table_name FROM kavibase_tables;                                       Display all records in the table.");
            System.out.println("\tCREATE TABLE table_name (column_name data_type [PRIMARY KEY] [NOT NULL],.);   Create a table.");
            System.out.println("\tINSERT INTO TABLE (column_list) table_name VALUES (value_list);               Insert a record into table.");
            System.out.println("\tDELETE FROM TABLE table_name WHERE row_id=key_value;                          Delete a record from a table.");
            System.out.println("\tSELECT * FROM table_name;                                                     Display all records in the table.");
            System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;                               Display records whose rowid is <id>.");
            System.out.println("\tDROP TABLE table_name;                                                        Remove table data and its schema.");
            System.out.println("\tVERSION;                                                                      Show the program version.");
            System.out.println("\tHELP;                                                                         Show this help information");
            System.out.println("\tEXIT;                                                                         Exit the program");
            System.out.println("\tQUIT;                                                                         Exit the program");
            System.out.println();
            System.out.println();
            System.out.println(printLine("*",120));
        }

    /** return the kaviBase version */
    public static String getVersion() {
        return version;
    }
    
    public static String getCopyright() {
        return copyright;
    }
    
    public static void displayVersion() {
        System.out.println("KaviBaseLite Version " + getVersion());
        System.out.println(getCopyright());
    }
        
    public static void parseUserCommand (String userCommand) {
        
        /* commandTokens is an array of Strings that contains one token per array element 
         * The first token can be used to determine the type of command 
         * The other tokens can be used to pass relevant parameters to each command-specific
         * method inside each case statement */
        // String[] commandTokens = userCommand.split(" ");
        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
        

        /*
        *  This switch handles a very small list of hard coded commands of known syntax.
        *  You will want to rewrite this method to interpret more complex commands. 
        */
        switch (commandTokens.get(0)) {
        	case "show":
        		display(userCommand);
        		break;
            case "select":
                parseQueryString(userCommand);
                break;
            case "drop":
                System.out.println("STUB: Calling your method to drop items");
                dropTable(userCommand);
                break;
            case "create":
                parseCreateString(userCommand);
                break;
            case "insert":
                parseInsertString(userCommand);
                break;
            case "delete":
                parseDeleteString(userCommand);   
                break;
            case "help":
                help();
                break;
            case "version":
                displayVersion();
                break;
            case "exit":
                isExit = true;
                break;
            case "quit":
                isExit = true;
                break;
            default:
                System.out.println("Unknown command: \"" + userCommand + "\"");
                break;
        }
    }
    
    public static void display(String showTableString) {
        //System.out.println("STUB: Calling showListOfTables(String s) to process queries");
        //System.out.println("Parsing the string:\"" + showTableString + "\"");
        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(showTableString.split(" ")));
        if(commandTokens.get(1).equals("tables")){
        	MetaDataHandler md = new MetaDataHandler();
        	md.displayListOfTables();
        }
        else if(commandTokens.get(1).equals("columns")){
        	MetaDataHandler md = new MetaDataHandler();
        	md.displayListOfColumns();
        }
        else{
        	System.out.println("Not a valid command");
        }
    }
   
    public static void dropTable(String dropTableString) {
        //System.out.println("STUB: Calling parseQueryString(String s) to process queries");
        System.out.println("Parsing the string:\"" + dropTableString + "\"");
    	System.out.println("Table droped");
    }
    
    public static void parseQueryString(String queryString) {
        //System.out.println("STUB: Calling parseQueryString(String s) to process queries");
        //System.out.println("Parsing the string:\"" + queryString + "\"");
        //get values list
        //Pattern pattern = Pattern.compile(".+\\bfrom\\b\\s+(\\w+)\\s+.+");
        Pattern pattern = Pattern.compile(".+\\bfrom\\b\\s+(\\w+)");
        Matcher matcher = pattern.matcher(queryString);
        if(matcher.find()){
        	//System.out.println("got it");
        	String tableName = matcher.group(1);
        	//System.out.println("Table Name"+tableName);
        	String fileName = "data/user_data/"+tableName+".tbl";
        	UserDataHandler table = new UserDataHandler(fileName);;
            table.displayTable(fileName, tableName);
        }
    }
    
    /**
     *  Stub method for creating new tables
     *  @param queryString is a String of the user input
     */
    public static void parseCreateString(String createTableString) {
        //System.out.println("Parsing the string:\"" + createTableString + "\"");
        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

        /* Define table file name */
        String tableFileName = createTableTokens.get(2) + ".tbl";
        String tableName = createTableTokens.get(2);
        //System.out.println("tableName:"+tableName);
        //System.out.println("tableFileName:"+tableFileName);
        
        //parse column fields into map
        String column = createTableString.substring(createTableString.indexOf("(")+1,createTableString.indexOf(")"));
        //System.out.println("column:"+column);
        ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList((column.trim()).split(",")));
        for(int i=0; i<columnTokens.size(); i++){
        	int size = (((columnTokens.get(i)).trim()).split("\\s+")).length;
        	//System.out.println("size:"+size);
        	if(i==0){
        		columnDataMap.put((((columnTokens.get(i)).trim()).split("\\s+"))[0],"INT");
        		columnIsNullableMap.put((((columnTokens.get(i)).trim()).split("\\s+"))[0],"NO");
        	}
        	else if(i>0)
        	{
        		columnDataMap.put((((columnTokens.get(i)).trim()).split("\\s+"))[0],(((columnTokens.get(i)).trim()).split("\\s+"))[1]);
        		if(size>2){
        			columnIsNullableMap.put((((columnTokens.get(i)).trim()).split("\\s+"))[0],"NO");
        		}
        		else if (size==2){
        			columnIsNullableMap.put((((columnTokens.get(i)).trim()).split("\\s+"))[0],"YES");
        		}
        	}
        	else
        		System.out.println("Given command is not correct");
        }
        /*
        Iterator entries = columnDataMap.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry entry = (Map.Entry) entries.next();
            String key = (String)entry.getKey();
            String value = (String)entry.getValue();
            System.out.println("Key = " + key + ", Value = " + value);
        }*/
        UserDataHandler table = new UserDataHandler(tableName);
        table.createTable(columnDataMap,columnIsNullableMap);
    }
    
    public static void parseInsertString(String insertTableString) {
       
        System.out.println("Parsing the string:\"" + insertTableString + "\"");
        String tableFileName;
        Pattern pattern = Pattern.compile("\\(([^)]+)\\)");
        Matcher matcher = pattern.matcher(insertTableString.trim());
        int count=0;
        while(matcher.find()){
        	System.out.println(matcher.group());
        	count++;
		}
        System.out.println("count:"+count);
        List<String> columnTokens = new ArrayList<String>();
        List<String> valueTokens = new ArrayList<String>();
        
        //get column list
        if(count==2){
        pattern = Pattern.compile(".+\\btable\\b\\s+\\(\\s?(.*)\\)\\s+\\w+");
        matcher = pattern.matcher(insertTableString);
        if(matcher.find()){
        	//System.out.println("got it");
        	String columnList = matcher.group(1);
        	//System.out.println("columnList:"+columnList);
        	columnTokens = Arrays.asList(columnList.trim().split(" "));
        	}
        }
        
        //get values list
        pattern = Pattern.compile(".+\\bvalues\\b\\s+\\(\\s?(.*)\\)");
        matcher = pattern.matcher(insertTableString);
        if(matcher.find()){
        	//System.out.println("got it");
        	String valueList = matcher.group(1);
        	//System.out.println("valueList:"+valueList);
        	valueTokens = Arrays.asList(valueList.trim().split(","));
        }
        
        //map column to values
        Map<String,String> columnValueInsertMap = new LinkedHashMap<String,String>(); 
        if(count==2){
        for(int i=0; i<columnTokens.size(); i++){
        	String column = (columnTokens.get(i)).trim();
        	String value = (valueTokens.get(i)).trim();
        	columnValueInsertMap.put(column,value);
        }
        }
        //get table name
        if(count==2){
        	//pattern = Pattern.compile("\\(([\\w+]) \\s values \\)");
        	pattern = Pattern.compile(".+\\s+(\\w+)\\s+\\bvalues\\b.*");
            matcher = pattern.matcher(insertTableString);
            if(matcher.find()){
            	tableFileName = matcher.group(1);
            	//System.out.println("tableName:"+tableFileName);
            	UserDataHandler table = new UserDataHandler("data/user_data/"+tableFileName+".tbl");
                table.insertTable(valueTokens,tableFileName);
            }
            else{
               	System.out.println("Please mention column list along with the table name & value list as shown below example");
               	System.out.println("INSERT INTO table (AUTHOR_ID, NAME ) AUTHORS VALUES (1,'Mark P. O. Morford')");
            }
        }	
    }

	public static void parseDeleteString(String deleteTableString) {
    
    //System.out.println("STUB: Calling your method to delete from table");
    System.out.println("Parsing the string:\"" + deleteTableString + "\"");
    ArrayList<String> deleteTableTokens = new ArrayList<String>(Arrays.asList(deleteTableString.split(" ")));

    /* Define table file name */
    String tableFileName = deleteTableTokens.get(2) + ".tbl";
    System.out.println(tableFileName);
    UserDataHandler table = new UserDataHandler(tableFileName);
    //table.deleteTable();
	}
}
