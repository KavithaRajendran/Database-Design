import java.io.*;
import java.util.*;
/**
 *   @author Kavitha Rajendran 
 *   @version 1.0
 */
public class MetaDataHandler{

	static int metatableRowId = 0;
	static int metacolumnRowId = 0;
	static String metaDataTables = "data/catalog/kavibase_tables.tbl";
	static String metaDataColumns = "data/catalog/kavibase_columns.tbl";
	
	/*
	public static int  getMetaTableRowId(){
		PageHandler ph = new PageHandler(metaDataTables);
    	int currentPageNumber = ph.findCurrentPageNumber();
    	metatableRowId = ph.getNoOfRecords(currentPageNumber);
		return metatableRowId;
	}
	*/
	public static int  getMetaTableRowId(){
		PageHandler ph = new PageHandler(metaDataTables);
    	int currentPageNumber = ph.findCurrentPageNumber();
    	int offset = ph.getRecordStartOffset(currentPageNumber);
    	try{
        	RandomAccessFile binaryFile = new RandomAccessFile(metaDataTables, "r");
        	//System.out.println("offset:"+(offset+2));
        	binaryFile.seek(offset+2);
        	metatableRowId = binaryFile.readInt();
        	//System.out.println("metatableRowId:"+metatableRowId);
        	binaryFile.close();
    	}catch(Exception e){
    		System.out.println(e);
    	}
    	//metacolumnRowId = ph.getNoOfRecords(currentPageNumber);
		return metatableRowId;
	}
	
	public static int  getMetaColumnRowId(){
		PageHandler ph = new PageHandler(metaDataColumns);
    	int currentPageNumber = ph.findCurrentPageNumber();
    	int offset = ph.getRecordStartOffset(currentPageNumber);
    	try{
        	RandomAccessFile binaryFile = new RandomAccessFile(metaDataColumns, "r");
        	System.out.println("offset:"+(offset+2));
        	binaryFile.seek(offset+2);
        	metacolumnRowId = binaryFile.readInt();
        	//System.out.println("metacolumnRowId:"+metacolumnRowId);
        	binaryFile.close();
    	}catch(Exception e){
    		System.out.println(e);
    	}
    	//metacolumnRowId = ph.getNoOfRecords(currentPageNumber);
		return metacolumnRowId;
	}

	//Creates the directory structure
    public void initiateSchema(){
         //Create directory structure if not exists
        File catalogDir = new File("data/catalog");
        boolean res=false;
        if(!catalogDir.exists()){
            System.out.println("Creating catalog directory");
            
            try{
                catalogDir.mkdirs();
                res=true;
            }
            catch(SecurityException se){
                System.out.println(se);
            }
            if(res==true)
                System.out.println("Directory catalog created");
        }
        File userDataDir = new File("data/user_data");
        if(!userDataDir.exists()){
            System.out.println("Creating user_data directory");
            res=false;
            try{
                userDataDir.mkdirs();
                res=true;
            }
            catch(SecurityException se){
                System.out.println(se);
            }
            if(res==true){
                System.out.println("Directory user_data created");
                boolean res1 = createTableCatalog();
                if (res1==true){
                	boolean res2= createColumnCatalog();
                	if(res2==true){
                		updateSchema();
                	}
                }
            }
        }
    }
    
    //creates kavibase_table file & writes page header
    public boolean createTableCatalog(){
    	boolean res=false;
        String fileName="data/catalog/kavibase_tables.tbl";
        File tableCatalogFile = new File(fileName);
        if(!tableCatalogFile.exists()){
            System.out.println("Creating table catalog file");
            
            try {
                tableCatalogFile.createNewFile();
                tableCatalogFile.setReadable(true);
                tableCatalogFile.setWritable(true);
                //System.out.println("Created table catalog file");
        
                //Increase file size by one more page
                RandomAccessFile binaryFile;
                try {
                    binaryFile = new RandomAccessFile(fileName, "rw");
                    binaryFile.setLength(512);
                    binaryFile.close();
                }
                catch (Exception e) {
                    System.out.println(e);
                }
              //Add header
                FileHandler fh = new FileHandler();
                fh.writePageHeader("data/catalog/kavibase_tables.tbl",true,1); //filename, leafCell?
                res=true;                
            } catch (Exception e) {
                System.out.println("Unable to create " + fileName);
                System.out.println(e);
                
            }
        }
        return res;
    } 
    
    //creates kavibase_column file & writes page header
    boolean createColumnCatalog(){
    	boolean res=false;
        String fileName="data/catalog/kavibase_columns.tbl";
        File columnCatalogFile = new File(fileName);
        if(!columnCatalogFile.exists()){
            System.out.println("Creating column catalog file");
            //boolean res=false;
            try {
                columnCatalogFile.createNewFile();
                columnCatalogFile.setReadable(true);
                columnCatalogFile.setWritable(true);
              //Increase file size by one more page
                RandomAccessFile binaryFile;
                try {
                    binaryFile = new RandomAccessFile(fileName, "rw");
                    binaryFile.setLength(512);
                    binaryFile.close();
                }
                catch (Exception e) {
                    System.out.println(e);
                }
                //System.out.println("Created column catalog file");
                //Add first page with a header
                FileHandler fh = new FileHandler();
                fh.writePageHeader("data/catalog/kavibase_columns.tbl",true,1); //filename, leafCell?
                res=true;
            } catch (Exception e) {
                System.out.println("Unable to create " + fileName);
                System.out.println(e);
             }
        }
        return res;
    }
    void updateSchema(){
    	
    	 //adding catalog details inside catalog
        updateTableCatalog("data/catalog/kavibase_tables.tbl", "kavibase_tables");
        updateTableCatalog("data/catalog/kavibase_tables.tbl", "kavibase_columns");
        
        //adding columns of table table file
        Map<String,String> columnDataMap1 = new LinkedHashMap<String, String>();
        columnDataMap1.put("rowid","INT");
        columnDataMap1.put("table_name","TEXT");
        Map<String,String> columnIsNullableMap1 = new LinkedHashMap<String, String>();
        columnIsNullableMap1.put("rowid","NO");
        columnIsNullableMap1.put("table_name","NO");
        updateColumnCatalog("data/catalog/kavibase_columns.tbl",columnDataMap1,"kavibase_tables",columnIsNullableMap1);
        //Adding columns of column table
        Map<String,String> columnDataMap = new LinkedHashMap<String, String>();
        columnDataMap.put("rowid","INT");
        columnDataMap.put("table_name","TEXT");
        columnDataMap.put("column_name","TEXT");
        columnDataMap.put("data_type","TEXT");
        columnDataMap.put("ordinal_position","TINYINT");
        columnDataMap.put("is_nullable","TEXT");
        Map<String,String> columnIsNullableMap = new LinkedHashMap<String, String>();
        columnIsNullableMap.put("rowid","NO");
        columnIsNullableMap.put("table_name","NO");
        columnIsNullableMap.put("column_name","NO");
        columnIsNullableMap.put("data_type","NO");
        columnIsNullableMap.put("ordinal_position","NO");
        columnIsNullableMap.put("is_nullable","NO");
        updateColumnCatalog("data/catalog/kavibase_columns.tbl",columnDataMap,"kavibase_columns",columnIsNullableMap);

    }
    public static void writeTableCatalogLeaf(int currentPageNumber, String tableFileName, String tableName, int offset, int payloadSize, int rowNo){
    	//write the header and data
    	PageHandler ph = new PageHandler(tableFileName);
		try{
	    	RandomAccessFile binaryFile = new RandomAccessFile(tableFileName, "rw");
			binaryFile.seek(offset);
			
			//write header - payloadsize and row id
			binaryFile.writeShort(payloadSize);
			binaryFile.seek(offset+2);
			binaryFile.writeInt(rowNo);
			
			//Write Paylod - no of columns, 
			int numOfColumns = 2;
			binaryFile.seek(offset+6);
			binaryFile.writeByte(numOfColumns);
			//Write specialCode
			binaryFile.seek(offset+7);
			binaryFile.writeByte(6); //For row id INT
			binaryFile.seek(offset+8);
			binaryFile.writeByte(12+tableName.length()); //For table name TEXT
			binaryFile.seek(offset+9);
			binaryFile.writeInt(rowNo); //write row id
			binaryFile.seek(offset+13);
			binaryFile.writeBytes(tableName); //write table name
			
			//Update no of records
			int newOffset = ph.getPageStartOffset(currentPageNumber);
			binaryFile.seek(newOffset+1);
			int numOfOldRecords = ph.getNoOfRecords(currentPageNumber);
			binaryFile.writeByte(numOfOldRecords+1);
			
			//Update start cell offset
			binaryFile.seek(newOffset+2);
			binaryFile.writeShort(offset);
			
			//update 2byte offset array of each cell
			int newcellOffsetArrayPosition = newOffset+8+(2*numOfOldRecords);
			binaryFile.seek(newcellOffsetArrayPosition);
			binaryFile.writeShort(offset);
			
			//Display the file
			FileHandler fh = new FileHandler();
			//System.out.println("Displaying file: "+ tableFileName);
			//fh.displayBinaryHex(binaryFile);
		
			//Close the file
			binaryFile.close();
	    }
		catch(Exception e){
	    	System.out.println(e);
	    }

    }
    
    public static void updateTableCatalog(String tableFileName, String tableName){
    	//calculate payload size = 1 TinyInt to represent 2 columns + 1 byte for metatableRowId + TableName Text+2 bytes to represent special data type
    	int payloadSize=1+4+tableName.length()+2;
    	//metatableRowId = 0;
    	metatableRowId = getMetaTableRowId();
    	int rowNo = metatableRowId+1;
    	int headerSize= 2+4;
    	int neededSpace = headerSize + payloadSize+2;
    	PageHandler ph = new PageHandler(tableFileName);
    	int availableSpace = ph.availablePageSpace();
    	int currentPageNumber = ph.findCurrentPageNumber();
    	if(availableSpace==-1)
    		System.out.println("Error while measuring available Space in page");
    	else if (availableSpace<neededSpace){
    		//System.out.println("Page overflow - need to write into non-leaf page");        		
    		int newPage = ph.metaDataOverflowHandler(tableFileName,currentPageNumber);
    		//System.out.println("new page:"+newPage);
    		int offset = ph.getOffsetToWriteLeaf(neededSpace-2);
    		//System.out.println("new page offset:"+offset);
    		writeTableCatalogLeaf(newPage, tableFileName,  tableName, offset,payloadSize,rowNo);
    	}
    	else {
    		//find offset - where to start writing
    		int offset = ph.getOffsetToWriteLeaf(neededSpace-2);
    		//System.out.println("neededSpace:"+neededSpace);
    		//System.out.println("going to write at:"+offset);
    		writeTableCatalogLeaf(currentPageNumber, tableFileName,  tableName, offset,payloadSize,rowNo );
    	}
    }
    
    public static void writeColumnCatalogLeaf(int currentPageNumber, int ordinalPosition, String key, String metaColumnFileName,Map<String,String> columnDataMap, String userTableName,Map<String,String> columnIsNullableMap, int rowNo){

    	PageHandler ph = new PageHandler(metaColumnFileName);
    	//int currentPageNumber = ph.findCurrentPageNumber();
    	
		//payloadSize=1+4(rowid)+tableName+columnName+columnDataType+1(ordinalPostion)+is_nullable+6(special type);
		int payloadSize = 12+userTableName.length()+key.length()+(columnDataMap.get(key)).length()+(columnIsNullableMap.get(key)).length();
		
    	int headerSize= 2+4;
    	int neededSpace = headerSize + payloadSize;
    	//find offset - where to start writing
		int offset = ph.getOffsetToWriteLeaf(neededSpace);
		//System.out.println("neededSpace:"+neededSpace);
		//System.out.println("going to write at:"+offset);
		
    	int numOfColoumnsInPayload = columnDataMap.size();
    	//write the header and data
		try{
	    	RandomAccessFile binaryFile = new RandomAccessFile(metaColumnFileName, "rw");
			binaryFile.seek(offset);
			
			//write header - payloadsize and row id
			binaryFile.writeShort(payloadSize);
			binaryFile.seek(offset+2);
			binaryFile.writeInt(rowNo);
			
			//Write Payload - no of columns
			binaryFile.seek(offset+6);
			binaryFile.writeByte(numOfColoumnsInPayload);
			
			//Write serialTypeCode based on data type
			//row id id int type
			binaryFile.seek(offset+7);
			binaryFile.writeByte(6);
			//table name is text
			binaryFile.seek(offset+8);
			binaryFile.writeByte(12+userTableName.length());
			//column name is text
			binaryFile.seek(offset+9);
			binaryFile.writeByte(12+key.length());
			//data type is text
			binaryFile.seek(offset+10);
			binaryFile.writeByte(12+(columnDataMap.get(key)).length());
			//Ordinal Position is tinyint
			binaryFile.seek(offset+11);
			binaryFile.writeByte(4);
			//is_nullable is text
			binaryFile.seek(offset+12);
			binaryFile.writeByte(12+(columnIsNullableMap.get(key)).length());
			
			int off=offset+13;
			
			//Write rowId
			binaryFile.seek(off);
			binaryFile.writeInt(rowNo);
			//Write User Table name
			binaryFile.seek(off+4);
			binaryFile.writeBytes(userTableName);
			//Write Column name
			binaryFile.seek(off+4+userTableName.length());
			binaryFile.writeBytes(key);
			//Write columnData Type
			binaryFile.seek(off+4+userTableName.length()+key.length());
			binaryFile.writeBytes((columnDataMap.get(key)).toUpperCase());
			//Write ordinal position
			binaryFile.seek(off+4+userTableName.length()+key.length()+(columnDataMap.get(key)).length());
			binaryFile.writeByte(ordinalPosition);
			//Write isNullable
			binaryFile.seek(off+5+userTableName.length()+key.length()+(columnDataMap.get(key)).length());
			binaryFile.writeBytes((columnIsNullableMap.get(key)).toUpperCase());
			
			//Update no of records
			int newOffset = ph.getPageStartOffset(currentPageNumber);
			//System.out.println("new page "+currentPageNumber+" newOffset:"+newOffset);
			binaryFile.seek(newOffset+1);
			int numOfOldRecords = ph.getNoOfRecords(currentPageNumber);
			//System.out.println("new page "+currentPageNumber+"numOfOldRecords:"+numOfOldRecords);
			binaryFile.writeByte(numOfOldRecords+1);
			
			//Update start cell offset
			binaryFile.seek(newOffset+2);
			binaryFile.writeShort(offset);
			
			//update 2byte offset array of each cell
			int newcellOffsetArrayPosition = newOffset+8+(2*numOfOldRecords);
			binaryFile.seek(newcellOffsetArrayPosition);
			binaryFile.writeShort(offset);
			
			//Display the file
			
			//FileHandler fh = new FileHandler();
			//System.out.println("Displaying file: "+ metaColumnFileName);
			//fh.displayBinaryHex(binaryFile);
			
			//metacolumnRowId++;
			
			//Close the file
			binaryFile.close();
	    }
		catch(Exception e){
	    	System.out.println(e);
	    }
    }
    public static void updateColumnCatalog(String metaColumnFileName,Map<String,String> columnDataMap, String userTableName,Map<String,String> columnIsNullableMap){
    	//For every key(column), add an entry
    	int ordinalPosition=0;
    	metacolumnRowId = getMetaColumnRowId();
		int rowNo = metacolumnRowId+1;
    	for(String key: columnDataMap.keySet()){
    		//System.out.println("row id:"+rowNo);
    		//payloadSize=1+4(rowid)+tableName+columnName+columnDataType+1(ordinalPostion)+is_nullable+6(special type);
    		int payloadSize = 12+userTableName.length()+key.length()+(columnDataMap.get(key)).length()+(columnIsNullableMap.get(key)).length();
    		//metacolumnRowId = getMetaColumnRowId();
        	int headerSize= 2+4;
        	int neededSpace = headerSize + payloadSize+2; //2 is space needed for 2n array
        	
    		ordinalPosition++;
    		//System.out.println("Inserting postion "+ordinalPosition+" column name:"+key);    		

        	PageHandler ph = new PageHandler(metaColumnFileName);
        	int availableSpace = ph.availablePageSpace();
        	int currentPageNumber = ph.findCurrentPageNumber();
        	if(availableSpace== -1)
        		System.out.println("Error while measuring available Space in page");
        	else if (availableSpace<neededSpace) {
        		//System.out.println("neededSpace:"+neededSpace);
        		//System.out.println("availableSpace:"+availableSpace);
        		int newPage = ph.metaDataOverflowHandler(metaColumnFileName,currentPageNumber);
        		//System.out.println("new page:"+newPage);
        		//System.out.println("new rowId:"+rowNo);
        		writeColumnCatalogLeaf(newPage,ordinalPosition,key, metaColumnFileName,columnDataMap,userTableName, columnIsNullableMap, rowNo);
        	}
        	else {
        		//System.out.println("neededSpace:"+neededSpace);
        		//System.out.println("availableSpace:"+availableSpace);
        		writeColumnCatalogLeaf(currentPageNumber,ordinalPosition,key, metaColumnFileName,columnDataMap,userTableName, columnIsNullableMap,rowNo);
        	}
        	metacolumnRowId = getMetaColumnRowId();
    		rowNo = metacolumnRowId+1;
        	//rowNo++;
    	}
    }  
    //This function read metaDataTable file & displays all table names
    public void displayListOfTables(){
     	PageHandler ph = new PageHandler(metaDataTables);
     	//get first leaf page
    	int currentPageNumber = ph.findFirstLeafNode(1);
    	int numberOfRecords = ph.getNoOfRecords(currentPageNumber);
    	System.out.println("Number of Tables:"+numberOfRecords);
    	try{
    		RandomAccessFile binaryFile = new RandomAccessFile(metaDataTables, "r");
    		System.out.println("----------------------------------------------------------");
	    	System.out.println("|	RowId	|	Table Name	|");
	    	System.out.println("----------------------------------------------------------");
    		if(ph.isLeafPage(currentPageNumber)){
    			//System.out.println("leafPageNumber:"+currentPageNumber);
	    		while(currentPageNumber!=0){
	    			numberOfRecords = ph.getNoOfRecords(currentPageNumber);
	    			//System.out.println("Number of Columns:"+numberOfRecords);
			    	int offset = ph.getPageStartOffset(currentPageNumber);
			    	int recordCounter=0;
			    	binaryFile.seek(offset+8);
			    	
			    	while(recordCounter<numberOfRecords){
			    		binaryFile.seek(offset+8+(2*recordCounter));
			    		int cellOffset = binaryFile.readShort();
			    		//binaryFile.seek(cellOffset);
			    		//int payLoadSize = binaryFile.readShort();
			    		//binaryFile.seek(cellOffset+2);
			    		//int rowId = binaryFile.readInt();
			    		//binaryFile.seek(cellOffset+6);
			    		//int numOfColumns= binaryFile.readByte();
			    		int specialTypeCodeStart = cellOffset+7;
			    		binaryFile.seek(specialTypeCodeStart);
			    		int rowIdType = binaryFile.readByte();
			    		//System.out.println("rowIdType:"+rowIdType);
			    		if(rowIdType==6){
			    			//System.out.println("rowIdType:"+rowIdType);
			    			binaryFile.seek(specialTypeCodeStart+2);
			    			System.out.printf("|	"+binaryFile.readInt()+"	");
			    		}
			    		binaryFile.seek(specialTypeCodeStart+1);
			    		int tableNameCode = binaryFile.readByte();
			    		//System.out.println("tableNameCode:"+tableNameCode);
			    		if(tableNameCode>12){
			    			int tableNameSize = tableNameCode-12;
			    			binaryFile.seek(specialTypeCodeStart+6);
			    			byte[] tableName = new byte[tableNameSize];
			    			binaryFile.read(tableName);
			    			System.out.printf("|	");
			    			for(int i=0; i<tableName.length;i++ ){
			    				System.out.printf(""+(char)tableName[i]);
			    			}
			    			System.out.println("		|");
			    		}
			    		recordCounter++;
			    	}
			    	int pageOffset = ph.getRightPointer(currentPageNumber);
			    	//System.out.println("pageOffset:"+pageOffset);
			    	if(pageOffset!=-1){
			    		currentPageNumber = ((ph.getRightPointer(currentPageNumber))/512)+1;
			    		//System.out.println("currentPageNumber:"+currentPageNumber);
			    	}
			    	else
			    		currentPageNumber=0;
	    		}
    		}
	    	System.out.println("----------------------------------------------------------");
	    	binaryFile.close();
    	}
    	catch (Exception e) {
            System.out.println("Unable to open " + metaDataTables);
        }
    }
    //This function read metaDataColumns file & displays all column names
    public void displayListOfColumns(){
    	//get first leaf page & loop till last leaf page
    	PageHandler ph = new PageHandler(metaDataColumns);
    	int currentPageNumber = ph.findFirstLeafNode(1);
    	int numberOfRecords = ph.getNoOfRecords(currentPageNumber);
    	//System.out.println("Number of Columns:"+numberOfRecords);
    	try{
    		RandomAccessFile binaryFile = new RandomAccessFile(metaDataColumns, "r");
    		System.out.println("-----------------------------------------------------------------------------------------------");
	    	System.out.println("|  RowId  |  Table Name  |  Column Name  |  Data Type  |  Ordinal Position  |  is_nullable  |");
	    	System.out.println("-----------------------------------------------------------------------------------------------");
    		if(ph.isLeafPage(currentPageNumber)){
    			//System.out.println("leafPageNumber:"+currentPageNumber);
	    		while(currentPageNumber!=0){
	    			int recordCounter=0;
	    			numberOfRecords = ph.getNoOfRecords(currentPageNumber);
	    			//System.out.println("Number of Columns:"+numberOfRecords);
	    	    	int offset = ph.getPageStartOffset(currentPageNumber);
	    	    	//System.out.println("offset:"+offset);
			    	while(recordCounter<numberOfRecords){
			    		//System.out.println("recordCounter:"+recordCounter);
			    		int off = offset+8+(2*recordCounter);
			    		//System.out.println("off:"+off);
			    		binaryFile.seek(off);
			    		int cellOffset = binaryFile.readShort();
			    		//System.out.println("cellOffset:"+cellOffset);
			    		int specialTypeCodeStart = cellOffset+7;
			    		//System.out.println("specialTypeCodeStart:"+specialTypeCodeStart);
			    		binaryFile.seek(specialTypeCodeStart);
			    		int rowIdType = binaryFile.readByte();
			    		//System.out.println("rowIdType:"+rowIdType);
			    		if(rowIdType==6){
			    			binaryFile.seek(specialTypeCodeStart+6);
			    			System.out.printf("|	"+binaryFile.readInt()+" ");
			    		}
			    		
			    		//Table Name
			    		binaryFile.seek(specialTypeCodeStart+1);
			    		int tableNameCode = binaryFile.readByte();
			    		//System.out.println("tableNameCode:"+tableNameCode);
			    		int tableNameSize =0;
			    		if(tableNameCode>12){
			    			tableNameSize = tableNameCode-12;
			    			binaryFile.seek(specialTypeCodeStart+10);
			    			byte[] tableName = new byte[tableNameSize];
			    			binaryFile.read(tableName);
			    			System.out.printf("|	");
			    			for(int i=0; i<tableName.length;i++ ){
			    				System.out.printf((char)tableName[i]+"");
			    			}
			    		}
			    		//column name
			    		int columnNameSize =0;
			    		binaryFile.seek(specialTypeCodeStart+2);
			    		int columnNameCode = binaryFile.readByte();
			    		//System.out.println("columnNameCode:"+columnNameCode);
			    		if(columnNameCode>12){
			    			columnNameSize = columnNameCode-12;
			    			binaryFile.seek(specialTypeCodeStart+10+tableNameSize);
			    			byte[] columnName = new byte[columnNameSize];
			    			binaryFile.read(columnName);
			    			System.out.printf("|	");
			    			for(int i=0; i<columnName.length;i++ ){
			    				System.out.printf((char)columnName[i]+"");
			    			}
			    		}
			    		//dataTypeSize
			    		int dataTypeSize =0;
			    		binaryFile.seek(specialTypeCodeStart+3);
			    		int dataTypeCode = binaryFile.readByte();
			    		//System.out.println("dataTypeCode:"+dataTypeCode);
			    		if(dataTypeCode>12){
			    			dataTypeSize = dataTypeCode-12;
			    			binaryFile.seek(specialTypeCodeStart+10+tableNameSize+columnNameSize);
			    			byte[] dataType = new byte[dataTypeSize];
			    			binaryFile.read(dataType);
			    			System.out.printf("|	");
			    			for(int i=0; i<dataType.length;i++ ){
			    				System.out.printf((char)dataType[i]+"");
			    			}
			    		}
			    		
			    		//ordinal position
			    		binaryFile.seek(specialTypeCodeStart+4);
			    		int ordinalPositionType = binaryFile.readByte();
			    		//System.out.println("ordinalPositionType:"+ordinalPositionType);
			    		if(ordinalPositionType==4){
			    			int size = specialTypeCodeStart+10+tableNameSize+columnNameSize+dataTypeSize;
			    			//System.out.printf("size:"+size);
			    			binaryFile.seek(size);
			    			System.out.printf("|	"+binaryFile.readByte());
			    		}
			    		
			    		//is_nullable
			    		int isNullableSize =0;
			    		binaryFile.seek(specialTypeCodeStart+5);
			    		int isNullableType = binaryFile.readByte();
			    		//System.out.println("isNullableType:"+isNullableType);
			    		if(isNullableType>12){
			    			isNullableSize = isNullableType-12;
			    			//System.out.println("isNullableSize:"+isNullableSize);
			    			binaryFile.seek(specialTypeCodeStart+10+tableNameSize+columnNameSize+dataTypeSize+1);
			    			byte[] isNullable = new byte[isNullableSize];
			    			binaryFile.read(isNullable);
			    			System.out.printf("|	");
			    			//System.out.printf("isNullable.length:"+isNullable.length);
			    			for(int i=0; i<isNullable.length;i++ ){
			    				System.out.printf((char)isNullable[i]+"");
			    			}
			    		}
			    		System.out.println(" |");
			    		/*
			    		int dataSize = 0;
			    		int columncounter = numOfColumns;
			    		while(columncounter>0){
			    			int dataStart = specialTypeCodeStart+numOfColumns+dataSize;
			    			binaryFile.seek(specialTypeCodeStart);
			    			int dataType = binaryFile.readByte();
			    			binaryFile.seek(dataStart);
			    			if dataType()
			    			dataSize=;
			    			specialTypeCodeStart++;
			    			columncounter--;
			    		}*/
			    		recordCounter++;
			    	}
			    	int pageOffset = ph.getRightPointer(currentPageNumber);
			    	//System.out.println("pageOffset:"+pageOffset);
			    	if(pageOffset!=-1){
			    		currentPageNumber = ((ph.getRightPointer(currentPageNumber))/512)+1;
			    		//System.out.println("currentPageNumber:"+currentPageNumber);
			    	}
			    	else
			    		currentPageNumber=0;
	    		}
	    		System.out.println("-----------------------------------------------------------------------------------------------");
	    		//close the file
	    		binaryFile.close();
    		}
    	}catch (Exception e) {
            System.out.println(e);
        }
    }
}