import java.io.*;
import java.util.*;
import java.util.jar.Attributes.Name;
/**
 *   @author Kavitha Rajendran 
 *   @version 1.0
 */
public class UserDataHandler{
	static String tableName;
	UserDataHandler(String fileName){
		tableName = fileName;
	}
	Config conf = new Config();
    int pageSize = conf.getPageSize();
    
	public static void createTable(Map<String,String> columnDataMap,Map<String,String> columnIsNullableMap){
		File userDataDir = new File("data/user_data");
        if(userDataDir.exists()){
            String tableFile = "data/user_data/"+tableName+".tbl";
            File tableFileHandler = new File(tableFile);
            if(!tableFileHandler.exists()){
            	//System.out.println("Creating table");
            	try{
            		tableFileHandler.createNewFile();
            		tableFileHandler.setReadable(true);
            		tableFileHandler.setWritable(true);
            		
            		//Increase file size by one more page
                    RandomAccessFile binaryFile;
                    try {
                        binaryFile = new RandomAccessFile("data/user_data/"+tableName+".tbl", "rw");
                        binaryFile.setLength(512);
                        binaryFile.close();
                    }
                    catch (Exception e) {
                        System.out.println(e);
                    }
            		//System.out.println("Created table file; updating header");
            		
            		//Add first page with a header
            		FileHandler fh = new FileHandler();
                    fh.writePageHeader(tableFile,true,1); //filename, leafCell?
                    //update metadata table catelog
                    MetaDataHandler md = new MetaDataHandler();
                    md.updateTableCatalog("data/catalog/kavibase_tables.tbl",tableName);
                    md.updateColumnCatalog("data/catalog/kavibase_columns.tbl",columnDataMap,tableName,columnIsNullableMap);
                    
                    //fh.writeLeafCell("data/catalog/kavibase_tables.tbl");
            	} catch(Exception e)
            	{
            		System.out.println("Unable to create .tbl file");
            	}
            }
            else{
            	System.out.println("Already a table exists in this Name. Delete & recreate");
            }
        }
	}
	
	public int pageSplit(int currentPageNumber,boolean isRoot, int newKey){
		int newPage=-1;
		PageHandler ph = new PageHandler(tableName);
		if(isRoot){
			//System.out.println("Preserving root");
			try{
    			//System.out.println("this is first time page is overflowing");
    	    	RandomAccessFile binaryFile = new RandomAccessFile(tableName, "rw");
    	    	binaryFile.setLength(pageSize * 3);
    	    	
    	    	//add page header in page 2 & 3 
    	    	FileHandler fh = new FileHandler();
    	    	fh.writePageHeader(tableName,true,2);
    	    	fh.writePageHeader(tableName,true,3);
    			
    	    	//get number of records from page1
    	    	int numberOfRecords = ph.getNoOfRecords(1);
    	    	//page2 key
    	    	int page2Key=-1;
    	    	//page3 key
    	    	int page3Key=-1;
    	    	
    	    	//write half data into page2
    	    	for(int i=0;i<numberOfRecords/2;i++){
    	    		//System.out.println("transfering half page content to page2");
    	    		int startOff = 8+(2*i);
    	    		binaryFile.seek(startOff);
        	    	int page1cellStartOffset = binaryFile.readShort();
        	    	//get the cell data from page1
        	    	binaryFile.seek(page1cellStartOffset);
        	    	int page1CellSize = 6 + binaryFile.readShort();
        	    	byte[] page1Cell = new byte[page1CellSize];
        	    	for(int cs=0; cs<page1CellSize; cs++){
        	    		binaryFile.seek(page1cellStartOffset+cs);
        	    		page1Cell[cs] = binaryFile.readByte();
        	    	}
        	    	//get cell start address of page2
        	    	int page2writeOffset = ph.getRecordStartOffset(2);
        	    	int page2cellOffset = page2writeOffset-page1CellSize;
        	    	binaryFile.seek(page2cellOffset);
        	    	for(int cs=0; cs<page1CellSize; cs++){
        	    		//binaryFile.seek(page1cellStartOffset+cs);
        	    		binaryFile.writeByte(page1Cell[cs]);
        	    	}
        	    	
        	    	//update 2n array
        	    	//get number of records from page2 before inserting data
        	    	int page2RecCount = ph.getNoOfRecords(2);
        	    	binaryFile.seek(512+8+(2*page2RecCount));
        	    	binaryFile.writeShort(page2cellOffset);
        	    	//update cellstart offset
        	    	ph.updateRecordStartOffset(2,page2cellOffset);
        	    	//increment record numbers in page2
        	    	ph.incrementNoOfRecords(2);
        	    	/*
        	    	if(i==0){
        	    		int keyoffset = page2cellOffset+2;
        	    		binaryFile.seek(keyoffset);
        	    		page2Key = binaryFile.readInt();
        	    	}*/
        	    	
    	    	}
    	    	page2Key = ph.findMaxKeyInLeafPage(2);
    	    	//write second half into page3
    	    	for(int i=numberOfRecords/2;i<numberOfRecords;i++){
    	    		//System.out.println("transfering remaining half page content to page3");
    	    		int startOff = 8+(2*i);
    	    		binaryFile.seek(startOff);
        	    	int page1cellStartOffset = binaryFile.readShort();
        	    	//get the cell data from page1
        	    	binaryFile.seek(page1cellStartOffset);
        	    	int page1CellSize = 6 + binaryFile.readShort();
        	    	byte[] page1Cell = new byte[page1CellSize];
        	    	for(int cs=0; cs<page1CellSize; cs++){
        	    		binaryFile.seek(page1cellStartOffset+cs);
        	    		page1Cell[cs] = binaryFile.readByte();
        	    	}
        	    	//get cell start address of page3
        	    	int page3writeOffset = ph.getRecordStartOffset(3);
        	    	int page3cellOffset = page3writeOffset-page1CellSize;
        	    	binaryFile.seek(page3cellOffset);
        	    	for(int cs=0; cs<page1CellSize; cs++){
        	    		//binaryFile.seek(page1cellStartOffset+cs);
        	    		binaryFile.writeByte(page1Cell[cs]);
        	    	}
        	    	//update 2n array
        	    	//get number of records from page3 before inserting data
        	    	int page3RecCount = ph.getNoOfRecords(3);
        	    	binaryFile.seek(1024+8+(2*page3RecCount));
        	    	binaryFile.writeShort(page3cellOffset);
        	    	//update cellstart offset
        	    	ph.updateRecordStartOffset(3,page3cellOffset);
        	    	//increment record numbers in page3
        	    	ph.incrementNoOfRecords(3);
        	    	/*
        	    	if(i==(numberOfRecords/2)+1){
        	    		int keyoffset = page3cellOffset+2;
        	    		binaryFile.seek(keyoffset);
        	    		page3Key = binaryFile.readInt();
        	    	}*/
    	    	}
    	    	page3Key = ph.findMaxKeyInLeafPage(3);
    	    	
    	    	//update page2 right sib pointer
    	    	int siblingPointer = ((2-1)*512)+4;
    	    	//System.out.println("siblingPointer offset:"+siblingPointer);
    	    	binaryFile.seek(siblingPointer);
    	    	//System.out.println("siblingPointer"+(3-1)*512);
    	    	binaryFile.writeInt((3-1)*512);
    	    	
    	    	//cleanup page1
    	    	for(int i=8;i<512;i++)
    	    	{
    	    		binaryFile.seek(i);
    	    		binaryFile.writeByte(0);
    	    	}
    	    	//write non leaf header
    	    	fh.writePageHeader(tableName,false,1);

    	    	//update page1 rightmost child pointer
    	    	binaryFile.seek(4);
    	    	binaryFile.writeInt((3-1)*512);
    	    	
    	    	//add page2 entry
    	    	binaryFile.seek(512-8);
    	    	binaryFile.writeInt(2);
    	    	binaryFile.writeInt(page2Key);
    	    	ph.incrementNoOfRecords(1);
    	    	binaryFile.seek(8);
    	    	binaryFile.writeShort(512-8);
    	    	/*
    	    	//add page3 entry
    	    	binaryFile.seek(512-16);
    	    	binaryFile.writeInt(2);
    	    	binaryFile.writeInt(page3Key);
    	    	ph.incrementNoOfRecords(3);
    	    	binaryFile.seek(8+2);
    	    	binaryFile.writeShort(512-16);
    	    	*/
    	    	//update page1 cell start offset
    	    	binaryFile.seek(2);
    	    	binaryFile.writeShort(512-16);
    	    	
    	    	if(newKey>page2Key){
    	    		newPage = 3;
    	    	}
    	    	else
    	    		newPage=2;
    	    	//System.out.println("Returning new page:"+newPage);
    	    	binaryFile.close();
    	    	return newPage;
			}catch(Exception e){
			   System.out.println(e);
			}
			return newPage;
		}
		return newPage;
	}
	
	public int findParentUsingKey(int currentPageNumber, int newKey){
		//int currentPageNumber=1;
		PageHandler ph = new PageHandler(tableName);
		int numberOfRecords = ph.getNoOfRecords(currentPageNumber);
		//System.out.println("numberOfRecords:"+numberOfRecords);
		for(int i=0;i<numberOfRecords;i++){
			try{
    	    	RandomAccessFile binaryFile = new RandomAccessFile(tableName, "rw");
    	    	int startOff = ((currentPageNumber-1)*512) + 8+(2*i);
    	    	//System.out.println("startOff:"+startOff);
    	    	//read cell start offset
    	    	binaryFile.seek(startOff);
    	    	int page1cellStartOffset = binaryFile.readShort();
    	    	//System.out.println("page1cellStartOffset:"+page1cellStartOffset);
    	    	boolean isLeaf = ph.isLeafPage(currentPageNumber);
    	    	//read key value 
    		    if(isLeaf){
    		    	//System.out.println("cellOffset+2:"+(page1cellStartOffset+2));
    		    	binaryFile.seek(page1cellStartOffset+2);
    		    }
    		    else{
    		    	//System.out.println("cellOffset+4:"+(page1cellStartOffset+4));
    		    	binaryFile.seek(page1cellStartOffset+4);
    		    }
    		    int key = binaryFile.readInt();
    		    //System.out.println("key:"+key);
    		    //System.out.println("new key:"+newKey);
				if((key>=newKey)&&(isLeaf)){
					binaryFile.close();
					//System.out.println("Got the leaf page:"+currentPageNumber);
					return currentPageNumber;
				}
				else if ((key>=newKey)&&(!isLeaf)){
					//System.out.println("page1cellStartOffset:"+page1cellStartOffset);
					binaryFile.seek(page1cellStartOffset);
					currentPageNumber = binaryFile.readInt();
					//System.out.println("Got the interior page:"+currentPageNumber);
					binaryFile.close();
					return findParentUsingKey(currentPageNumber,newKey);
				}
			}catch(Exception e){
			   System.out.println(e);
			}
		}
		//return rightmost child page of root
    	return ((ph.getRightPointer(1)/512)+1);
	}
	
	public void insertTable(List<String> valueTokens, String tName){
		
		int newKey = Integer.valueOf(valueTokens.get(0));
		PageHandler ph = new PageHandler(tableName);
		int currentPageNumber=-1;
		//find page in which we need to insert - from btree
		if(ph.isLeafPage(1))
			currentPageNumber=1;
		else{
			//find leaf page using key
			currentPageNumber = findParentUsingKey(1,newKey);
		}
		if(currentPageNumber==-1){
			System.out.println("not able to find page:");
			return;
		}
		//int currentPageNumber = ph.findCurrentPageNumber();
		int key[] = getListOfKeysFromPage(tableName, currentPageNumber);
		//System.out.println("key.length:"+(key.length));
		for(int i=0;i<key.length;i++){
			if(key[i]==newKey){
				System.out.println("First columns is considered as Primary key");
				System.out.println("Primary key should be unique integer; Cannot insert duplicate key");
				return;
			}
		}
    	int availableSpace = ph.availablePageSpace();
    	//System.out.println("availableSpace to write data:"+availableSpace);
	  	int numOfColumns = valueTokens.size();
    	int neededSpace=getOneRecordSizeOfUserData(tableName, currentPageNumber, valueTokens, tName)+2;
    	//System.out.println("neededSpace to write data:"+(neededSpace-2));
    	int payLoadSize= neededSpace-6-2;
    	if(availableSpace== -1)
    		System.out.println("Error while measuring available Space in page");
    	else if (availableSpace<neededSpace) {
    		if(currentPageNumber==1){
        		//System.out.println("Single page file - overflow");
        		int newpage=-1;
        		//System.out.println("going for root split");
        		newpage = pageSplit(currentPageNumber, true,newKey);
        		//System.out.println("New page:"+newpage);
        		writeUserDataLeaf(tableName,newpage,valueTokens,tName);
        		}
    		else {
    			//System.out.println("going for non root split");
    			int newpage=-1;
    			newpage = pageSplitNotRoot(currentPageNumber,false, newKey);
    			//System.out.println("Overflow");
        		//int newpage = pageSplit(currentPageNumber, false, newKey);
        		//System.out.println("New page:"+newpage);
        		writeUserDataLeaf(tableName,newpage,valueTokens,tName);
        		}
    	}
    	else {
    		writeUserDataLeaf(tableName,currentPageNumber,valueTokens,tName);
    	}
	}
	public int getOneRecordSizeOfUserData(String tableName, int currentPageNumber, List<String> valueTokens, String tName){
		Map<String,String> columnDataMap = getColumnDataType(tableName,tName);
    	Iterator entries = columnDataMap.entrySet().iterator();
    	PageHandler ph = new PageHandler(tableName);
    	int i=0;
    	int size=0;
        while (entries.hasNext()) {
            Map.Entry entry = (Map.Entry) entries.next();
            String key = (String)entry.getKey();
            String dataType = (String)entry.getValue();
            //System.out.println("Key = " + key + ", Value = " + dataType);
    		//String dataType = columnDataMap.get(i);
    		//System.out.println("dataType:"+dataType);
    		int specialcode = ph.getSerialTypeCode(dataType);
    		String value = valueTokens.get(i);
    		//System.out.println("Inserting data:"+value);
    		//System.out.println("specialcode:"+specialcode);
    		if(specialcode==1)
    			size+=2;
    		else if(specialcode==2)
    			size+=4;
    		else if(specialcode==3)
    			size+=8;
    		else if(specialcode==4||specialcode==0)
    			size+=1;
    		else if(specialcode==5)
    			size+=2;
    		else if(specialcode==6||specialcode==8)
    			size+=4;
    		else if(specialcode==7||specialcode==9||specialcode==10||specialcode==11)
    			size+=4;
    		else if(specialcode==12)
    			size+=value.length();
    		
    		//System.out.println("Data field size:"+size);
    		
    		 i++;
    	}
        //System.out.println("Data field size:"+size);
        //int numOfColumns = valueTokens.size();
        //int neededSpace = 6+1+valueTokens.size()+size+2;
       return (6+1+valueTokens.size()+size);
        
	}
	
	public int[] getListOfKeysFromPage(String tableName, int pageNumber){
		PageHandler ph = new PageHandler(tableName);
		int numOfRecords = ph.getNoOfRecords(pageNumber);
		int[] keyList=new int[numOfRecords];
    	
    	try{
			RandomAccessFile binaryFile = new RandomAccessFile(tableName, "rw");
			
			boolean isLeaf = ph.isLeafPage(pageNumber);
			for ( int i=1; i<=numOfRecords; i++){
				int offset = ((pageNumber-1)*512)+8+(2*(i-1));
		    	binaryFile.seek(offset);
		    	int cellOffset=binaryFile.readShort();
		    	if(isLeaf){
		    		//System.out.println("cellOffset+2:"+(cellOffset+2));
		    		binaryFile.seek(cellOffset+2);
		    	}
		    	else{
		    		//System.out.println("cellOffset+4:"+(cellOffset+4));
		    		binaryFile.seek(cellOffset+4);
		    		}
				keyList[i-1]=binaryFile.readInt();
				System.out.println(keyList[i-1]);
			}
			binaryFile.close();
			return keyList;
    	} catch(Exception e){
    		System.out.println(e);
    		return keyList;
    	}
	}
	
	public void writeUserDataLeaf(String tableName, int currentPageNumber, List<String> valueTokens, String tName){
		
		Map<String,String> columnDataMap;
		//System.out.println("writeUserDataLeaf - tableName:"+tableName);
		
    	int numOfColumns = valueTokens.size();
    	int neededSpace=getOneRecordSizeOfUserData(tableName, currentPageNumber, valueTokens, tName)+2;
    	//System.out.println("neededSpace to write data:"+(neededSpace));
    	PageHandler ph = new PageHandler(tableName);
    	int offset = ph.getOffsetToWriteLeaf(neededSpace-2);
    	//System.out.println("going to write at:"+offset);
    	
		//write the header and data
		try{
	    	RandomAccessFile binaryFile = new RandomAccessFile(tableName, "rw");
			binaryFile.seek(offset);
			int payLoadSize= neededSpace-6-2;
			//System.out.println("payLoadSize:"+payLoadSize);
			/*write cell leaf header*/
	    	binaryFile.writeShort(payLoadSize);
	    	binaryFile.seek(offset+2);
	    	//First value is the key
	    	binaryFile.writeInt(Integer.valueOf(valueTokens.get(0)));
	    	binaryFile.seek(offset+6);
	    	binaryFile.writeByte(numOfColumns);
	    	
	    	//Writing values According to data type
	    	int size=0;
	    	columnDataMap = getColumnDataType(tableName,tName);
	    	Iterator entries = columnDataMap.entrySet().iterator();
	    	int i=0;
	    	int dataSize=0;
	    	int newKey=-1;
	        while (entries.hasNext()) {
	            Map.Entry entry = (Map.Entry) entries.next();
	            String key = (String)entry.getKey();
	            if(i==0){
	            	newKey=Integer.valueOf(valueTokens.get(0));
	            	//System.out.println("newKey:"+newKey);
	            }
	            String dataType = (String)entry.getValue();
	            //System.out.println("Key = " + key + ", Value = " + dataType);
	    		//String dataType = columnDataMap.get(i);
	    		//System.out.println("dataType:"+dataType);
	    		int specialcode = ph.getSerialTypeCode(dataType);
	    		String value = valueTokens.get(i);
	    		int serialTypeoffset=(offset+7)+i;
	    		//System.out.println("Inserting data:"+value);

	    		//System.out.println("Data field size:"+size);
	    		
	    		int dataOffset=(offset+7+numOfColumns+size);
	    		//System.out.println("serialTypeoffset:"+serialTypeoffset);
	    		ph.writeSerialTypeCode(value, specialcode, serialTypeoffset);
	    		//System.out.println("dataOffset:"+dataOffset);
	    		ph.writeValueAsPerDataType(value, specialcode,dataOffset);
	    	
	    		if(specialcode==1)
	    			dataSize=2;
	    		else if(specialcode==2)
	    			dataSize=4;
	    		else if(specialcode==3)
	    			dataSize=8;
	    		else if(specialcode==4||specialcode==0)
	    			dataSize=1;
	    		else if(specialcode==5)
	    			dataSize=2;
	    		else if(specialcode==6||specialcode==8)
	    			dataSize=4;
	    		else if(specialcode==7||specialcode==9||specialcode==10||specialcode==11)
	    			dataSize=4;
	    		else if(specialcode>12)
	    			dataSize=value.length();
	    		
	    		size+=dataSize;
	    		i++;
	    		//System.out.println("Going out while loop");
	    	}
	    
	        int newOffset = ph.getPageStartOffset(currentPageNumber);
	        int numOfOldRecords = ph.getNoOfRecords(currentPageNumber);
	       
			//update 2byte offset array of each cell according to primary key
	        int maxOffset = ph.getMaxCellOffet(currentPageNumber);
	        int maxKey = -1;
	        if(maxOffset!=-1)
	        	maxKey = ph.findMaxKeyInLeafPage(currentPageNumber);
	        //System.out.println("maxOffset:"+maxOffset);
	        //System.out.println("maxKey:"+maxKey);
	        //System.out.println("newKey:"+newKey);
			if(newKey>maxKey){
				int newcellOffsetArrayPosition = newOffset+8+(2*numOfOldRecords);
				binaryFile.seek(newcellOffsetArrayPosition);
				binaryFile.writeShort(offset);
			}
			else{
				//System.out.println("Rearrage 2n array");
				int key[] = getListOfKeysFromPage(tableName, currentPageNumber);
				//System.out.println("key.length:"+(key.length));
				for(i=0;i<key.length;i++){
					//System.out.println("key value which is greater than newKey:"+key[i]);
					//System.out.println("key position where is greater than newKey:"+i);
					if(key[i]>newKey)
						break;
				}
				int numOfShift = (key.length)- i;
				i=1;
				int oldPostion=-1;
				int newPosition;
				while(numOfShift > 0)
				{
					oldPostion = newOffset+8+(2*(numOfOldRecords-i));
					binaryFile.seek(oldPostion);
					int valueToBeShifted = binaryFile.readShort();
					newPosition = oldPostion +2;
					binaryFile.seek(newPosition);
					binaryFile.writeShort(valueToBeShifted);
					i++;
					numOfShift--;
				}
				//insert new cell offset now
				//int newcellOffsetArrayPosition = newOffset+8+(2*numOfOldRecords);
				//System.out.println("Position where new offset inserted:"+oldPostion);
				binaryFile.seek(oldPostion);
				binaryFile.writeShort(offset);
			}
			
			//Update start cell offset
			binaryFile.seek(newOffset+2);
			binaryFile.writeShort(offset);
			
			//Update no of records
			binaryFile.seek(newOffset+1);
			binaryFile.writeByte(numOfOldRecords+1);
			
	    	//Display the file
			FileHandler fh = new FileHandler();
			//System.out.println("Displaying file: "+ tableName);
			fh.displayBinaryHex(binaryFile);
			
	    	binaryFile.close();
		}catch(Exception e) {
			System.out.println("Unable to insert");
    		System.out.println(e);
    	}
	}
	public Map<String,String> getColumnDataType(String tableName, String originalTName){
		Map<String,String> columnDataMap = new LinkedHashMap<String,String>();
		Map<String,String> columnValueMap = new LinkedHashMap<String,String>();
		Map<String,String> columnIsNullMap = new LinkedHashMap<String,String>();
		
		//System.out.println("Trying to get column type of table: "+tableName);
		try{
	    	RandomAccessFile binaryFile = new RandomAccessFile("data/catalog/kavibase_columns.tbl", "rw");
	    	PageHandler ph = new PageHandler("data/catalog/kavibase_columns.tbl");
	    	int leafPage = ph.findFirstLeafNode(1);
	    	
	    	while(leafPage!=-1){
	    		//System.out.println(" leaf page:"+leafPage);
	    		//get cell offset array of leaf page
    	    	int numberOfRecords = ph.getNoOfRecords(leafPage);
		    	//System.out.println("Number of Records:"+numberOfRecords);
		    	int offset=0;
		    	//int firstRecordOffset = 0;
    	    	for(int j=0;j<numberOfRecords;j++){
    	    		offset = ((leafPage-1)*512)+8+(2*j);
    	    		binaryFile.seek(offset);
        	    	int cellOffset = binaryFile.readShort();
        	    	//6 (header)+1(numberofcolumns)+1
        	    	int tablenameoffset = cellOffset+8;
        	    	
        	    	binaryFile.seek(tablenameoffset);
        			int tableNameFieldSize = (binaryFile.readByte())-12;
        			//System.out.println("tableNameFieldSize:"+tableNameFieldSize);
        			
        			byte[] tName = new byte[tableNameFieldSize];
        			int tableNameFieldOffset = cellOffset+6+1+6+4;
        			binaryFile.seek(tableNameFieldOffset);
        			binaryFile.read(tName);
        			
        			String tempTname=""; 
        			for(int a=0;a<tableNameFieldSize;a++){
        				tempTname=tempTname+((char)tName[a]);
        			}
        			//System.out.println("tempTname:"+tempTname);
        			
        			if(tempTname.equals(originalTName))
        			{
        			int columnOffset = cellOffset+9;
        			binaryFile.seek(columnOffset);
        			int columnNameFieldSize = (binaryFile.readByte())-12;
        			//System.out.println("columnNameFieldSize:"+columnNameFieldSize);
        			
        			int dataTypeOffset = cellOffset+10;
        			binaryFile.seek(dataTypeOffset);
        			int dataTypeOffsetSize = (binaryFile.readByte())-12;
        			//System.out.println("dataTypeOffsetSize:"+dataTypeOffsetSize);
        			
        			int nullableOffset = cellOffset+12;
        			binaryFile.seek(nullableOffset);
        			int isNullableFieldSize = (binaryFile.readByte())-12;
        			//System.out.println("isNullableFieldSize:"+isNullableFieldSize);
        		
        			byte[] cName = new byte[columnNameFieldSize];
        			byte[] dType = new byte[dataTypeOffsetSize];
        			byte[] isNull = new byte[isNullableFieldSize];
        			
        			int columnNameFieldOffset = tableNameFieldOffset+tableNameFieldSize;
        			binaryFile.seek(columnNameFieldOffset);
        			binaryFile.read(cName);
        			
        			String tempCname=""; 
        			for(int a=0;a<columnNameFieldSize;a++){
        				tempCname=tempCname+((char)cName[a]);
        			}
        			//System.out.println("tempCname:"+tempCname);
        			
        			int dataTypeFieldOffset = columnNameFieldOffset+columnNameFieldSize;
        			binaryFile.seek(dataTypeFieldOffset);
        			binaryFile.read(dType);
        			
        			String tempDataType="";
        			for(int a=0;a<dataTypeOffsetSize;a++){
        				tempDataType=tempDataType+((char)dType[a]);
        			}
        			//System.out.println("tempDataType:"+tempDataType);
        			
        			columnDataMap.put(tempCname,tempDataType);
        			
        			int isNullFieldOffset = dataTypeFieldOffset+dataTypeOffsetSize+1;
        			binaryFile.seek(isNullFieldOffset);
        			binaryFile.read(isNull);
        			
        			String tempIsNull=""; 
        			for(int a=0;a<isNullableFieldSize;a++){
        				tempIsNull=tempIsNull+((char)isNull[a]);
        			}
        			//System.out.println("tempIsNull:"+tempIsNull);
        			columnIsNullMap.put(tempCname,tempIsNull);
        			}
        			else
        				continue;
    	    	}
        	    	//next leaf page
    	    		leafPage=ph.getRightPointer(leafPage);
    	    	}
	    	
	    	binaryFile.close();
	    	Iterator entries = columnDataMap.entrySet().iterator();
	        while (entries.hasNext()) {
	            Map.Entry entry = (Map.Entry) entries.next();
	            String key = (String)entry.getKey();
	            String value = (String)entry.getValue();
	            //System.out.println("Key = " + key + ", Value = " + value);
	        }
	        entries = columnIsNullMap.entrySet().iterator();
	        while (entries.hasNext()) {
	            Map.Entry entry = (Map.Entry) entries.next();
	            String key = (String)entry.getKey();
	            String value = (String)entry.getValue();
	            //System.out.println("Key = " + key + ", Value = " + value);
	        }
	    	return columnDataMap;
			}catch(Exception e) {
				System.out.println("Unable to insert");
				System.out.println(e);
			}
		
		return columnDataMap;
		}
	public boolean isTableExist(String tableName){
		boolean res = false;
		//System.out.println("Trying to get column type of table: "+tableName);
		try{
	    	RandomAccessFile binaryFile = new RandomAccessFile("data/catalog/kavibase_columns.tbl", "rw");
	    	PageHandler ph = new PageHandler("data/catalog/kavibase_columns.tbl");
	    	int leafPage = ph.findFirstLeafNode(1);
	    	while(leafPage!=-1){
	    		//System.out.println(" leaf page:"+leafPage);
	    		//get cell offset array of leaf page
    	    	int numberOfRecords = ph.getNoOfRecords(leafPage);
		    	//System.out.println("Number of Records:"+numberOfRecords);
		    	int offset=0;
		    	//int firstRecordOffset = 0;
    	    	for(int j=0;j<numberOfRecords;j++){
    	    		offset = ((leafPage-1)*512)+8+(2*j);
    	    		binaryFile.seek(offset);
        	    	int cellOffset = binaryFile.readShort();
        	    	//6 (header)+1(numberofcolumns)+1
        	    	int tablenameoffset = cellOffset+8;
        	    	
        	    	binaryFile.seek(tablenameoffset);
        			int tableNameFieldSize = (binaryFile.readByte())-12;
        			//System.out.println("tableNameFieldSize:"+tableNameFieldSize);
        			
        			byte[] tName = new byte[tableNameFieldSize];
        			int tableNameFieldOffset = cellOffset+6+1+6+4;
        			binaryFile.seek(tableNameFieldOffset);
        			binaryFile.read(tName);
        			
        			String tempTname=""; 
        			for(int a=0;a<tableNameFieldSize;a++){
        				tempTname=tempTname+((char)tName[a]);
        			}
        			//System.out.println("tempTname:"+tempTname);
        			
        			if(tempTname.equals(tableName))
        			{
        				binaryFile.close();
        				return true;
        			}
        			else
        				continue;
    	    	}
        	    //next leaf page
    	    	leafPage=ph.getRightPointer(leafPage);
    	    }
	    binaryFile.close();
		}catch(Exception e) {
			System.out.println(e);
		}
		return res;
	}
	
	//This function read metaDataColumns file & displays all column names
    public void displayTable(String tableFileName, String tableName){
    	
    	if(tableName.equals("kavibase_tables")){
    		MetaDataHandler md = new MetaDataHandler();
    		md.displayListOfTables();
    		return;
    	}
    	else if(tableName.endsWith("kavibase_columns")){
    		MetaDataHandler md = new MetaDataHandler();
    		md.displayListOfColumns();
    		return;
    	}
    	else if(!isTableExist(tableName)){
    		System.out.println("Given table doesnt exist;");
    		return;
    	}
    	//get dataTypemap
    	Map<String,String> columnDataMap = new LinkedHashMap<String,String>();
    	columnDataMap = getColumnDataType(tableFileName, tableName);
    	int numOfColumns = columnDataMap.size();
    	//System.out.println("numOfColumns:"+numOfColumns);
    	//print header with column names
    	PageHandler ph = new PageHandler(tableFileName);
    	int currentPageNumber = ph.findFirstLeafNode(1);
    	//System.out.println(" first leafPageNumber:"+currentPageNumber);

    	System.out.println("-----------------------------------------------------------------------------------------------");
    	Iterator entries = columnDataMap.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry entry = (Map.Entry) entries.next();
            String key = (String)entry.getKey();
            System.out.printf("|  "+key+"	");
            //String value = (String)entry.getValue();
            //System.out.println("Key = " + key + ", Value = " + value);
        }
        System.out.println("	|");
    	System.out.println("-----------------------------------------------------------------------------------------------");
    	int numberOfRecords = ph.getNoOfRecords(currentPageNumber);
    	//System.out.println("Number of Records:"+numberOfRecords);
    	//get first leaf page & loop till last leaf page
    	//System.out.println("tableFileName:"+tableFileName);
    	try{
    		RandomAccessFile binaryFile = new RandomAccessFile(tableFileName, "r");
    		if(ph.isLeafPage(currentPageNumber)){
    			//System.out.println("leafPageNumber:"+currentPageNumber);
    	    	while(currentPageNumber!=0){
	    			int recordCounter=0;
	    	    	int offset = ph.getPageStartOffset(currentPageNumber);
	    	    	//System.out.println("offset:"+offset);
	    	    	//read according to every record type
			    	while(recordCounter<numberOfRecords){
			    		numberOfRecords = ph.getNoOfRecords(currentPageNumber);
		    	    	//System.out.println("Number of Records:"+numberOfRecords);
			    		int rec=0;
			    		int dataSize=0;
			    		//System.out.println("recordCounter:"+recordCounter);
			    		int off = offset+8+(2*recordCounter);
			    		//System.out.println("off:"+off);
			    		binaryFile.seek(off);
			    		int cellOffset = binaryFile.readShort();
			    		//System.out.println("cellOffset:"+cellOffset);
			    		int specialTypeCodeStart = cellOffset+7;
			    		//System.out.println("specialTypeCodeStart:"+specialTypeCodeStart);
			    		while(rec<numOfColumns){
				    		binaryFile.seek(specialTypeCodeStart+rec);
				    		int specialCode = binaryFile.readByte();
				    		//System.out.println("specialCode:"+specialCode);
				    		if(specialCode==4){
				    			binaryFile.seek(specialTypeCodeStart+numOfColumns+dataSize);
				    			System.out.printf("|	"+binaryFile.readByte()+" ");
				    			dataSize+=1;
				    		}
				    		if(specialCode==5){
				    			binaryFile.seek(specialTypeCodeStart+numOfColumns+dataSize);
				    			System.out.printf("|	"+binaryFile.readShort()+" ");
				    			dataSize+=2;
				    		}
				    		if(specialCode==6 || specialCode== 8){
				    			binaryFile.seek(specialTypeCodeStart+numOfColumns+dataSize);
				    			System.out.printf("|	"+binaryFile.readInt()+" ");
				    			dataSize+=4;
				    		}
				    		if(specialCode==7 || specialCode==9 || specialCode==10 || specialCode==11){
				    			binaryFile.seek(specialTypeCodeStart+numOfColumns+dataSize);
				    			System.out.printf("|	"+binaryFile.readDouble()+" ");
				    			dataSize+=8;
				    		}
				    		if(specialCode>12){
				    			int charSize = specialCode-12;
				    			byte[] colVal = new byte[charSize];
				    			binaryFile.seek(specialTypeCodeStart+numOfColumns+dataSize);
				    			binaryFile.read(colVal);
				    			String value = "";
				    			for(int a=0; a<charSize;a++){
				    				value+=(char)colVal[a];
				    			}
				    			System.out.printf("|	"+value+" ");
				    			dataSize+=charSize;
				    		}
				    		rec++;
			    		}
			    		System.out.println(" |");
			    		recordCounter++;
			    	}
			    	int pageOffset = ph.getRightPointer(currentPageNumber);
			    	//System.out.println("right sib offset:"+pageOffset);
			    	if(pageOffset!=-1){
			    		currentPageNumber = ((ph.getRightPointer(currentPageNumber))/512)+1;
			    		//System.out.println("right PageNumber:"+currentPageNumber);
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
    
    public int pageSplitNotRoot(int currentPageNumber,boolean isRoot, int newKey){
		int newPage=-1;
		PageHandler ph = new PageHandler(tableName);
		if(!isRoot){
			//System.out.println("Not root");
			//check for leaf 
			boolean isLeaf = ph.isLeafPage(currentPageNumber);
			if(isLeaf){
				try{
	    			//System.out.println("this is first time page is overflowing");
	    	    	RandomAccessFile binaryFile = new RandomAccessFile(tableName, "rw");
					int currentPages=((int)binaryFile.length())/pageSize;
					//System.out.println("current number of pages:"+currentPages);
					
	    	    	binaryFile.setLength((currentPages+1) * 512);
	    	    	int page2number = currentPages+1;
					
					//add page header in page 2
	    	    	FileHandler fh = new FileHandler();
	    	    	fh.writePageHeader(tableName,true,page2number);
	    	    	
	    	    	//get number of records from page1
	    	    	int numberOfRecords = ph.getNoOfRecords(currentPageNumber);
	    	    	//page2 key
	    	    	int page2Key=-1;
	    	    	//page3 key
	    	    	//int page3Key=-1;
	    	    	
	    	    	//write half data into page2
	    	    	for(int i=numberOfRecords/2;i<numberOfRecords;i++){
	    	    		//System.out.println("transfering half page content to page2");
	    	    		int startOff = ((currentPageNumber-1)*512)+8+(2*i);
	    	    		binaryFile.seek(startOff);
	        	    	int page1cellStartOffset = binaryFile.readShort();
	        	    	//get the cell data from page1
	        	    	binaryFile.seek(page1cellStartOffset);
	        	    	int page1CellSize = 6 + binaryFile.readShort();
	        	    	byte[] page1Cell = new byte[page1CellSize];
	        	    	for(int cs=0; cs<page1CellSize; cs++){
	        	    		binaryFile.seek(page1cellStartOffset+cs);
	        	    		page1Cell[cs] = binaryFile.readByte();
	        	    	}
	        	    	//get cell start address of page2
	        	    	int page2writeOffset = ph.getRecordStartOffset(page2number);
	        	    	int page2cellOffset = page2writeOffset-page1CellSize;
	        	    	binaryFile.seek(page2cellOffset);
	        	    	for(int cs=0; cs<page1CellSize; cs++){
	        	    		//binaryFile.seek(page1cellStartOffset+cs);
	        	    		binaryFile.writeByte(page1Cell[cs]);
	        	    	}
	        	    	
	        	    	//update 2n array
	        	    	//get number of records from page2 before inserting data
	        	    	int page2RecCount = ph.getNoOfRecords(page2number);
	        	    	binaryFile.seek(((page2number-1)*512)+8+(2*page2RecCount));
	        	    	binaryFile.writeShort(page2cellOffset);
	        	    	//update cellstart offset
	        	    	ph.updateRecordStartOffset(page2number,page2cellOffset);
	        	    	//increment record numbers in page2
	        	    	ph.incrementNoOfRecords(page2number);
	        	    
	    	    	}
	    	    	page2Key = ph.findMaxKeyInLeafPage(page2number);
	    	    	//page3Key = ph.findMaxKeyInLeafPage(page3number);
	    	    	
	    	    	//current page cleanup
	    	    	for(int i=numberOfRecords/2;i<numberOfRecords;i++){
	    	    		//System.out.println("transfering half page content to page2");
	    	    		int startOff = ((currentPageNumber-1)*512)+8+(2*i);
	    	    		binaryFile.seek(startOff);
	        	    	int page1cellStartOffset = binaryFile.readShort();
	        	    	//get the cell data from page1
	        	    	binaryFile.seek(page1cellStartOffset);
	        	    	int page1CellSize = 6 + binaryFile.readShort();
	        	    	//remove cells
	        	    	//byte[] page1Cell = new byte[page1CellSize];
	        	    	for(int cs=0; cs<page1CellSize; cs++){
	        	    		binaryFile.seek(page1cellStartOffset+cs);
	        	    		binaryFile.writeByte(0);
	        	    	}
	        	    	//wipeoff 2n array 
	        	    	binaryFile.seek(startOff);
	        	    	binaryFile.writeShort(0);
	        	    	//decrement record count
	        	    	ph.decrementNoOfRecords(currentPageNumber);
	    	    	}
	        	    	
	    	    	//update current page right sib pointer
	    	    	int siblingPointer = ((currentPageNumber-1)*512)+4;
	    	    	//System.out.println("siblingPointer offset:"+siblingPointer);
	    	    	binaryFile.seek(siblingPointer);
	    	    	//System.out.println("siblingPointer"+(3-1)*512);
	    	    	binaryFile.writeInt((page2number-1)*512);

	    	    	//update cellstart in currentpage
	    	    	int modifiedNoOfRecords = ph.getNoOfRecords(currentPageNumber);
	    	    	int lastcellOffset = ((currentPageNumber-1)*512)+8+(2*modifiedNoOfRecords)-2;
	    	    	binaryFile.seek(lastcellOffset);
	    	    	int lastcellstart = binaryFile.readShort();
	    	    	binaryFile.seek(((currentPageNumber-1)*512)+2);
	    	    	binaryFile.writeShort(lastcellstart);
	    	    	
	    	    	//add current page as left pointer in root
	    	    	int rootNoOfRecords = ph.getNoOfRecords(1);
	    	    	lastcellOffset = 8+(2*rootNoOfRecords)-2;
	    	    	binaryFile.seek(lastcellOffset);
	    	    	lastcellstart = binaryFile.readShort();
	    	    	binaryFile.seek(lastcellstart-8);
	    	    	binaryFile.writeInt(currentPageNumber);
	    	    	int currentPageMaxKey = ph.findMaxKeyInLeafPage(currentPageNumber);
	    	    	binaryFile.writeInt(currentPageMaxKey);
	    	    	
	    	    	//update page1 rightmost child pointer
	    	    	binaryFile.seek(4);
	    	    	binaryFile.writeInt((page2number-1)*512);
	    	    	
	    	    	if(newKey>currentPageMaxKey){
	    	    		newPage = page2number;
	    	    	}
	    	    	else
	    	    		newPage=currentPageNumber;
	    	    	//System.out.println("Returning new page:"+newPage);
	    	    	
					binaryFile.close();
					return newPage;
				}catch (Exception e) {
					System.out.println(e);
				}
				return newPage;
			}
			return newPage;
		}
		return newPage;
	}
}
			
