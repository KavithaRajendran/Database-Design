import java.io.*;
import java.util.*;

/**
 *  @author Kavitha Rajendran 
 *  @version 1.0
 *
 */

public class PageHandler{
	String fileName;
	
	//Constructor
	PageHandler(String file) {
		fileName = file;
	}
	
	//Get Configured page size
	Config conf = new Config();
    int pageSize = conf.getPageSize();
    
	//Returns last page number
    public int findCurrentPageNumber(){
    	int currentPages;
    	try{
    	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
        //calculate page size
    	currentPages=((int)binaryFile.length())/pageSize;
    	//System.out.println("currentPage:"+currentPages);
    	binaryFile.close();
    	return currentPages;
    	}catch(Exception e){
    		System.out.println(e);
    		return -1;
    	}
    }
    
    //Returns page start
    public int getPageStartOffset(int currentPageNumber){
    	int pageStart=-1;
    	try{
    		RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    		pageStart = (currentPageNumber-1)*512;
    		binaryFile.close();
    		return pageStart;
    	}catch(Exception e){
    		System.out.println(e);
    		return -1;
    	}
    }
    
    //Returns true if the current page is leaf; otherwise False
    public boolean isLeafPage(int currentPageNumber){
    	int flag;
    	try{
    	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
		int pageStart = (currentPageNumber-1)*512;
		binaryFile.seek(pageStart);
		flag = binaryFile.readByte();
		binaryFile.close();
		if (flag==13)
			return true;
		else 
			return false;
		
    	}catch(Exception e){
    		System.out.println(e);
    		return false;
    	}
    }
    
    //Read & return total number of cells
    public int getNoOfRecords(int currentPageNumber){
    	int numOfRecords;
    	try{
    	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
		int pageStart = (currentPageNumber-1)*512;
		binaryFile.seek(pageStart+1);
		numOfRecords = binaryFile.readByte();
		binaryFile.close();
		return numOfRecords;
    	}catch(Exception e){
    		System.out.println(e);
    		return 0;
    	}
    }
    
    //increments number of records field in cell header & returns new count
    public int incrementNoOfRecords(int currentPageNumber){
    	int numOfRecords=-1;
    	try{
    	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
		int pageStart = (currentPageNumber-1)*512;
		binaryFile.seek(pageStart+1);
		numOfRecords = binaryFile.readByte();
		binaryFile.seek(pageStart+1);
		binaryFile.writeByte(numOfRecords+1);
		binaryFile.seek(pageStart+1);
		numOfRecords = binaryFile.readByte();
		binaryFile.close();
    	}catch(Exception e){
    		System.out.println(e);
    	}
    	return numOfRecords;
    }
  //increments number of records field in cell header & returns new count
    public int decrementNoOfRecords(int currentPageNumber){
    	int numOfRecords=-1;
    	try{
    	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
		int pageStart = (currentPageNumber-1)*512;
		binaryFile.seek(pageStart+1);
		numOfRecords = binaryFile.readByte();
		binaryFile.seek(pageStart+1);
		binaryFile.writeByte(numOfRecords-1);
		binaryFile.seek(pageStart+1);
		numOfRecords = binaryFile.readByte();
		binaryFile.close();
    	}catch(Exception e){
    		System.out.println(e);
    	}
    	return numOfRecords;
    }
    
    
    //Reads and returns starting offset of records
    public int getRecordStartOffset(int currentPageNumber){
    	int cellStartOffset;
    	try{
    	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
		int pageStart = (currentPageNumber-1)*512;
		binaryFile.seek(pageStart+2);
		cellStartOffset = binaryFile.readShort();
		//System.out.println("cellStartOffset:"+cellStartOffset);
		binaryFile.close();
		return cellStartOffset;
    	}catch(Exception e){
    		System.out.println(e);
    		return 0;
    	}
    }
    //updates starting offset of records
    public void updateRecordStartOffset(int currentPageNumber, int newRecordStartOffset){
    	try{
    	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
		int pageStart = (currentPageNumber-1)*512;
		binaryFile.seek(pageStart+2);
		binaryFile.writeShort(newRecordStartOffset);
		binaryFile.close();
    	}catch(Exception e){
    		System.out.println(e);
    		
    	}
    }
    
    //Returns right sibling pointer
    public int getRightPointer(int currentPageNumber){
    	int rightPointer;
    	//System.out.println("fileName:"+fileName);
    	try{
    	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "r");
		int pageStart = (currentPageNumber-1)*512;
		binaryFile.seek(pageStart+4);
		rightPointer = binaryFile.readInt();
		//System.out.println("rightPointer:"+rightPointer);
		binaryFile.close();
		return rightPointer;
    	}catch(Exception e){
    		//System.out.println("Printing from here: getRightPointer");
    		System.out.println(e);
    		return -1;
    	}
    }
    
 	//Returns free space available in last leaf page of the file
    public int availablePageSpace(){
    	//System.out.println("@availablePageSpace fileName:"+fileName);
    	int spaceRemaining=-1;
    	int currentPageNumber = findCurrentPageNumber();
    	//System.out.println("@availablePageSpace currentPageNumber:"+currentPageNumber);
 		int header = getPageStartOffset(currentPageNumber)+8+(2*getNoOfRecords(currentPageNumber));
 		//System.out.println("@availablePageSpace header:"+header);
 		spaceRemaining = getRecordStartOffset(currentPageNumber) - header;
 		//System.out.println("@availablePageSpace spaceRemaining:"+header);
 		//System.out.println("availablePageSpace:"+spaceRemaining);
    	return spaceRemaining;
    }
    
  //Returns free space available in given page of the file
    public int availablePageSpaceNonLeaf(int currentPageNumber){
    	//System.out.println("fileName:"+fileName);
    	int spaceRemaining=-1;
    	//int currentPageNumber = findCurrentPageNumber();
 		int header = getPageStartOffset(currentPageNumber)+8+(2*getNoOfRecords(currentPageNumber));
 		spaceRemaining = getRecordStartOffset(currentPageNumber) - header;
 		//System.out.println("availablePageSpaceNonLeaf:"+spaceRemaining);
    	return spaceRemaining;
    }
    
    //Leaf page - This function returns offset from where cell can start (cell header + payload)
    public int getOffsetToWriteLeaf(int neededSpace){
    	int currentPageNumber = findCurrentPageNumber();
    	int offset = (getRecordStartOffset(currentPageNumber))-neededSpace;
    	//System.out.println("offset returning:"+offset);
    	return offset;
    }
    
    //Interior page
    public int getOffsetToWriteNonLeaf(int currentPageNumber, int neededSpace){
    	return getRecordStartOffset(currentPageNumber)-neededSpace;
    }
    
    public boolean isPageoverflow(int neededSpace, int availableSpace){
    	if (availableSpace<neededSpace)
    		return true;
    	else
    		return false;
    }
    
    public int getSerialTypeCode(String dataType){
    	switch (dataType){
    	case "TINYINT":
    		return 4;
    	case "SMALLINT":
    		return 5;
    	case "INT":
    		return 6;
    	case "BIGINT":
    		return 7;
    	case "REAL":
    		return 8;
    	case "DOUBLE":
    		return 9;
    	case "DATETIME":
    		return 10;
    	case "DATE":
    		return 11;
    	case "TEXT":
    		return 12;
    	default:
    		System.out.println("Data type not matching");
    		return -1;
    	}	
    }
    
    public void writeSerialTypeCode(String Value, int DataType, int offset){

    	if(DataType == 12)
    		DataType = 12+Value.length();
    	//write specialcode
    	try{
        	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    		binaryFile.seek(offset);
    		binaryFile.writeByte(DataType);
    		binaryFile.close();
        }catch(Exception e){
        	System.out.println(e+" "+fileName);
        }	 
    }
   
    public void writeValueAsPerDataType(String Value, int DataType, int offset){
    	
    	try{
        	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    		binaryFile.seek(offset);
    		switch (DataType){
        		case 4:
        			binaryFile.writeByte(Integer.valueOf(Value));
        			break;
        		case 5:
        			binaryFile.writeShort(Integer.valueOf(Value));
        			break;
        		case 6:
        			binaryFile.writeInt(Integer.valueOf(Value));
        			break;
        		case 7:
        			binaryFile.writeDouble(Integer.valueOf(Value));
        			break;
        		case 8:
        			binaryFile.writeInt(Integer.valueOf(Value));
        			break;
        		case 9:
        			binaryFile.writeDouble(Integer.valueOf(Value));
        			break;
        		case 10:
        			binaryFile.writeDouble(Integer.valueOf(Value));
        			break;
        		case 11:
        			binaryFile.writeDouble(Integer.valueOf(Value));
        			break;
        		case 12:
        			binaryFile.writeBytes(Value);
        			break;
        		default:
            		System.out.println("Data type not matching");
            		break;
    		}
    		binaryFile.close();
        }catch(Exception e){
        	System.out.println(e+" "+fileName);
        }
    }
    
    public int updateParentPage(int parentPage){
    	//if overflow occurs
    	int neededSpace=-1;
    	int availableSpace=-1;
    	boolean res = isPageoverflow(neededSpace,availableSpace);
    	if (res == true){
    		int newparentPage = handleMetaDataInteriorPageOverflow(fileName, parentPage);
    		return newparentPage;
    	}
    	else
    		return parentPage;
    }
    
    public int  handleMetaDataLeafOverflow(String FileName, int pageNumber, int parent){
    	int keyDelimiter = getNoOfRecords(pageNumber);
    	int newLeafPageNumber=-1;
    	PageHandler ph = new PageHandler(FileName);
    	if(pageNumber==1){
    		try{

    	    	RandomAccessFile binaryFile = new RandomAccessFile(FileName, "rw");
    	    	newLeafPageNumber = 3;
    	    	pageNumber=3;
    	    	binaryFile.setLength(pageSize * 3);
    	    	
    	    	//add page header in page3
    	    	FileHandler fh = new FileHandler();
    	    	fh.writePageHeader(FileName,true,3);
    			
    	    	/* splitting page */
    	    	byte[] page1 = new byte[512];
    	    	binaryFile.seek(0);
    	    	binaryFile.read(page1);
    	    	//copy page1
    	    	int i = 0;
    	    	while(i<512){
    	    		binaryFile.seek(i);
    	    		binaryFile.writeByte(page1[i]);
    	    		i++;
    	    	}
    	    	
    	    	//write page1 content to page2
    	    	binaryFile.seek(512);
    	    	i = 0;
    	    	while(i<512){
    	    		binaryFile.seek(512+i);
    	    		binaryFile.writeByte(page1[i]);
    	    		i++;
    	    	}
    		
    	    	//make page1 as clean
    	    	int t=0;
    	    	i = 0;
    	    	while(i<512){
    	    		binaryFile.seek(i);
    	    		binaryFile.writeByte(t);
    	    		i++;
    	    	}  

    	    	//add non-leaf page header in page1
                fh.writePageHeader(FileName,false,1);
                
    	    	/*add new entries into root page - left pointer & key delimiter - increase record count - change cell start offset - update 2n cell offset array*/
    	    	binaryFile.seek(504);
    	    	binaryFile.writeInt(2);
    	    	binaryFile.writeInt(keyDelimiter);
    	    	int newCountOfRecords = ph.incrementNoOfRecords(1);
    	    	//System.out.println("newCountOfRecords:"+newCountOfRecords);
    	    	ph.updateRecordStartOffset(1,504);
    	    	binaryFile.seek(4);
    	    	binaryFile.writeInt(512*(3-1));
    	    	binaryFile.seek(8);
    	    	binaryFile.writeShort(504);
    	    	
    	    	//update right sibling pointer of page2
    	    	binaryFile.seek(516);
    	    	binaryFile.writeInt(1024);
    	    	
    	    	//update cell offset array of page2
    	    	int numberOfRecords = ph.getNoOfRecords(2);
		    	//System.out.println("Number of Tables:"+numberOfRecords);
		    	int offset=0;
		    	int firstRecordOffset = 0;
    	    	for(int j=0;j<numberOfRecords;j++){
    	    		offset = 512+8+(2*j);
    	    		binaryFile.seek(offset);
        	    	int oldOffset = binaryFile.readShort();
        	    	//System.out.println("oldOffset:"+oldOffset);
        	    	binaryFile.seek(offset);
        	    	//System.out.println("newOffset:"+(oldOffset+512));
        	    	binaryFile.writeShort(oldOffset+512);
        	    	
    	    		firstRecordOffset=oldOffset+512;
    	    	}
    	    	//update cell start offset of page2
    	    	binaryFile.seek(512+2);
    	    	binaryFile.writeShort(firstRecordOffset);
    	    	
    			//System.out.println("right sib:"+getRightPointer(1));
    			//System.out.println("right sib:"+getRightPointer(2));
    			//System.out.println("right sib:"+getRightPointer(3));
    			
    	    	//displaying for debugging purpose
    	    	//System.out.println("Displaying file at last: "+ FileName);
    			//fh.displayBinaryHex(binaryFile);
    			
    	    	binaryFile.close();
    		}catch(Exception e){
    			   System.out.println(e);
    		}
    	}
    	else{
    		//add another 512 bytes to size
    		try{	
    			//System.out.println("root is not null & leaf is overflowing");
    	    	RandomAccessFile binaryFile = new RandomAccessFile(FileName, "rw");
    	    	
    	    	newLeafPageNumber = pageNumber+1;
    	    	//System.out.println("New leaf page:"+newLeafPageNumber);
    	    	
    	    	binaryFile.setLength(pageSize * newLeafPageNumber);
    	    	//add page header in page4
    	    	FileHandler fh = new FileHandler();
    	    	fh.writePageHeader(FileName,true,newLeafPageNumber);
    	    	
    	    	//Nead to update interior node - may overflow
    	    	int interiorNeededSpace = 10;
    	    	int interiorAvailableSpace = availablePageSpaceNonLeaf(parent);
    	    	if(interiorAvailableSpace < interiorNeededSpace){
    	    		//System.out.println("Parent is overflowing");
    	    		parent = handleMetaDataInteriorPageOverflow(fileName, parent);
    	    	}
    	    	//parent right offset points to new page
    	    	int parentRightChildOffset = ((parent-1)*512)+4;
    	    	//System.out.println("updating parent: "+parent+" @"+parentRightChildOffset+" value right most child pointer :"+((newLeafPageNumber-1)*512));
    	    	binaryFile.seek(parentRightChildOffset);
    	    	binaryFile.writeInt((newLeafPageNumber-1)*512);
    	    	
    	    	//current page sib points to new page
    	    	//System.out.println("updating current page: "+pageNumber+" right sib pointer");
    	    	int siblingPointer = ((pageNumber-1)*512)+4;
    	    	//System.out.println("siblingPointer offset:"+siblingPointer);
    	    	binaryFile.seek(siblingPointer);
    	    	//System.out.println("siblingPointer"+(newLeafPageNumber-1)*512);
    	    	binaryFile.writeInt((newLeafPageNumber-1)*512);
    	    	
    	    	//Writing into interior page - add current page as left child of parent
    	    	int cellStart = getRecordStartOffset(parent);
    	    	int offset = cellStart-8;
    	    	//System.out.println("updating parent: "+parent+" @"+offset);
    	    	binaryFile.seek(offset);
    	    	binaryFile.writeInt((pageNumber-1)*512);
    	    	binaryFile.seek(offset+4);
    	    	binaryFile.writeInt(keyDelimiter);
    	    	
    	    	//increase no of records in interior page
    	    	incrementNoOfRecords(parent);
    	    	
    	    	//update cell start offset in parent
    	    	updateRecordStartOffset(parent,offset);
    	    	
    	    	//update 2n array
    	    	int numberOfRecords = getNoOfRecords(parent);
    	    	int newOffset = getPageStartOffset(parent)+8+(2*(numberOfRecords-1));
    	    	binaryFile.seek(newOffset);
    	    	binaryFile.writeShort(offset);
    	    	
    	    	binaryFile.close();
    		}catch(Exception e){
    			   System.out.println(e);
    		}
    	}
    	return newLeafPageNumber;
    }
    public int handleMetaDataInteriorPageOverflow(String FileName, int pageNumber){
    	int newParent=-1;
    	if(pageNumber==1){
    		System.out.println("interior is root - and it is overflowing");
    	}
    	else{
    		System.out.println("interior is non-root is overflowing");
    	}
    	return newParent;
    }
    public int metaDataOverflowHandler(String FileName, int pageNumber){
    	//System.out.println("here FileName:"+FileName);
    	boolean isLeaf = isLeafPage(pageNumber);
    	int newPageNumber=0;
    	if(isLeaf){
    		//int maxKey = findMaxKeyInLeafPage(pageNumber);
    		//find parent with my page number(offset of page) - overflowing page will be rightmost child of my parent
    		int parent = findMetaDataParentPageNumber(FileName,pageNumber);
    		//System.out.println("Parent:"+parent);
    		//System.out.println("right most child:"+pageNumber);
    		newPageNumber=handleMetaDataLeafOverflow(fileName, pageNumber, parent);
    	}
    	/*
    	else{
    		//find max key
    		newPageNumber=handleMetaDataInteriorPageOverflow(fileName, pageNumber);
    	}*/
    	System.out.println("metaDataOverflowHandler returing newPageNumber:"+newPageNumber);
    	return newPageNumber;
    }
    
    public int getMaxCellOffet(int pageNumber){
    	
    	int celloffset = -1;
    	int numOfRecords =  getNoOfRecords(pageNumber);
    	//System.out.println("No of records:"+numOfRecords);
    	if (numOfRecords==0){
    		//System.out.println("No records");
    		return -1;
    	}
    	else{
    		try{
    			RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    			//System.out.println("fileName:"+fileName);
    			int offset = (pageNumber-1)*512+8 + ((numOfRecords-1)*2);
    			//System.out.println("offset:"+offset);
    			binaryFile.seek(offset);
    			celloffset = binaryFile.readShort();
    			binaryFile.close();
    		}catch(Exception e){
			   System.out.println(e);
    		}
    		//System.out.println("returning celloffset:"+celloffset);
    		return celloffset;
    	}
    }
    
    public int getMinCellOffet(int pageNumber){
    	int celloffset = -1;
    	int numOfRecords =  getNoOfRecords(pageNumber);
    	if (numOfRecords==0)
    		return -1;
    	else{
    		try{
    			RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    			binaryFile.seek(8);
    			celloffset = binaryFile.readShort();
    			binaryFile.close();
    		}catch(Exception e){
			   System.out.println(e);
    		}
    		
    		return celloffset;
    	}
    }
    
    public int getMiddleCellOffet(int pageNumber){
    	int celloffset = -1;
    	int numOfRecords =  getNoOfRecords(pageNumber);
    	int middle = numOfRecords/2;
    	if (numOfRecords==0)
    		return -1;
    	else{
    		try{
    			RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    			binaryFile.seek(8+((middle-1)*2));
    			celloffset = binaryFile.readShort();
    			binaryFile.close();
    		}catch(Exception e){
			   System.out.println(e);
    		}
    		
    		return celloffset;
    	}
    }
    
    public int findMiddleKeyInLeafPage(int pageNumber){
    	int middleKey=-1;
    	int offset = getMiddleCellOffet(pageNumber)+2;
    	if(offset!=-1){
    		try{
    			RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    			binaryFile.seek(offset);
    			middleKey = binaryFile.readInt();
    			binaryFile.close();
    		}catch(Exception e){
			   System.out.println(e);
    		}
    	}
    	return middleKey;    			
    }
    public int findMiddleKeyInNonLeafPage(int pageNumber){
    	int middleKey=-1;
    	int offset = getMiddleCellOffet(pageNumber)+4;
    	if(offset!=-1){
    		try{
    			RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    			binaryFile.seek(offset);
    			middleKey = binaryFile.readInt();
    			binaryFile.close();
    		}catch(Exception e){
			   System.out.println(e);
    		}
    	}
    	return middleKey;    			
    }
    
    public int findMaxKeyInLeafPage(int pageNumber){
    	int maxKey=-1;
    	int offset = getMaxCellOffet(pageNumber)+2;
    	if(offset!=-1){
    		try{
    			RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    			binaryFile.seek(offset);
    			maxKey = binaryFile.readInt();
    			binaryFile.close();
    		}catch(Exception e){
			   System.out.println(e);
    		}
    	}
    	return maxKey;    			
    }
   
    public int findMaxKeyInNonLeafPage(int pageNumber){
    	int maxKey=-1;
    	int offset = getMaxCellOffet(pageNumber)+4;
    	if(offset!=-1){
    		try{
    			RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    			binaryFile.seek(offset);
    			maxKey = binaryFile.readInt();
    			binaryFile.close();
    		}catch(Exception e){
			   System.out.println(e);
    		}
    	}
    	return maxKey;    			
    }
    
    //find the page number with key
    public int findPageNumber(int key, int pageNumber, boolean isMetaFile){
    	
    	int numOfRecords =-1;
    	int offset;
    	try{
			RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
			numOfRecords =  getNoOfRecords(pageNumber);
			int[] keyList = new int[numOfRecords];
			boolean isLeaf = isLeafPage(pageNumber);
			for ( int i=1; i<=numOfRecords; i++){
				offset = ((pageNumber-1)*512)+8+(2*(i-1));
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
			}
			
	    	if(isMetaFile==true){
	    		//System.out.println("Reached Leaf Page");
	    		for ( int i=0; i<numOfRecords; i++){
	    			if(keyList[i]==key){
	    				//System.out.println("Found the page number");
	    				binaryFile.close();
	    				return pageNumber;
	    			}
	    		}
	    		binaryFile.close();
	    		return pageNumber;
	    	}
	    	else{
	    		//System.out.println("Not a leaf page:"+pageNumber);
	    		//System.out.println("numOfRecords:"+numOfRecords);
	    		//System.out.println("keyList[0]:"+keyList[0]);
    			//System.out.println("key"+key);
	    		if(numOfRecords==1){
	    			if(keyList[0]>=key){
	    				
	    				offset = ((pageNumber-1)*512)+8+(2*0);
	    				//pageNumber = (keyList[i+1]);
	    				binaryFile.seek(offset+4);
	    				pageNumber=binaryFile.readInt();
	    				pageNumber=findPageNumber(key, pageNumber,isMetaFile);
	    			}
	    		}
	    		for ( int i=1; i<numOfRecords; i++){
	    			//System.out.println("keyList[i-1]:"+keyList[i-1]);
	    			//System.out.println("keyList[i]:"+keyList[i]);
	    			//System.out.println("key"+key);
	    			if((keyList[i-1]<key)&&(keyList[i]>=key)){
	    				//System.out.println("Recursive call");
	    				offset = ((pageNumber-1)*512)+8+(2*i);
	    				//pageNumber = (keyList[i+1]);
	    				binaryFile.seek(offset+4);
	    				pageNumber=binaryFile.readInt();
	    				pageNumber=findPageNumber(key, pageNumber,isMetaFile);
	    			}
	    		}
	    	}
	    	binaryFile.close();
		} catch(Exception e){
		   System.out.println(e);
		}
    	return -1;
    }
    //find the parent of a page
    public int findMetaDataParentPageNumber(String fileName, int pageNumber){
    	//System.out.println("Find parent of this page:"+pageNumber);
    	int currentPageNumber = 1;
    		try{
    			RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
    			//get right pointer
    			int rightChild = getRightPointer(currentPageNumber);
    			while(rightChild!=-1){
    				//System.out.println("Current page:"+currentPageNumber);
    				//System.out.println("rightChild:"+rightChild);
    				//System.out.println("Required page:"+pageNumber);
    				if(rightChild==((pageNumber-1)*512)){
    					binaryFile.close();
    					return currentPageNumber;
    				}
    				else{
    					currentPageNumber = (rightChild/512)+1;
    					rightChild = getRightPointer(currentPageNumber);
    				}
    			}
    			binaryFile.close();
    		} catch(Exception e){
    			System.out.println(e);
    		}
    	return -1;
    }
    
  //find the parent page number with key
    public int findSibling(int key, boolean isMetaFile){
    	int pageNumber = 1;
    	int siblingPage = -1;
    	pageNumber=findPageNumber(key,pageNumber,isMetaFile);
    	siblingPage=getRightPointer(pageNumber);
    	return siblingPage;
    }
 
    public int userDataOverflowHandler(String FileName, int pageNumber){
    	
    	boolean isLeaf = isLeafPage(pageNumber);
    	int newPageNumber=0;
    	if(isLeaf){
    		//newPageNumber=handleUserDataLeafOverflow(fileName, pageNumber);
    	}
    	else{
    		//newPageNumber=handleUserDataInteriorPageOverflow(fileName, pageNumber);
    	}
    	return newPageNumber;
    }

    public int findFirstLeafNode(int pageNumber){
    	int firstLeaf=-1;
		boolean isLeaf = isLeafPage(pageNumber);
		if(isLeaf)
			return pageNumber;
		else{
			//get first left child pointer
		    int offset = ((pageNumber-1)*512)+504;
		    //System.out.println("offset:"+offset);
		    try{
		    	RandomAccessFile binaryFile = new RandomAccessFile(fileName, "r");
		    	binaryFile.seek(offset);
		    	//pageNumber = ((binaryFile.readInt())/512)+1;
		    	pageNumber = binaryFile.readInt();
		    	//System.out.println("pageNumber:"+pageNumber);
		    	binaryFile.close();
		    	pageNumber = findFirstLeafNode(pageNumber);
		    	return pageNumber;
		    } catch(Exception e){
		    	System.out.println(e);
		    }
		}
		return firstLeaf;
    }
}