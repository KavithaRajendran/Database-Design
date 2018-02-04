import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;

/**
 *   @author Kavitha Rajendran 
 *   @version 1.0
 */
public class FileHandler {
 /*
 *  The size of the page file will always be increments of this values
 *  Whenever the length of a RandomAccessFile is increased, the added space is padded with 0x00 value bytes.
 */
    Config conf = new Config();
    int pageSize = conf.getPageSize();
    
    public void writePageHeader(String fileName, boolean leaf, int pageNumber) {
        RandomAccessFile binaryFile;
        try {
            binaryFile = new RandomAccessFile(fileName, "rw");
            //calculate page size
            //int currentPages=(int)binaryFile.length()/pageSize;
            //pageNumber = currentPages+1;
            //System.out.println("New page number:"+pageNumber);
            //offset should be in begining of the page
            int offset = pageSize*(pageNumber-1);
            //System.out.println("header starting at:"+offset);
            try{
            binaryFile.seek(offset);
            //recordSize = flag(1 byte)+ number of Cells(1 byte) + Start of cell (2 bytes) + child/sibiling page pointer(4 bytes)
            //int recordSize = 8;
            int flag;
            if(leaf){
                //System.out.println("leaf node");
                flag=13;
            }
            else{
                //System.out.println("Interior node");
                flag=5;
            }
            //System.out.println("Writing page header");
            binaryFile.writeByte(flag);
            }catch (Exception e){
            	System.out.println(e);
            }
            
            //binaryFile.seek(offset);
            //System.out.println("Flag value:"+binaryFile.read());
            int noOfCells = 0;
            binaryFile.seek(offset+1);
            binaryFile.writeByte(noOfCells);
            //binaryFile.seek(offset+1);
            //System.out.println("No of cells:"+binaryFile.read());
            int cellPointer=(pageNumber*512);
            int pagePointer=-1;
            binaryFile.seek(offset+2);
            binaryFile.writeShort(cellPointer);
            binaryFile.seek(offset+4);
            binaryFile.writeInt(pagePointer);
            //binaryFile.seek(offset+4);
            
            //System.out.println("The file is now " + binaryFile.length() + " bytes long");
            //System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
            System.out.println();
            
            //System.out.println("Displaying file: "+ fileName);
            //displayBinaryHex(binaryFile);
            binaryFile.close();
        }
        catch (Exception e) {
            System.out.println("Unable to open " + fileName);
        }
    }

   public void writeLeafCell(String fileName) {
        RandomAccessFile binaryFile;
        try {
            //System.out.println("Writing cell header");
            binaryFile = new RandomAccessFile(fileName, "rw");
            PageHandler ph = new PageHandler(fileName);
            int pageNumber = ph.findCurrentPageNumber();
            //System.out.println("Leaf Page Number:"+pageNumber);
            //Write leaf header
            
            displayBinaryHex(binaryFile);
            binaryFile.close();

        }
        catch (Exception e) {
            System.out.println("Unable to open " + fileName);
        }
    }
   	
   //Write a record into a page
   public void writeRecord(String fileName){
	   PageHandler ph = new PageHandler(fileName);
	   int currentPage=ph.findCurrentPageNumber();
	   //starting of current page
	   int offsetStart = (currentPage-1)*512;
	   
	   try{
		   RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
		   binaryFile.seek(offsetStart);
		   int pageTypeOffset = binaryFile.readByte();
		   int cellStartOffset = offsetStart+2;
		   if (pageTypeOffset==13){
			   //System.out.println("Writing cell into leaf page");
			   short payloadSize=0;
			   writeLeafCellHeader(fileName, cellStartOffset,payloadSize);
		   }/*
		   else if (pageTypeOffset==5){
			   //System.out.println("Writing cell into interior page");
			   writeInteriorCellHeader(cellStartOffset);
		   }*/
		   binaryFile.close();
	   }catch(Exception e){
		   System.out.println(e);
	   }
	   
   }
   
   public void writeLeafCellHeader(String fileName, int cellStartOffset, short payloadSize){
	   //size of payload (excluding header) + row id
	   int leafCellHeaderSize = 6;
	   //512-6-20+1=487
	   int offset = cellStartOffset -( leafCellHeaderSize + payloadSize) + 1;
	   //int bytesNeeded = leafCellHeaderSize + payloadSize;
	   //page has enough space? cellStartOffset - last2n_offset > record payload = leaf header
	   //boolean sufficientSpace = checkLeafForSpace();
	   try{
		   RandomAccessFile binaryFile = new RandomAccessFile(fileName, "rw");
		   binaryFile.seek(offset);
		   binaryFile.close();
	   }catch(Exception e){
		   System.out.println(e);
	   }
   }

 /**
 *   This method is used for debugging and file analysis.
 *   @param raf is an instance of {@link RandomAccessFile}. 
 *   This method will display the contents of the file to Stanard Out (stdout)
 *   as hexadecimal byte values.
*/
    public void displayBinaryHex(RandomAccessFile raf) {
        try {
            System.out.println("Dec\tHex\t 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F");
            raf.seek(0);
            long size = raf.length();
            int row = 1;
            System.out.print("0000\t0x0000\t");
            while(raf.getFilePointer() < size) {
                System.out.print(String.format("%02X ", raf.readByte()));
                if(row % 16 == 0) {
                    System.out.println();
                    System.out.print(String.format("%04d\t0x%04X\t", row, row));
                }
                row++;
            }
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
}
