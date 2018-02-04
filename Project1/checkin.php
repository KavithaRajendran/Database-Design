<html>
<head>
<title> Book Checkin </title>
<div style="right:100px;" ><a href="menu.php">Home Page</a></div>
<hr/>
<h2> ISBN or Borrower id or Borrower Name needed to checkin a book </h2>
</head>

<body>
<div id="results" class="Holder">
</div>
<script>
function checkout(value1, value2)
{	
	value = value1 + " " + value2;
			var xmlhttp="";
			if (window.XMLHttpRequest)
			{  
				xmlhttp=new XMLHttpRequest();
			} 	
			else 
			{
				xmlhttp=new ActiveXObject("Microsoft.XMLHTTP"); 
			}		
			xmlhttp.onreadystatechange=function()
			{
				if (xmlhttp.readyState==4 && xmlhttp.status==200)
				{
					document.getElementById("results").innerHTML=xmlhttp.responseText;   
				}		  
			}  
			xmlhttp.open("GET","updateCheckin.php?isbn_cardID="+value,true);  
			xmlhttp.send();
}
</script>
<form action="" method="post">
<table>
<tr>
    <td>ISBN</td>
    <td><input type="text" name="isbn"/></td>
</tr>
<tr>
    <td>Book Name</td>
    <td><input type="text" name="bookname" /></td>
</tr>
<tr>
    <td>Borrower Card Id</td>
    <td><input type="text" name="card_id" required/></td>
</tr>
<tr>
    <td>Borrower Name</td>
    <td><input type="text" name="Bname" /></td>
</tr>
</table>
<input type="Submit" value="Search for checkedout books"/>
</form>

<?php
   include 'credential.php';
   //if(isset($_POST['isbn']) or isset($_POST['card_id']) or isset($_POST['Bname']) or isset($_POST['bookname']))
	if(isset($_POST['isbn']) or isset($_POST['bookname']))
	{
		//Connect to MySQL Server
		$con = mysqli_connect($servername,$username,$password,$database);
		if($con)
		{
		// Retrieve data from Query String
		$isbn = $_POST['isbn'];
		$card_id = $_POST['card_id'];
		//$Bname = $_POST['Bname'];
		$bookname = $_POST['bookname'];
   
		// Escape User Input to help prevent SQL Injection
		$isbn = mysqli_real_escape_string($con,$isbn);
		$card_id = mysqli_real_escape_string($con,$card_id);
		//$Bname = mysqli_real_escape_string($con,$Bname);
		$bookname = mysqli_real_escape_string($con,$bookname);
   
		//check in date - today's date
		$date_in = date("Y-m-d");
   
		//$query = "select loan_id,isbn,card_id,date_out,due_date,date_in,Bname from book_loans as bl,borrower as b where isbn=".$isbn." or card_id=".$card_id." or Bname like '%".$Bname."%';";
		//build query
		if(!empty($_POST['isbn'])) {
			$query = "select b.title,b.isbn,bl.card_id,date_out,due_date,date_in from book as b, book_loans as bl where bl.isbn=".$isbn." and bl.isbn=b.isbn;";
		}
		else if(!empty($_POST['bookname'])){
			$query = "select b.title,b.isbn,bl.card_id,date_out,due_date,date_in from book as b, book_loans as bl where b.title like '%".$bookname."%' and bl.isbn=b.isbn;";
		}
		
		else {
			echo "Please enter ISBN/BookName ";
			exit;
		}
		echo "Query: " . $query . "<br />";
		//Execute query
		$qry_result = mysqli_query($con, $query) or die(mysqli_error($con));
   
		//Build Result String
		$display_string = '<table border="1">';
		$display_string .= "<tr>";
		//$display_string .= "<th>Loan Id</th>";
		$display_string .= "<th>ISBN</th>";
		$display_string .= "<th>Borrower Card Id</th>";
		$display_string .= "<th>Checkedout date</th>";
		$display_string .= "<th>Due Date</th>";
		$display_string .= "<th>Date in</th>";
		//$display_string .= "<th>Borrower Name</th>";
		$display_string .= "<th>Select</th>";
		$display_string .= "</tr>";
   
		// Insert a new row in the table for each person returned
		while($row = mysqli_fetch_array($qry_result)) {
			$display_string .= "<tr>";
			//$display_string .= "<td>$row[loan_id]</td>";
			$display_string .= "<td>$row[isbn]</td>";
			$display_string .= "<td>$row[card_id]</td>";
			$display_string .= "<td>$row[date_out]</td>";
			$display_string .= "<td>$row[due_date]</td>";
			$display_string .= "<td>$row[date_in]</td>";
			//$display_string .= "<td>$row[Bname]</td>";
			//$display_string .= '<td> <button type="button" onclick="checkin()";>CHECK IN</button> </td>';
			$display_string .= "<td> <button type='button' onclick='checkin(\"$row[isbn]\",\"$card_id\")'>CheckIn</button> </td>";
			$display_string .= "</tr>";
		}
		$display_string .= "</table>";
		echo $display_string;
		}
	}
?>

</body>
</html>