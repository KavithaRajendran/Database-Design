<html>
<head>
<title> Book Checkout </title>
<div style="right:100px;" ><a href="menu.php">Home Page</a></div>
<hr/>
<h2> Book isbn & Borrower id needed to checkout a book </h2>
</head>

<body>
<div id="results" class="Holder">
</div>

<form action="" method="post">
<table>
<tr>
    <td>ISBN</td>
    <td><input type="text" name="isbn" /></td>
</tr>
<tr>
    <td>Book Name</td>
    <td><input type="text" name="bookname" /></td>
</tr>
<tr>
    <td>Borrower Card Id</td>
    <td><input type="text" name="card_id" required/></td>
</tr>
</table>
<input type="Submit" value="Search for checkout"/>
</form>

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
			xmlhttp.open("GET","updateCheckout.php?isbn_cardID="+value,true);  
			xmlhttp.send();
}
</script>

<?php
	include 'credential.php';
	if(isset($_POST['isbn']) or isset($_POST['bookname']) or isset($_POST['card_id']))
	{
	//Connect to MySQL Server
    $con = mysqli_connect($servername,$username,$password,$database);
	if ($con)
	{
	// Retrieve data from Query String
	$isbn = $_POST['isbn'];
	$card_id = $_POST['card_id'];
	$bookname = $_POST['bookname'];
   
	// Escape User Input to help prevent SQL Injection
	$isbn = mysqli_real_escape_string($con,$isbn);
	$card_id = mysqli_real_escape_string($con,$card_id);
	$bookname = mysqli_real_escape_string($con,$bookname);
   
    $date_out = date("Y-m-d");
	$more = strtotime("+14 day", time());
	$due_date = date('Y-m-d', $more);
	//$loan_id = abs(crc32(uniqid()));
	if(!empty($_POST['isbn'])) {
		$check = "SELECT isbn,title from book where isbn=".$isbn." and availability='available';";
	}
	else if (!empty($_POST['bookname'])) {
	//If a book is already checked out, then the checkout should fail and return a useful error message.
	$check = "SELECT isbn,title from book where title LIKE '%".$bookname."%' and availability='available';";
	}
	//echo "Check: " . $check . "<br />";
	if ($check_result=mysqli_query($con,$check)) {
		if($rowcount=mysqli_num_rows($check_result)) {
			//Display available books in table format
			$display_string = '<table border="1">';
			$display_string .= "<tr>";
			$display_string .= "<th>ISBN</th>";
			$display_string .= "<th>Book Title</th>";
			$display_string .= "<th>Select</th>";
			$display_string .= "</tr>";
			
			// Insert a new row in the table for each person returned
			while($row = mysqli_fetch_array($check_result)) {
				//$isbn = $row[isbn];
				//$title = $row[title];
				$display_string .= "<tr>";
				$display_string .= "<td>$row[isbn]</td>";
				$display_string .= "<td>$row[title]</td>";
				//<button onclick='checkout(".'"'.$isbn.'","'.$card_id.'"'.")' type='button'>Confirm</button>";
				//$display_string .= '<td> <button type="button" onclick="checkout('.'"'.'$row[isbn]'.'","'.$card_id.'")">CheckOut</button> </td>"';
				$display_string .= "<td> <button type='button' onclick='checkout(\"$row[isbn]\",\"$card_id\")'>CheckOut</button> </td>";
				$display_string .= "</tr>";
			}
			$display_string .= "</table>";
			echo $display_string;
		}
	}
	else{
		echo "Book is already checked out";
		exit;
	}
}	
}
?>
</body>
</html>