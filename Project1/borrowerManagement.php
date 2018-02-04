<html>
<head>
<title> Borrower Management </title>
<div style="right:100px;" ><a href="menu.php">Home Page</a></div>
<hr/>
<h2> Enter new borrower details to be added in database </h2>
</head>

<body>

<form action="" method="post">
<table>
<tr>
    <td>SSN</td>
    <td><input type="text" name="ssn" required/></td>
</tr>
<tr>
    <td>Borrower Name</td>
    <td><input type="text" name="bname" required/></td>
</tr>
<tr>
    <td>Address</td>
    <td><input type="text" name="address" required/></td>
</tr>
<tr>
    <td>Phone</td>
    <td><input type="text" name="phone"/></td>
</tr>
</table>
<input type="Submit" value="Create Account"/>
</form>


<?php
   include 'credential.php';
   if(isset($_POST['ssn']) and isset($_POST['bname']) and isset($_POST['address']))
   {
	//Connect to MySQL Server
    $con = mysqli_connect($servername,$username,$password,$database);
	if (mysqli_connect_errno()) {
		die('Could not connect: ' . mysqli_error($con));
	}
   
   // Retrieve data from Query String
   $ssn = $_POST['ssn'];
   $bname = $_POST['bname'];
   $address = $_POST['address'];
   
   // Escape User Input to help prevent SQL Injection
   $ssn = mysqli_real_escape_string($con,$ssn);
   $bname = mysqli_real_escape_string($con,$bname);
   $address = mysqli_real_escape_string($con,$address);
   //Generate unique id for card no
   //$card_no = 12;
   //If phone number not given, set NULL
   
   if(empty($_POST['phone'])) {
		//build query
		$query = "INSERT INTO BORROWER(Card_id, Ssn, Bname, Address, Phone) SELECT MAX(card_id)+1 , ".$ssn.", '".$bname."' ,'".$address."' ,NULL FROM BORROWER;";
   }
   else 
   {
		$phone = $_POST['phone'];
		$query = "INSERT INTO BORROWER(Card_id, Ssn, Bname, Address, Phone) SELECT MAX(card_id)+1 , ".$ssn.", '".$bname."' ,'".$address."' ,'".$phone."' FROM BORROWER;";
   }
   echo "Query: " . $query . "<br />";
   
   //Execute query
   $qry_result = mysqli_query($con, $query) or die(mysqli_error($con));
   if($qry_result)
	   echo "Account created successfully in database";
   }
?>

</body>

</html>