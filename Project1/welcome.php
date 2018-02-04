<html>
<title> Library Management System </title>
<body>
<h2> Welcome to Library Management System </h2>
<h3>Admin Login</h3>
<form action="" method="post">
<table>
<tr>
    <td>Username</td>
    <td><input type="text" name="username" required/></td>
</tr>
<tr>
    <td>Password</td>
    <td><input type="text" name="password" required/></td>
</tr>
</table>

<input type="Submit" value="Login"/>
</form>

<?php
include 'credential.php';
//session_start();
if(isset($_POST['username']) and isset($_POST['password']))
{
	$username = $_POST['username'];
	$password = $_POST['password'];

	// Connect to mysql with given credentials
	$conn = mysqli_connect($servername,$username,$password) or die ("unable to connect to mysql");
	
	//select library schema
	mysqli_select_db($conn, $database) or die ("Unable to select library database");
	
	//closing the session
	//$conn->close();
	echo "<script>window.open('menu.php','_self')</script>";
}
?>

</body>
</html>
