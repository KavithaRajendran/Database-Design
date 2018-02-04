<html>
<head>
<title> Book Search </title>
<div style="right:100px;" ><a href="menu.php">Home Page</a></div>
<hr/>
<h2> Search for book using ISBN, Title, Author </h2>
</head>

<body>

<form action="" method="post">
<table>
<tr>
    <td>ISBN</td>
    <td><input type="text" name="isbn"/></td>
</tr>
<tr>
    <td>Book Title</td>
    <td><input type="text" name="bookname"/></td>
</tr>
<tr>
    <td>Author</td>
    <td><input type="text" name="authorname"/></td>
</tr>
</table>
<input type="Submit" value="Search"/>
</form>

<?php
   include 'credential.php';
   if(isset($_POST['isbn']) or isset($_POST['bookname']) or isset($_POST['authorname']))
   {
   //Connect to MySQL Server
    $con = mysqli_connect($servername,$username,$password,$database);
	if (mysqli_connect_errno()) {
		die('Could not connect: ' . mysqli_error($con));
	}
	
   // Retrieve data from Query String
   $isbn = $_POST['isbn'];
   $title = $_POST['bookname'];
   $author = $_POST['authorname'];
   
   // Escape User Input to help prevent SQL Injection
   $isbn = mysqli_real_escape_string($con,$isbn);
   $title = mysqli_real_escape_string($con,$title);
   $author = mysqli_real_escape_string($con,$author);
   
   //build query
   if(!empty($_POST['isbn']))
   {
   $query="select b.isbn,b.title,a.name,b.availability from book b, authors a, book_authors ba where b.isbn=".$isbn." and b.isbn=ba.isbn and ba.author_id = a.author_id;";
   }
   else if((!empty($_POST['bookname'])) and (empty($_POST['authorname'])))
   {
   $query="select b.isbn,b.title,a.name,b.availability from book b, authors a, book_authors ba where b.title LIKE '%".$title."%' and b.isbn=ba.isbn and ba.author_id = a.author_id;";
   }
   else if((!empty($_POST['authorname'])) and (empty($_POST['bookname'])))
   {
	$query="select b.isbn,b.title,a.name,b.availability from book b, authors a, book_authors ba where a.name LIKE '%".$author."%' and b.isbn=ba.isbn and ba.author_id = a.author_id;";
   }
   else if( !empty($_POST['bookname']) and !empty($_POST['authorname']))
   {
	$query="select b.isbn,b.title,a.name,b.availability from book b, authors a, book_authors ba where b.title LIKE '%".$title."%' and a.name LIKE '%".$author."%' and b.isbn=ba.isbn and ba.author_id = a.author_id;";
   }
   else 
   {
	   echo "Please enter ISBN/BookName/AuthorName ";
	   exit;
   }
   echo "Query: " . $query . "<br />";
   
   //Execute query
   $qry_result = mysqli_query($con, $query) or die(mysqli_error($con));
   
   //Build Result String
   $display_string = '<table border="1">';
   $display_string .= "<tr>";
   $display_string .= "<th>ISBN</th>";
   $display_string .= "<th>Book</th>";
   $display_string .= "<th>Author</th>";
   $display_string .= "<th>Availability</th>";
   $display_string .= "</tr>";
   
   // Insert a new row in the table for each person returned
   while($row = mysqli_fetch_array($qry_result)) {
      $display_string .= "<tr>";
      $display_string .= "<td>$row[isbn]</td>";
      $display_string .= "<td>$row[title]</td>";
      $display_string .= "<td>$row[name]</td>";
	  $display_string .= "<td>$row[availability]</td>";
      $display_string .= "</tr>";
   }
   
   $display_string .= "</table>";
   echo $display_string;
   }
?>

</body>

</html>