<?php
include 'credential.php';
?>

<?php
if(isset($_GET['isbn_cardID']))
{
	$value = $_GET['isbn_cardID'];
	$arr =  explode(" ", $value);
	$isbn = $arr[0];
	$card_id= $arr[1];
	//Connect to MySQL Server
	$con = mysqli_connect($servername,$username,$password,$database);
	if ($con)
	{
		//check max book limit
		$book_limit = "SELECT card_id from book_loans where card_id=".$card_id.";";
		//echo "Book limit check: " . $book_limit . "<br />";
		if ($limit_result=mysqli_query($con,$book_limit)) {
			$count = mysqli_num_rows($limit_result);
			//echo "count ".$count;
			if($count>=3) {
				echo "Borrower already have 3 books";
				exit;
			}
			$sql_query = "SELECT max(loan_id) FROM book_loans";
			$result = mysqli_query ($con, $sql_query)  or die(mysqli_error($con));  
			if($result == false)
			{
				echo 'The query by ISBN failed.';
				exit();
			}
			echo "checking out";
			$row = mysqli_fetch_array($result);
			$id = $row["max(loan_id)"];
			echo "id:"+$id;
			$id = $id+1;
			echo "id:"+$id;
			$sql_query = "select title from book where isbn='$isbn' and availability='Available'";
			$result1 = mysqli_query ($con, $sql_query)  or die(mysqli_error($link));  
			$res_count = mysqli_num_rows($result);
			echo "res_count ".$res_count;
			if($res_count!=0) {
			
			$sql_query = "INSERT INTO book_loans(loan_id,isbn,card_id,date_out,due_date,date_in) VALUES ('$id','$isbn','$card_id',CURDATE(),DATE_ADD(CURDATE(),INTERVAL 14 DAY),null)";
			//$query = "INSERT INTO BOOK_LOANS(loan_id,isbn,card_id,date_out,due_date,date_in) VALUES (".$id.",".$isbn.",".$card_id.",'".$date_out."','".$due_date."',NULL from book_loans;";
			echo "Query: " . $sql_query . "<br />";
			if ($checkout_result=mysqli_query($con,$sql_query)) {
				//Execute query to update availability in book table
				$update_query = "UPDATE book set availability='Checkedout' where isbn='$isbn';";
				echo "update query: " . $update_query . "<br />";
				if ($checkout_result=mysqli_query($con,$update_query)) {
					//echo "checkedout successfully";
					header("Location:successfulCheckout.php");
				}
			}
			else {
				//echo "Check out failed";
				header("Location:failedCheckout.php");
		
			}
			}
		}
	}
}	
?>
