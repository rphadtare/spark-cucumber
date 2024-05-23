# spark-cucumber
Sample program for demo of spark program with cucumber framework

Employee dataframe with data - 
+-----------+-------------+-------+------+
|employee_id|employee_name|dept_id|salary|
+-----------+-------------+-------+------+
|101        |"Rohit P"    |10     |1000  |
|102        |"Pooja P"    |10     |1000  |
|103        |"Rutu M"     |10     |400   |
|104        |"Rushi M"    |20     |4000  |
|105        |"Prithvi D"  |20     |6000  |
|106        |"Rajani D"   |30     |10000 |
|107        |"Shrikant D" |30     |5000  |
|108        |"Rahul S"    |30     |3000  |
+-----------+-------------+-------+------+

Calculated average salary per department dataframe - 
+-------+----------------+
|dept_id|avg_sal_per_dept|
+-------+----------------+
|30     |6000.0          |
|20     |5000.0          |
|10     |800.0           |
+-------+----------------+

Expected average salary per department dataframe - 
+-------+----------------+
|dept_id|avg_sal_per_dept|
+-------+----------------+
|10     |800.0           |
|20     |5000.0          |
|30     |6000.0          |
+-------+----------------+
