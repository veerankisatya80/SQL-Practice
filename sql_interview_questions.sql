 https://www.quora.com/What-is-the-toughest-SQL-query

 employee_id start_date end_date
 101         2017-01-01 2017-02-01
 101		 2017-01-15 2017-02-15
 101		 2017-01-22 2017-03-22
 102 		 2017-04-10 2017-05-01
 102		 2017-04-15 2017-05-03
 102		 2017-05-05 2017-07-15
 102		 2017-06-01 2017-07-01

required_result:

employee_id start_date end_date
 101         2017-01-01 2017-03-22
 102		 2017-04-10 2017-05-03
 102 		 2017-05-05 2017-07-15
 

 1) How do you find the Nth percentile
		select
		max(case when a.rownum/a.cnt<=0.9 then a.marks else null) as Nth_percentile
		select 
		(
			select
			marks,
			row_number() (over order by marks) as row_num
			from 
			table
		) a
		cross join
		(
		select
		count(*) as cnt
		from 
		table
		) b
		where 
		1=1

2) How to find the median.

		select
		avg(marks)
		select 
		(
			select
			marks,
			row_number() (over order by marks) as row_num
			from 
			table
		) a
		cross join
		(
		select
		count(*) as cnt
		from 
		table
		) b
		where
		row_num  between (cnt+1)/2 and (cnt+2)/2

3) How to find sum of sales by Category and sub category.

Table:

Customer ID, Product ID, Category ID, sub Category ID, Sales
kartik, P001, C001, S001, 1000

Output:

Customer ID, Categorgy ID, Sub Category ID, Sales, Sales by Category, Sales by Sub Category.

		Select
		customer_id,
		category_id,
		sub_category_id,
		sales,
		sum(sales) over(partition by category_id) as sales_by_category,
		sum(sales) over(partition by sub_category_id) as sales_by_sub_category
		from
		Table;
		
4) Given below data.

Table:

Order_id, order_date, sales

output:

order_date,sales, sales_begining_of_month_to_current_date

		select
		order_date,
		sum(sales) over (partion by order_date) as sales_current_date_sales,
		sum(sales) over(partion by month_begin_date order by order_date asc rows between current_row and unbounded preceeding) as sales_month_to_date
		from
		table1 a,
		dates_table b
		where
		a.order_date=b.calendar_date;

5) Given below data find the manager of the employee, if there is no manager, print "no manager"

Employee ID, employee Name, manager_id

		select
		a.employee_name,
		case when b.employee_id is null then "No Manager" else b.employee_name end as Manager_name
		from
		Table1 a
		left join
		Table1 b
		on b.employee_id=a.manager_id

6) Given the below data, find how many employees where in the building at 10:30 AM

Employee_ID, event_type, event_time,
A    IN        06:00
A    OUT       07:14
B    IN        07:25
C    IN        08:45
B    OUT       10:01
D    IN        10:30
A    IN        10:32
B    IN        11:05
C    OUT       12:00

		select
		employee_id
		from
		(
			select 
			employee_id,
			in_time,
			out_time
			from 
			(
				select
				employee_id,
				event_type,
				event_time as in_time
				lead(event_time) over(partition by employee_id order by event_time asc) as out_event_time
				from
				table1
			) T1
			where T.event_type = 'IN'
		) T2
		where to_date('2015-10-05 10:30:00') between in_time and out_time;
		
7) Write ETL for SCD2

Source working Table: source_table_w
Target Table: target_table_d

		create temp table new_records_v as (
		select
		a.*
		from source_table_w a
		left join
		target_table_d b
		on a.col1=b.col1 
		where b.col1 is null
		);

		create temp table existing_records_v as (
		select
		a.*
		from source_table_w a
		inner join
		target_table_d b
		on a.col1=b.col1 
		);

		update target_table_d 
		from existing_recors_v a
		set end_date=current_timestamp
		where a.col1=col1;

		insert into target_table_d
		select
		a.col1
		curent_date as start_date,
		'2099-12-31' as end_date
		from
		existing_recors_v a
		union
		select
		a.col1
		curent_date as start_date,
		'2099-12-31' as end_date
		from
		new_records_v a

8) Write the ETL for SCD3 

example for SCD3:

customer_id, first_order, last_order

		create temp table first_order_v as (
			select
			customer_id,
			order_id,
			order_date
			from
			(
				select
				customer_id,
				order_id,
				order_date,
				row_number() over(partition by customer_id order by order_date asc) as row_num
				from
				orders_table
			) T
			where T.row_num=1
		);

		create temp table last_order_v as (
			select
			customer_id,
			order_id,
			order_date
			from
			(
				select
				customer_id,
				order_id,
				order_date,
				row_number() over(partition by customer_id order by order_date desc) as row_num
				from
				orders_table
			) T
			where T.row_num=1
		);

		select
		a.customer_id,
		a.order_id as first_order_id,
		a.order_date as first_order_date,
		b.order_id as last_order_id,
		b.order_date as last_order_date
		from
		first_order_v a
		inner jon
		last_order_v b
		on a.customer_id=b.customer_id;

9) Write ETL for late arriving dimensions.

Dimension table: products_table
Fact_table: target_fact_table
late_arriving_temporary table: late_fact_table
source working table: source_table_w

		insert into target_fact_table
		select
		coalesce(b.product_id,-9999) as product_id,
		a.metric_data
		from
		sourc_table_w a
		left join
		products_table b
		where
		a.product_name=b.product_name;

		insert into target_fact_table
		select
		b.product_id as product_id,
		a.metric_data
		from
		late_fact_table a
		inner join
		products_table b
		where
		a.product_name=b.product_name;

		insert into target_fact_table
		select
		-999 as product_id,
		(-1)* a.metric_data as metric_data
		from
		late_fact_table a
		inner join
		products_table b
		where
		a.product_name=b.product_name;

		delete from late_fact_table
		where product_name in (select product_name 
		from
		late_fact_table a
		inner join
		products_table b
		where
		a.product_name=b.product_name);

		insert into late_fact_table
		select
		a.product_name,
		a.metri_data
		from
		source_table_w a
		left join
		product_table b
		on a.product_name=b.product_name
		where b.product_name is null;
		
10) Given the below data, find the 2 actors (male and female both) who have worked together in 2 or more movies.

actor_id, actor_name, actor_gender, movie_id, movie_name
101, kartik, M, M01, ghayal
102, katrina, F, M01, Ghayal

		create temp table male_actor_v as (
		select
		actor_id,
		actor_name,
		movie_id,
		movie_name
		from
		actors where actor_gender='M'
		);

		create temp table female_actor_v as (
		select
		actor_id,
		actor_name,
		movie_id,
		movie_name
		from
		actors where actor_gender='F'
		);

		select
		a.actor_name as male_actor,
		b.actor_name as female_actor,
		count(*) as cnt
		from
		male_actor_v a
		inner join
		female_actor_v b
		where
		a.movie_id=b.movie_id
		group by 1,2
		having count(*)>=2;
		
11) Find the employees whose salary is more than the average sales in their respective departments.

employee_id, employee_name, department_id, salary

		select
		a.employee_id,
		a.employee_name,
		a.department_id,
		a.salary,
		b.avg_dept_salary
		from
		employee_table a,
		(
		select
		department_id,
		avg(salary) as avg_dept_salary
		from employee_table
		group by department_id
		) b
		where
		a.department_id=b.department_id 
		and a.salary>b.avg_dept_salary;
		
12) Find the 2nd highest Salary.

		select
		max(a.salary)
		from
		employee_table a
		(
		select
		max(salary) as max_salary
		from employee_table ) b
		where a.salary<max_salary

13) Given the below data, find the following. Students are allowed to take either of 2 subjects i.e. maths or science

Student_id, student_name, subject_id, subject_name

	Find the students who have taken only Maths or Science and NOT both.
	
	select
	a.student_id,
	a.student_name
	from
	(
	select
	student_id,
	student_name,
	sum(case when subject_name='Maths' then 1
		 when subject_name='Science' then 1
		 else 0 end) as flag_cnt
	from
	students_table
	) a
	where flag_cnt=1;
	
	Find the students who have taken both Maths and Science.
	
	select
	a.student_id,
	a.student_name
	from
	(
	select
	student_id,
	student_name,
	sum(case when subject_name='Maths' then 1
		 when subject_name='Science' then 1
		 else 0 end) as flag_cnt
	from
	students_table
	) a
	where flag_cnt=2;
	
14) Lets say we have two tables, one maps product to color
and the other table maps stores to products.

SAMPLE DATA:
TABLE 1          TABLE 2
---------------  --------------
PRODUCT COLOR    STORE PRODUCT
A       YELLOW   1     A
A       BLUE     1     C
B       ORANGE   1     E
C       YELLOW   2     A
D       BLUE     2     B
D       BLACK    3     D
E       YELLOW   3     E



How many stores have yellow color products?

Which store has the most number of colors of any stores?

What is the maximum number of colors any store carries?

select count(*) from (
select
store_id
from
table1 a,
table2 b
where
a.product=b.product and color='YELLOW'
group by 1)

select
store_id
from
(
	select
	store_id,
	row_number() over(order by color_cnt desc) as row_num
	(
		select 
		store_id, 
		count(*) over(partition by store_id) as color_cnt
		from 
		(
			select
			store_id,
			color
			from
			table1 a,
			table2 b
			where
			a.product=b.product
			group by 1,2
		) T1
	) T2
) where T2.row_num=1;

/* My query*/

select distinct(store) from 
(
select store, rank() over(order by count desc) as rank
from 
(
select B.store, A.color,count(store) over(partition by store) as count 
from Table1 A, table2 B
where A.product= B.product
)

X
) Y
where rank = 1


15) There are 2 tables with entries of Apples and Oranges sold each day. Write a query to get difference between apples and oranges sold each day.

Fruit, sale_date, sales_amount
Apple, Sep 1st 2015, 100
oranges, sep 1st 2015, 200

		select
		a.sale_date,
		a.sales_amt-b.sales_amt as diff_amt
		from
		(
		select
		sale_date,
		sales_amt
		from
		fruits_table
		where fruit='Apples'
		) a,
		(
		select
		sale_date,
		sales_amt
		from
		fruits_table
		where fruit='Oranges'
		) b
		where
		a.sale_date=b.sale_date;
		
16) Given the below data, flatten out the data in one row.

Cusomer_id, event_type, event_time,
101, 'UBER BOOKED', Sep 1st 10:00 AM
101, 'UBER Confirmed', Sep 1st 10:05 AM
101, 'UBER Arrived', Sep 1st 10:10 AM
101, 'UBER Dropped', Sep 1st 10:30 AM

		select
		customer_id,
		max(case when event_type='UBER BOOKED' then event_time end) as Uber_booked,
		max(case when event_type='UBER Confirmed' then event_time end) as Uber_Confirmed,
		max(case when event_type='UBER Arrived' then event_time end) as Uber_Arrived,
		max(case when event_type='UBER Dropped' then event_time end) as Uber_Dropped
		from
		Uber_table
		group by 1;

17) Difference between UNION, UNION ALL, INTERSECT

UNION -- eliminates duplicates while used in query. if above and below query between union have the same record only 1 will appear in resultset. Even if one of the queries in have a record it will appear. Can be inefficient as duplicate check is done.
UNION ALL -- Does not do the duplicates check and will result in data from both queries between union all. Efficient as no duplicate check is required
INTERSECT -- Will only display those records who are present in the resultset of above and below query. Records which are present in only one of the queries will not be displayed.

18) How to you replicate row number without using the row_number() window function

Contemporary SQL:

		select
		name
		row_number() over(order by name asc) as row_num
		from
		table1;
		

Without row_number() window function:

		select
		a.name,
		(select count(*) from table1 b where a.name>=b.name) as row_num
		from
		table1 a;
		
19) Given the below data, find out the number of tasks and the time taken to finish every task.

		create table sayee_v
		(
		start_date date,
		end_date date
		)

		insert into sayee_v values('2016-09-01','2016-09-02');
		insert into sayee_v values('2016-09-02','2016-09-03');
		insert into sayee_v values('2016-09-03','2016-09-04');

		insert into sayee_v values('2016-09-05','2016-09-06');
		insert into sayee_v values('2016-09-06','2016-09-07');
		insert into sayee_v values('2016-09-07','2016-09-08');

		insert into sayee_v values('2016-09-10','2016-09-11');
		insert into sayee_v values('2016-09-11','2016-09-12');

		select
		min(start_date) as start_date,
		max(end_date) as end_date,
		max(end_date)-min(start_date) as diff
		from
		(
			select
			start_date,
			end_date,
			row_number() over(order by end_date) as row_num,
			end_date - row_number() over(order by end_date) as tmp
			from sayee_v
		) t
		group by t.tmp;

20) Given the below table, give me the list of all objects which are associated with all tags.

Objects table - has object ID and object name.
Tags Table: has tag ID and Tag name
Bridge table: mapping relation between tag and objects

		select
		T1.object_id,
		T1.object_name
		from
		(
			select
			a.object_id,
			a.object_name,
			count(*) as tag_count
			from
			objects a,
			tags b,
			bridge c
			where
			a.object_id=c.object_id and
			b.tag_id=c.tag_id
		) T1,
		(
			select
			count(*) as tag_count
			from
			tags
		) T2
		where
		T1.tag_count=T2.tag_count;
		
21) Given thebelow tables, find the list of authors for which the books count dont match with the books table.

			Books
           ------------------
           Book_id (PK)
           book_name
           Author_id (FK to authors.author_id)
           published_date
           number_of_words
           number_pages
           number_of_sales

           Authors
           -------------------
           Author_id (PK)
           author_name
           Number_of_books_written


		   select T1.autho_id
		   (
		   select
			author_id,
			count(*) as book_cnt
			from
			books
			) T1,
			authros T2
			where T1.author_id=T2.author_id and
			T1.book_cnt<>T2.number_of_books_written;

			select * from (
			select 
			T1.autor_id 
			case when T2. autorr__id is not null and T1.book_cnt<> T2.Number_of_books_written then ‘NOT matching’ 
			when T2.author_id is null “NOt matching”
			else ‘Mching’ 
			End as tmp

			from
			(select
			author_id,
			count(*) as book_cnt
			from
			books
			group by 1
			) T1
			left join 
			authors T2
			on T1. author_id=T2.auhor_id 
			where
			T1.author_id=T2.author_id )  Fnl where fnl.tmp in “NoT matchin”

21) Give me the list of all the subordinates under one manager. Say a VP, give me all his subordinates.


			select T1.employee_id, T1.employee_name  from (
			select
			employee_id,
			employee_name,
			case when b.employe_id is null then “no manager” else b.emloyeename end as “manager_name”
			from
			employees a
			left jin 
			employees b
			on b.employee_id=a.employee_supervisor
			 ) T1 where T1.manager_name =’Jorie Jhonson”
			 
			 recursive
			 
			 with emp_temp(employee_id,employee_name,manager_id, manager_name)
			 (
			 select employee_id, employee_name,manager_id, employee_name
			 from
			 employees where employee_id=manager_id
			 union all
			 select
			 a.employee_id, a.employee_name, a.manager_id, b.employee_name as manager_name
			 from
			 employees a,
			 emp_temp b
			 where
			 a.manager_id=b.employee_id 
			 )
			 select * from emp_temp where manager_name='Jorie Johonson';
			 
22) Given the below data, explore it for every start and end point. meaning replicate the rows.

city start_ip end_ip
seattle 	100		200
portland 	300		400
San Jose	500		700

		select city,explode_col from (
		select
		city,
		case when start_ip=row_rnk-1 then start_ip 
			 when row_rnk<=end_ip then row_rnk
			 else null end as explode_col
		from
		(
		select	
		city,
		start_ip,
		end_ip,
		start_ip+sum(1) over(partition by city rows unbounded preceding) as row_rnk
		from
		kartik_v
		cross join
		amzrep_ddl.o_reporting_days
		) a 			 
		) b where b.explode_col is not null;
		
23) Each customer account is managed by  managers. One account may be managed by multiple  managers. 
The  managers will share revenue for the corresponding period if they manage the same account. 
Each manager has a ratio, start_date, and end_date for the account. The ratio is used to compute the revenue for the date range.
For example , multiple managers manage the same account, manager_i has a ratio: 1/m,  m is the number of managers manage the same account for that period. 
If the manager does not share the same date range with other managers, the ratio is 1.

Given an input table AccountManagers( manager_id, account_id, start_date,  end_date), 
Compute the ratio for each manager for all date ranges.
The output table definition is
RecomputedRevenueDistributions( manager_id, account_id,start_date,  end_date,   ratio)

Example
input:
manager_id,account_id, start_date,  end_date        
2,         1,         2016-07-10, 2016-07-20        
3,         1,         2016-07-18, 2016-07-25        
3,         2,         2016-08-01, 2016-08-25        
5,         2,         2016-09-20, 2016-09-30

Output:
2, 1, 2016-07-10, 2016-07-17, 1       //ratio is  1/1=1
2, 1, 2016-07-18, 2016-07-20, 1/2        //ratio  1/2
3, 1, 2016-07-18, 2016-07-20, 1/2     //ratio  1/2
3, 1, 2016-07-21, 2016-07-25, 1       //ratio 1.
3, 2, 2016-08-01, 2016-08-25,  1      // ratio 1
5, 2, 2016-09-20, 2016-09-30, 1       // ratio 1
			 
		create temp table explode_data_v as (
		select 
		manager_id,
		account_id,
		start_date,
		end_date,
		explore_data as data_date
		from
		(
			select
			manager_id,
			account_id,
			case when start_date=row_date-1 then start_date 
				 when row_date<=end_date then row_date
				 else null end as explode_data
			from
			(
				select
				manager_id,
				account_id,
				start_date,
				end_date,
				start_date + sum(1) over(partition by account_id,manager_id rows unbounded preceding) as row_date
				from
				table a
				cross join
				system_table b
				where 1=1
			) a
		) b
		where b.explode_data is not null
		);

		create temp table tmp_v1 as (
		select 
		manager_id,
		account_id,
		start_date,
		end_date,
		data_date,
		count(*) over(partition by account_id,data_date) as manager_cnt
		from
		explore_data_v
		);

		create temp table tmp_v2 as (
		select 
		manager_id,
		account_id,
		min(data_date) over(partition by accont_id,manager_id,manager_cnt) as start_date,
		max(data_date) over(partition by accont_id,manager_id,manager_cnt) as end_date,
		manager_cnt
		from
		tmp_v1
		);

		create temp table final_v as (
		select 
		manager_id,
		account_id,
		start_date,
		end_date,
		1/manager_cnt as ratio
		from
		tmp_v1
		group by 1,2,3,4,5
		);

/** Satyas answer**/
		select 
		manager_id,
		account_id,
		min(explode_date) over(partition by account_id,manager_id,manager_cnt) as start_date,
		max(explode_date) over(partition by account_id,manager_id,manager_cnt) as end_date,
		manager_cnt
        from 
(
select manager_id,account_id,start_date,end_date ,explode_date,
sum(1) over(partition by account_id,explode_date) as manager_cnt
from
(
select manager_id,account_id,start_date,end_date ,
      case when  datediff(day,start_date,row_date)=1 then start_date
           when row_date <= end_date then row_date
           else null end as explode_date
from 
(
select manager_id,account_id,start_date,end_date ,
   dateadd(day, sum(1) over(partition by account_id order by manager_id rows unbounded preceding),start_date) as row_date
   from AccountManagers ) A
 ) B
  ) C
  /******************************************************************/

24) There is professor table, cources table and students table. 
Then there is one fact less fact table which has keys of professor, cources and students. Find the top 10 courses taken by top 10 professors

		create temp table tmp1_v as (
			sselect
			professor_id,
			course_id,
			count(distinct student_id) over(partition by course_id) as num_of_studs_by_course,
			count(distinct course_id) over(partition by professor_id) as num_of_prof_by_course
			from
			factless_fact_table
			);

		create temp table tmp2_v as (
			select
			professor_id,
			course_id,
			num_of_studs_by_course,
			num_of_prof_by_course
			from
			tmp1_v 
			group by 1,2,3,4
			);	

		create temp table tmp3_v as (
			select
			professor_id,
			course_id,
			row_number() over( order by num_of_studs_by_course desc) as row_num_studs_by_course,
			row_number() over( order by num_of_prof_by_course desc) as row_num_prof_by_course
			from
			tmp2_v 
			group by 1,2,3,4
			);	
			
			select
			professor_id,
			course_id
			from
			tmp3_v where row_num_studs_by_course<=10 and row_num_prof_by_course<=10;

25) Given the below table, rank the salary of employees in each depart in descing and rank commission of employees in each dept in ascending

Employee_id, employee_name, Dept_id, salary, commision

		select
		employee_id,
		employee_name,
		dept_id,
		salary,
		commision,
		row_number() over(partition by dept_id order by salary desc) as row_num_salary_rank_by_dept,
		row_number() over(partition by dept_id order by commision asc) as row_num_commision_rank_by_dept
		from
		employees_table
		
26) Given two tables Friend_request (requester_id, sent_to_id, time) Request_accepted (acceptor_id, requestor_id, time) Find the overall acceptance rate of requests
Assumption: It can take upto one week to accept a request.

Solution-1:

create temp table friends_denorm_v as (
select
a.time as frnd_req_sent_time,
a.requester_id,
a.sent_to_id,
b.time as frnd_req_acpt_time
from
friend_request a
left join
request_accepted b
on a.requester_id=b.requester_id and
a.sent_to_id=b.acceptor_id
);

27) Below is the data in the table. Find the rollwing 2 days sum.

drop table kartik_v;
create  table kartik_v (
order_date date,
sales bigint
);

insert into kartik_v values('2016-11-01',100);
insert into kartik_v values('2016-11-02',800);
insert into kartik_v values('2016-11-03',200);
insert into kartik_v values('2016-11-04',100);
insert into kartik_v values('2016-11-05',900);
insert into kartik_v values('2016-11-06',800);
insert into kartik_v values('2016-11-07',400);
insert into kartik_v values('2016-11-08',300);
insert into kartik_v values('2016-11-09',500);
insert into kartik_v values('2016-11-10',400);

select
b.calendar_day,
sum(sales)
from
kartik_v a,
amzrep_ddl.o_reporting_days b
where
a.order_date between b.calendar_day -1 and b.calendar_day
group by 1

28) Second hightest salary in every department

select
a.department_id,
max(salary) as max_salary
from
employee a
(select
department_id,
max(salary) as max_sal
from
employees) b
where 
a.department_id=b.department_id and
a.salary<max_sal
group by 1

select
a.department_id,
max(a.salary) as max_salary
from
employees a
where a.salary <(select max(salary) from employees b where a.department_id=b.department_id
group by 1

28) Given the below table, get all actor who has acted in maximum number of movies.

Movie_id, City, Location_id, Actor_1, Actor_2, Actor_3

select
actor,
sum(cnt) as movies_count
from
(
	select
	actor_1 as actor,
	count(*) as cnt
	from 
	movies
	group by 1

	union all

	select
	actor_2 as actor,
	count(*) as cnt
	from 
	movies
	group by 1

	union all

	select
	actor_3 as actor,
	count(*) as cnt
	from 
	movies
	group by 1
) T1
group by 1
order by sum(cnt) desc;

A table schema with tables like employee, department, employee_to_projects, projects

1) Select employee from departments where max salary of the department is 40k
2) Select employee assigned to projects
3) Select employee which have the max salary in a given department
4) Select employee with second highest salary
5) Table has two data entries every day for # of apples and oranges sold. write a query to get the difference between the apples… 
Python Questions -

1) Print Max element of a given list
2) Print median of a given list
3) Print the first nonrecurring element in a list
4) Print the most recurring element in a list
5) Greatest common Factor  

