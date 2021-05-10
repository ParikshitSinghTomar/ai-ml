SELECT * FROM market_star_schema.market_fact_full;

select count(*) as Total_Customers 
from cust_dimen;

select Customer_Name AS 'Customrer Full Name', City 'Customer City', State
from cust_dimen;


select Customer_Name AS 'Customrer Full Name', City 'Customer City', State
from cust_dimen
where state='West Bengal';

select Customer_Name AS 'Customrer Full Name', City 'Customer City', State
from cust_dimen
where state='West Bengal' and City='Kolkata';

select count(*) as Bengal_Customers 
from cust_dimen
where state='West Bengal';

select count(*) as Maharashtra_Customers 
from cust_dimen
where state='Maharashtra' and Customer_Segment='CORPORATE';

select * 
from cust_dimen
where State in ('Tamil Nadu','Karnatka','Telangana','Kerla');


select * 
from cust_dimen
where Customer_Segment !='Sall BUSSINESS';#SMALL BUSINESS same output

select Ord_id, Profit
from market_fact_full
where profit<0;


select Ord_id,Shipping_Cost
from market_fact_full
where Ord_id like '%\_5%' and Shipping_Cost between 10 and 15;

select '54' > 'a';

select count(sales) as No_Of_Sales from market_fact_full;
#diff in count(sales) and count(*) is count(*) will also count null values.


select count(Ord_id) as Loss_Count from market_fact_full where Profit < 0;

select count(Customer_Name) as Segment_Wise_Customers, Customer_Segment from cust_dimen where state ='Bihar'
group by Customer_Segment;

select Customer_Name from cust_dimen order by  Customer_Name;

select distinct Customer_Name from cust_dimen order by  Customer_Name;

select distinct Customer_Name from cust_dimen order by  Customer_Name DESC;

select Customer_Name, City from cust_dimen order by City, Customer_Segment;

select Customer_Name, City from cust_dimen order by Customer_Segment, City asc;

Select count(*) from cust_dimen;

select  Prod_id, sum(Order_Quantity) from market_fact_full group by Prod_id order by sum(Order_Quantity) desc limit 3;

# having when we used aggregate function, where appiled on table
select  Prod_id, sum(Order_Quantity) from market_fact_full group by Prod_id having sum(Order_Quantity)>40 order by sum(Order_Quantity) desc;

#String and Date Functions
Select Product_Category,Product_Sub_Category, concat(Product_Category,' ' ,Product_Sub_Category) as Product_Name
from prod_dimen;

Select Customer_Name, concat(upper(substring_index(Customer_Name,0,1))) as Customer_Name_ from cust_dimen;

Select count(Ship_id) as Ship_Count, month(Ship_Date) as Ship_Month from shipping_dimen
group by Ship_Month
order by Ship_Count desc;

select count(Ord_id) as Order_Count, month(Order_Date) as Order_Month,
year(Order_Date) as Order_Year
from orders_dimen
where Order_Priority='Critical'
group by Order_Year,Order_Month
order by Order_Count desc;

select Ship_Mode, count(Ship_Mode) as Ship_Mode_Count
from shipping_dimen
where year(Ship_Date)=2011
group by Ship_Mode
order by Ship_Mode desc;


select mod(pow(79,16),17);


#1. Print all names whose name contain car
select Customer_Name from cust_dimen where Customer_Name regexp 'car';
#2. Print customers names starting with A,B,C,D and ending with 'er';
select Customer_Name from cust_dimen where Customer_Name regexp '^[abcd].*er$';

#Nested Queris

#1. Print the order number of the most valuable order by states
select Ord_id, Sales,round(Sales) as Rounded_Sales from market_fact_full
where Sales=(Select max(Sales) from market_fact_full);

#2. Return all the product categories and sub categories of all the product which don't have the details
#base margin 
#it should not be like this
select Prod_id from market_fact_full where Product_Base_Margin=null;
#it should be like following
select Prod_id from market_fact_full where Product_Base_Margin is null;

Select * from prod_dimen 
where Prod_id in (select Prod_id
				from market_fact_full
				where Product_Base_Margin is null);
                
#Print the name of the most frequent customer 
Select Customer_Name, Cust_id
from cust_dimen
where Cust_id=(Select Cust_id from market_fact_full group by Cust_id order by count(Cust_id) desc limit 1);

#4. Print the three most common products;
select Product_Category, Product_Sub_Category
from prod_dimen
where Prod_id in (select Prod_id 
    from market_fact_full 
    group by Prod_id 
    order by count(Prod_id) 
    desc);
    
# CTE Common table expression
Select 	Prod_id, Profit, Product_Base_Margin
from market_fact_full
where Profit<0
order by Profit desc
limit 5;

#Find the 5 products which resulted  in the least losses. Which  product had the highest product margin among these.
with least_losses as (
Select 	Prod_id, Profit, Product_Base_Margin
from market_fact_full
where Profit<0
order by Profit desc
limit 5
) select * 
from least_losses
where Product_Base_Margin=(
	select max(Product_Base_Margin) from least_losses
)


#View: Only Virtual Table, No space in HardDisk. 

create view order_info
as select Ord_id, Sales, Order_Quantity, Profit, Shipping_Cost
from market_fact_full;


Select Ord_id, Profit from order_info where Profit>1000;


#Joins Inner join:Only common values

#1. Print the product categories and sub categories along with the profits made for each other.
select Ord_id, Product_Category, Product_Sub_Category, Profit
from prod_dimen p inner join market_fact_full m
on p.Prod_id = m.Prod_id;

#2. Find the shipment date, shipment mode and profit made for every single order.
select Ord_id, Profit, Ship_Mode, Ship_Mode
from market_fact_full m inner join shipping_dimen s
on m.Ship_id=s.Ship_id;

#3. Three way join
select m.prod_id, m.profit, p.product_category, s.ship_mode
from market_fact_full m inner join prod_dimen p on m.prod_id=p.prod_id
inner join shipping_dimen s on m.ship_id=s.ship_id;

# Which customer ordered the most number of products
select Customer_Name, sum(Order_Quantity) as Total_Orders
from cust_dimen c
inner join  market_fact_full m
on c.cust_id=m.cust_id
group by Customer_Name
order by Total_Orders desc;

#alertnate any
select Customer_Name, sum(Order_Quantity) as Total_Orders
from cust_dimen
inner join market_fact_full
using (cust_id)
group by Customer_Name
order by Total_Orders desc;

# Selling offices supplies was more profitable in Delhi as compared to Patna. True or False?
select p.Prod_id, Profit, Product_Category, City,sum(Profit) as City_Wise_Profit
from prod_dimen p
inner join market_fact_full m
on p.Prod_id=m.Prod_id
inner join cust_dimen c
on m.Cust_id=c.Cust_id
where Product_Category='Office Supplies' and City in ('Delhi','Patna')
group by City;
#We should not use p.Prod_id, Profit in above query.
#SELECT list is not in GROUP BY clause and contains nonaggregated column 'market_star_schema.p.Prod_id' which 
#is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by

select Product_Category, City,sum(Profit) as City_Wise_Profit
from prod_dimen p
inner join market_fact_full m
on p.Prod_id=m.Prod_id
inner join cust_dimen c
on m.Cust_id=c.Cust_id
where Product_Category='Office Supplies' and City in ('Delhi','Patna')
group by City,Product_Category;


# Print the names of the customer with the maximum no of orders
select Customer	_Name,count(Customer_Name) as No_Of_Orders
from cust_dimen c
inner join market_fact_full m
on c.Cust_id=m.Cust_id
group by Customer_Name
order by No_Of_Orders desc
limit 1;



#Outer Join 
#Display the products sold by all the manufacturers using both INNER JOIN and OUTER JOIN
select * from manu;

select distinct manu_id from prod_dimen;

select m.manu_name, p.prod_id
from manu m inner join prod_dimen p on m.manu_id=p.manu_id;

select m.manu_name, p.prod_id
from manu m left join prod_dimen p on m.manu_id=p.manu_id;

#only manufacturer who have poroduct 
select m.manu_name, count(prod_id)
from manu m inner join prod_dimen p on m.manu_id=p.manu_id
group by m.manu_name;

#All manufacturer who have product sale 0 or more
select m.manu_name, count(prod_id)
from manu m left join prod_dimen p on m.manu_id=p.manu_id
group by m.manu_name;




create view order_details 
as select Customer_Name, Customer_Segment, Sales, Order_Quantity, Product_Category, Product_Sub_Category 
from cust_dimen c
inner join market_fact_full m
on c.cust_id=m.Cust_id
inner join prod_dimen p
on m.prod_id=p.prod_id;

select Customer_Name, Customer_Segment, Order_Quantity,Product_Sub_Category 
from order_details
where Order_Quantity>20 ;


#Two tables are union-compatible if:
#1. They have the same number of attributes.
#2. The attribute types are compatible - the corresponding attributes have the same data type.

#union:no duplicate values, union all: with duplicate values
#Intersect not supported: Common Records in two sets(queries)
#Minus not supported: same as in set theory: A-B. all fields which are not part of B(right hand side), only A(left hand side).
(select Prod_id, sum(Profit)
from market_fact_full
group by Prod_id
order by sum(Profit) desc
limit 2)
union 
(select Prod_id, sum(Profit)
from market_fact_full
group by Prod_id
order by sum(Profit)
limit 2);

select reverse(substring('Sachin Tendulkar', -7, 3));#udn
select reverse(substring('Sachin Tendulkar', 7, 3));#et