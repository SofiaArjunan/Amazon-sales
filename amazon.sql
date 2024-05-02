use amazon;
select *
from amazon_data;

create table amazon_data1
like amazon_data;

insert amazon_data1
select *
from amazon_data;

select *
from amazon_data1;

#assign row number to each row with respect to order id to find duplicates

select *,
row_number ( ) over (partition by `order id`) as row_num
from amazon_data1;

with duplicate_cte as
(select *,
row_number ( ) over (partition by `order id`) as row_num
from amazon_data1)
select *
from duplicate_cte
where row_num>1;


select count(`order id`)
from amazon_data1
group by `order ID`;

# duplicate cte and count function shows that there is no duplicates found in the data

# the following are the queries to convert text format of order date and ship date to date format

select `order date`, str_to_date(`order date`, '%m/%d/%Y')
from amazon_data1;

update amazon_data1
set `order date` = str_to_date(`order date`, '%m/%d/%Y');

select `ship date`, str_to_date(`ship date`, '%m/%d/%Y')
from amazon_data1;

update amazon_data1
set `ship date` = str_to_date(`ship date`, '%m/%d/%Y');

select *
from amazon_data1;

# Total sales per month, year, yearly month is obtained along with their region, country, item_type and sales channel.
#By adding where clause we can filter region wise, country wise etc.,

#yearly month
select region, country, `item type`, `sales channel`, year(`order date`) as 'year',month(`order date`) as 'month',sum(`units sold`) as 'sum of sales'
     from amazon_data1
     where region = 'europe'
     group by region, country,`item type`,`sales channel`, year(`order date`),month(`order date`)
     order by year(`order date`),month(`order date`);

#yearly
select region, country, `item type`, `sales channel`, year(`order date`) as 'year',sum(`units sold`) as 'sum of sales'
     from amazon_data1
     group by region, country,`item type`,`sales channel`, year(`order date`)
     order by year(`order date`);

#monthly
select region, country, `item type`, `sales channel`, month(`order date`) as 'month',sum(`units sold`) as 'sum of sales'
     from amazon_data1
     group by region, country,`item type`,`sales channel`,month(`order date`)
     order by month(`order date`);
   
   #the following queries gives countrywise and regionwise the most bought item with the number of items sold
    
    select country, max(`item type`), max(`units sold`)
    from amazon_data1
    group by country;
    
    select region, max(`item type`), max(`units sold`)
    from amazon_data1
    group by region;
    
    #The following query gives the country and their most preferred sales channel
    
    select country, max(`sales channel`)
    from amazon_data1
    group by country;
    
    #Total number of sales per year and month
    
    SELECT YEAR(`Order date`) AS Year, 
MONTH(`Order date`) AS Month,SUM(`units Sold`) 
AS Total_Sales FROM amazon_data1
GROUP BY YEAR(`Order date`), MONTH(`Order date`) 
order by YEAR(`Order date`) asc; 
    
    #To calculate total sales
    
    select region, country, `item type`, sum(`units sold`*`unit price`) as `total sales`
    from amazon_data1
    group by region, country, `item type`;
    
    alter table amazon_data1
    drop column`total sales`;
    
    select *
from amazon_data1;
 
 #The following query is used to calculate the number of days taken for shipping
 
    select country, `item type`, `order id`, datediff(`Ship Date`,`Order Date`) as days_taken_for_shipping
    from amazon_data1
    order by days_taken_for_shipping desc;

#The following helps to find the average purchase values

    select `total revenue` div `units sold` as apv
    from amazon_data1;
    
    # The following shows that there loss in the data given 
    
    select  `item type`, `order id`, `total revenue`,`total cost`
    from amazon_data1
    where `total revenue`> `total cost`;
    
    select `item type`, month(`order date`), `total revenue`, 
    (`total revenue`) - lag(`total revenue`) over (order by month(`order date`)) as revenue_growth
    from amazon_data1
    order by `item type`;
    
   #Total revenue by country and year
   
    select country, `units sold`, year(`order date`), `total revenue`
    from amazon_data1
    order by year(`order date`), country;
    
  # Revenue growth by year, month
  
    SELECT
EXTRACT(YEAR FROM `order date`) AS `year`,
EXTRACT(MONTH FROM `order date`) AS `month`,
SUM(`total revenue`) AS current_month_revenue,
LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(YEAR FROM `order date`), EXTRACT(MONTH FROM `order date`)) AS previous_month_revenue,
(SUM(`total revenue`) - LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(YEAR FROM `order date`), EXTRACT(MONTH FROM `order date`))) / LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(YEAR FROM `order date`), EXTRACT(MONTH FROM `order date`)) AS revenue_growth
FROM
amazon_data1
GROUP BY `year`, `month`
ORDER BY `year`,`month`;

# Revenue growth by year, month by item type

SELECT `item type`,
EXTRACT(YEAR FROM `order date`) AS `year`,
EXTRACT(MONTH FROM `order date`) AS `month`,
SUM(`total revenue`) AS current_month_revenue,
LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(YEAR FROM `order date`), EXTRACT(MONTH FROM `order date`)) AS previous_month_revenue,
(SUM(`total revenue`) - LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(YEAR FROM `order date`), EXTRACT(MONTH FROM `order date`))) / LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(YEAR FROM `order date`), EXTRACT(MONTH FROM `order date`)) AS revenue_growth
FROM
amazon_data1
GROUP BY `item type`,`year`, `month`
ORDER BY `item type`,`year`,`month`;

# Revenue growth by year by item type

SELECT `item type`,
EXTRACT(YEAR FROM `order date`) AS `year`,
SUM(`total revenue`) AS current_year_revenue,
LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(YEAR FROM `order date`)) AS previous_year_revenue,
(SUM(`total revenue`) - LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(YEAR FROM `order date`))) / LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(YEAR FROM `order date`)) AS revenue_growth
FROM
amazon_data1
GROUP BY `item type`,`year`
ORDER BY `item type`,`year`;

# Revenue growth by month by item type

SELECT `item type`, 
EXTRACT(month FROM `order date`) AS `month`,
SUM(`total revenue`) AS current_month_revenue,
LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(month FROM `order date`)) AS previous_month_revenue,
(SUM(`total revenue`) - LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(month FROM `order date`))) / LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(month FROM `order date`)) AS revenue_growth
FROM
amazon_data1
GROUP BY `item type`,`month`
ORDER BY `item type`,`month`;

# Revenue growth by year by region 

SELECT region, 
EXTRACT(year FROM `order date`) AS `year`,
SUM(`total revenue`) AS current_year_revenue,
LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(year FROM `order date`)) AS previous_year_revenue,
(SUM(`total revenue`) - LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(year FROM `order date`))) / LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(year FROM `order date`)) AS revenue_growth
FROM
amazon_data1
GROUP BY region,`year`
ORDER BY region,`year`;

# Revenue growth by month by region 

SELECT region, 
EXTRACT(month FROM `order date`) AS `month`,
SUM(`total revenue`) AS current_month_revenue,
LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(month FROM `order date`)) AS previous_month_revenue,
(SUM(`total revenue`) - LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(month FROM `order date`))) / LAG(SUM(`total revenue`)) OVER (ORDER BY EXTRACT(month FROM `order date`)) AS revenue_growth
FROM
amazon_data1
GROUP BY region,`month`
ORDER BY region,`month`;

select distinct region
from amazon_data1;