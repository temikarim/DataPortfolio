-- Removing Duplicates

with cte_duplicate_check as 
(select *,
row_number() over(
					partition by `year`, country, solarenergy, windenergy, hydroenergy, otherrenewableenergy, totalrenewableenergy)
                    as duplicates
from production
)
select * from cte_duplicate_check where duplicates >=1;

-- No duplicates present in the data.



-- Standardising Data: Checking all energy columns add up to total energy, checking what countries and years are present and if there are any absnormalities
select * from production 
where solarenergy + windenergy + hydroenergy + otherrenewableenergy != totalrenewableenergy
;

select distinct country from production;
select distinct year from production;

-- Country, years, and total amounts all okay

-- Editing column names for readability

select round(solarenergy,2) from production;
select round(windenergy,2) from production;
select round(hydroenergy,2) from production;
select round(otherrenewableenergy,2) from production;
select round(totalrenewableenergy,2) from production;

alter table production rename column solar_energy_GWh to Solar_Energy_GWh; 
alter table production rename column windenergy to Wind_Energy_GWh; 
alter table production rename column hydroenergy to Hydro_Energy_GWh;
alter table production rename column otherrenewableenergy to Other_Renewable_Energy_GWh; 
alter table production rename column totalrenewableenergy to Total_Renewable_Energy_GWh; 



-- Creating a staging table. 

create table production_staging
like production;

insert production_staging
select `year`, country, round(solar_energy_gwh,2), round(wind_energy_gwh,2), round(hydro_energy_gwh,2),
round(other_renewable_energy_gwh,2), round(total_renewable_energy_gwh,2)
 from production;
 
 select * from production_staging;
 
 
 -- Analysis
# 1. the global percentage of each energy type each year, and then for each country


select distinct `year`, sum(solar_energy_gwh) over(partition by `year`) as solar_sum,
	sum(total_renewable_energy_gwh) over(partition by `year`) as total_sum
from production_staging
;

select distinct country, sum(solar_energy_gwh) over(partition by country) as solar_sum,
	sum(total_renewable_energy_gwh) over(partition by country) as total_sum
from production_staging
;
-- ^ Checking if code works.


create table production_percentages (
`Year` int not null,
Solar  double,
Wind double,
Hydro double,
Other_Renewables double,
Total_Renewables double
);


with global_percentages as 
(
select distinct `year`,
	sum(solar_energy_gwh) over(partition by `year`) as solar_sum,
    sum(wind_energy_gwh) over(partition by `year`) as wind_sum,
    sum(hydro_energy_gwh) over(partition by `year`) as hydro_sum,
    sum(other_renewable_energy_gwh) over(partition by `year`) as other_sum,
	sum(total_renewable_energy_gwh) over(partition by `year`) as total_sum
from production_staging

)
select `year`,
	round(solar_sum/total_sum * 100,2) as solar_per,
    round(wind_sum/total_sum * 100,2) as wind_per,
    round(hydro_sum/total_sum * 100,2) as hydro_per,
    round(other_sum/total_sum * 100,2) as other_per,
    round(total_sum/total_sum * 100,2) as total_per
from global_percentages
;

-- CTE works, aim is to add this into a new table.

insert into production_percentages (
`year`,
solar,
wind,
hydro,
other_renewables,
total_renewables
)
with global_percentages as 
(
select distinct `year`,
	sum(solar_energy_gwh) over(partition by `year`) as solar_sum,
    sum(wind_energy_gwh) over(partition by `year`) as wind_sum,
    sum(hydro_energy_gwh) over(partition by `year`) as hydro_sum,
    sum(other_renewable_energy_gwh) over(partition by `year`) as other_sum,
	sum(total_renewable_energy_gwh) over(partition by `year`) as total_sum
from production_staging

)
select `year`,
	round(solar_sum/total_sum * 100,2) as solar_per,
    round(wind_sum/total_sum * 100,2) as wind_per,
    round(hydro_sum/total_sum * 100,2) as hydro_per,
    round(other_sum/total_sum * 100,2) as other_per,
    round(total_sum/total_sum * 100,2) as total_per from global_percentages
;

select * from production_percentages;

select * from production_staging;


create table production_by_country (
country varchar(50),
Solar  double,
Wind double,
Hydro double,
Other_Renewables double,
Total_Renewables double
);

insert into production_by_country (
country,
solar,
wind,
hydro,
other_renewables,
total_renewables
)
with global_production as 
(
select distinct country,
	sum(solar_energy_gwh) over(partition by country) as solar_sum,
    sum(wind_energy_gwh) over(partition by country) as wind_sum,
    sum(hydro_energy_gwh) over(partition by country) as hydro_sum,
    sum(other_renewable_energy_gwh) over(partition by country) as other_sum,
	sum(total_renewable_energy_gwh) over(partition by country) as total_sum
from production_staging
)
select country,
	round(solar_sum/total_sum * 100,2) as solar_per,
    round(wind_sum/total_sum * 100,2) as wind_per,
    round(hydro_sum/total_sum * 100,2) as hydro_per,
    round(other_sum/total_sum * 100,2) as other_per,
    round(total_sum/total_sum * 100,2) as total_per from global_production
;

select * from production_by_country;
select * from production_percentages;

alter table production_percentages rename production_by_year;

select * from production_by_country;
select * from production_by_year;
select * from production_staging;


-- Further tests

select country, max(solar)
from production_by_country
group by country;

select max(solar)
from production_by_country;


select distinct year, round(sum(total_renewable_energy_gwh),2) from production_staging
group by year
order by sum(total_renewable_energy_gwh) desc; 