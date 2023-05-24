drop table if exists {{ target_table }};

create table {{ target_table }} as (
    select
        film_id,
        title,
        release_year,
        category_name,
        sales,
        avg(sales) over (partition by release_year) as avg_sales_by_release_year,
        avg(sales) over (partition by category_id) as avg_sales_by_category,
        rank() over (order by sales desc) as rank_sales, 
        rank() over (partition by release_year order by sales desc) as rank_sales_by_release_year, 
        rank() over (partition by category_id order by sales desc) as rank_sales_by_category
    from
        staging_films
    order by film_id
);