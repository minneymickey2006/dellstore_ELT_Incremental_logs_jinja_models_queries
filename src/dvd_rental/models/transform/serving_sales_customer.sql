drop table if exists {{ target_table }};

create table {{ target_table }} as (
    with customer_sales as (
        select
            c.customer_id,
            c.first_name,
            c.last_name,
            ci.city_id,
            ci.city,
            co.country_id,
            co.contry,
            sum(p.amount) as sales
        from
            customer c inner join payment as p
                on c.customer_id=p.customer_id
            left join address a
                on c.address_id = a.address_id
            inner join city ci
                on ci.city_id = a.city_id
            inner join country co
            on co.country_id = ci.country_id
        group by
            c.customer_id,
            c.first_name,
            c.last_name,
            ci.city_id,
            ci.city,
            co.country_id,
            co.country
    )
        select
            customer_id,
            first_name,
            last_name,
            city,
            country,
            sales,
            round(avg(sales) over (partition by city_id)::numeric,2) as avg_city_sales,
            round(avg(sales) over (partition by country_id)::numeric,2) as avg_country_sales,
            rank() over (order by sales desc) as rank_customer_sales,
            rank() over (partition by city_id order by sales desc) as rank_city_sales,
            rank() over (partition by country_id order by sales desc) as rank_country_sales
        from
            customer_sales
        order by customer_id

);
