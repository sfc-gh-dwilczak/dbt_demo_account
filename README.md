# From 5 Hours to 1, DBT & Snowflake Optimization

Discovering and learning DBT for the first time was one of the greatest things, if not the greatest thing, I've learned with SQL. Discovering and learning why our DBT jobs took 5 hours to run was one of the most excruciating things I've had to experience joining my first DBT project, and I'm not the only one.

This isn't necessarily due to DBT, but I believe DBT enables large jobs and costs by increasing the velocity with which people can create transformational models. In the time it used to deliver 1 dataset, DBT enabled people to deliver 3 or 4 datasets.

This doesn't mean nothing can be done. And no, getting rid of DBT isn't the solution. In this blog post, I've shared my experience optimizing our DBT jobs from 5 hours down to 1 hour while simultaneously introducing new models to be used by end users.

## Optimize Intentionally

Don't try to optimize the entire repository all at once. DBT gives you its job times so that you can easily see which jobs take the longest to run. Start with the slowest models and their related models for the most bang for your buck. Don't try to go for small optimizations at the cost of other things like disabling tests that take 1 minute instead of investing your efforts into models that take 20 minutes.

## Denormalize Too Early

Too common is it the case that people assume intermediate models should be used to denormalize a fact model as much as possible so that they don't have to denormalize later. This can lead to hidden performance problems waiting to happen down the road, depending on how the model is materialized.

When a denormalized intermediate model is materialized as a table (DBT table, incremental model, etc.), the main problem is the volume of the table. In this case, the majority of the cost is simply the 1 time loading of the data into the table. A rough estimate of the cost is the table volume i.e. the number of columns times the number of rows. By denormalizing (many columns) a fact table (many rows), the cost easily explodes here.

But is the cost of materializing a denormalized fact as a table worth it? No! Joins in Snowflake are not that expensive (usually). This means doing the joins in multiple places is often significantly faster than doing saving the large volume to a table. Materializing intermediate models as tables is best reserved for when cost of saving to a table significantly outweighs the cost of computing the query (a complex query).

"But I'm materializing my denormalized fact as a view and downstream models are costing a lot to compute, what gives?" This is a lot more complicated which I will explain in the next section.

In short, don't use intermediate models just to avoid having to do joins! Writing out your joins very well may be the biggest and simplest cost reduction you can have.

## Understand CTE Caching

Understanding when CTEs get cached in Snowflake and how to take advantage of it is equivalent to understanding when you should materialize your DBT models as tables or views. A thorough explanation is given [here](https://select.dev/posts/should-you-use-ctes-in-snowflake), but I'll summarize the results here.

When viewing the query profiler in Snowsite, you may notice nodes which say "WithClause" and "WithReference". A "WithClause" represents a cached CTE. A "WithReference" represents a query from a cached CTE.

CTE caching is only triggered if a CTE is used more than once. This is done to avoid recalculating the CTE multiple times, but cost of caching the CTE may outweigh the cost of just recalculating the CTE.

If a CTE is not cached, then it is treated as a passthrough i.e. it gets substituted with a subquery.

Now, if you follow DBT standard practices, usually the first few CTEs will be `select *` queries to the referenced models. With this in mind, if these models are used more than once in a model then Snowflake will cache the import CTE. This leads to a caveat with CTE caching: which columns are cached? In Snowflake, every column in the cached CTE is cached, even if only a few columns from the CTE are actually used. This means explicitly listing out the columns you want from a cached CTE might actually give a performance boost if the "WithClause" node in the query profiler is one of the most expensive nodes.

But better yet, why not just use references directly instead of `select *` import CTEs? Well, it ultimately depends. If you're referencing a view with complex calculations in it, maybe CTE caching really is the best thing to use. If you're referencing a table, maybe using references directly is a better idea, but beware that loading data from tables has more overhead than loading data from cached CTEs.

If possible, the best solution may be to rewrite the logic so that your CTE computes multiple things at the same time. Here's some examples:

Multiple counts and filters which forces a large CTE to get cached.

```sql
with
    large_cte as (...),

    count_1 as (
        select
            count(*) as total_1
        from
            large_cte
        where
            col_1 = 1
    ),

    count_2 as (
        select
            count(*) as total_2
        from
            large_cte
        where
            col_2 = 2
    ),

    count_3 as (
        select
            count(*) as total_3
        from
            large_cte
        where
            col_3 = 3
    ),

    counts as (
        select
            *
        from
            count_1, count_2, count_3
    )

select * from counts
```

Use `count_if` instead to avoid caching the large CTE.

```sql
with
    large_cte as (...),

    counts as (
        select
            count_if(col_1 = 1) as total_1,
            count_if(col_2 = 2) as total_2,
            count_if(col_3 = 3) as total_3
        from
            large_cte
    )

select * from counts
```

Aggregations at multiple granularities are done in multiple CTEs and unioned back together, causing a large CTE to get cached.

```sql
with
    large_cte as (...),

    revenue_rollup as (
        select
            state,
            city,
            sum(revenue) as revenue
        from
            large_cte
        group by
            all

        union all

        select
            state,
            null as city,
            sum(revenue) as revenue
        from
            large_cte
        group by
            all
    )

select * from revenue_rollup
```

Aggregations at multiple granularities are done using `group by grouping sets` to avoid caching the large CTE.

```sql
with
    large_cte as (...),

    revenue_rollup as (
        select
            state,
            city,
            sum(revenue) as revenue
        from
            large_cte
        group by grouping sets(
            (state, city),
            (state)
        )
    )

select * from revenue_rollup
```

Aggregations at multiple granularities are joined together, causing a large CTE to get cached.

```sql
with
    large_cte as (...),

    state_revenue as (
        select
            state,
            sum(revenue) as revenue
        from
            large_cte
        group by
            all
    ),

    city_revenue as (
        select
            state,
            city,
            sum(revenue) as revenue
        from
            large_cte
        group by
            all
    ),

    city_percent_revenue as (
        select
            state,
            city,
            city_revenue / state_revenue as percent_revenue
        from
            state_revenue
        inner join
            city_revenue
                using(state)
    )

select * from city_percent_revenue
```

Window functions are used to avoid needing to perform joins with multiple CTEs.

```sql
with
    large_cte as (...),

    city_percent_revenue as (
        select
            state,
            city,
            sum(revenue) / sum(sum(revenue)) over (
                partition by state
            ) as percent_revenue
        from
            large_cte
        group by
            1, 2
    )

select * from city_percent_revenue
```

Snowflake offers lots of tools that allow us to overcome the CTE caching problem in the cases that I've had to deal with, and it may also work for you.

## Aggregate & Denormalize Marts Early

I make this mistake all the time. I denormalize my marts table and do a huge group by on the dimensions and sum of the measures to compute my metrics. There's nothing more to be done here, right? Wrong! Let's look at this example:

```sql
with
    fact as (select * from {{ ref('fact') }}),
    dimension as (select * from {{ ref('dimension') }}),

    denormalized as (
        select
            dimension.attribute,
            sum(fact.value) as total
        from
            fact
        left join
            dimension
                using(dimension_key)
        group by
            1
    )

select * from denormalized
```

Check how fine your joins are. By performing additional group by's earlier, you may be able to significantly reduce how many rows need to be joined onto your dimensions. If you have many joins, you may want to perform aggregations after joining certain tables. Some experimentation should be used as needed.

```sql
with
    fact as (select * from {{ ref('fact') }})

-- Estimate what percentage of rows we would need
-- to join with if we aggregated on 1 table first.
select
    count(distinct dimension_key) / count(*) as distinct_percent
from
    fact
```

Finally, try to keep dimension transformations after the join minimal by again moving the logic before the join happens. In some cases, you may even be able to merge several dimension CTEs into a single dimension CTE as well. Make sure not to create cross joins.

```sql
with
    dimension as (select * from {{ ref('dimension') }}),

    modified_dimension as (
        select
            dimension_key,
            variant_data:user:name as user_name
        from
            dimension
    )
```

## Credit Draining Cross Joins

You open your Snowsite query profiler for a query that seems to be draining massive amounts of credits. Suspisciously one of the most expensive nodes is a join. You click it and discover 2 tables are being joined together, each a million rows, but the output is a trillion rows. Say hello to your first cross join, an absolute performance nightmare.

My first hope would be that there's some mistake. If there was a mistake in the join causing a many-to-many join, then that should definitely be fixed. But if the cross join was intentional, then something more needs to be said.

If the cross join exists only to generate values for a combination of 2 other dimensions, then you may want to consider some alternatives. Create a UDF or UDTF to dynamically generate values for combinations of dimensions instead of trying to create a table that has every possible combination. Alternatively, limit the cross join to combinations of dimensions which are actually being used. This might be a simple `select distinct customer, product from fact` to get all of the actual combinations. Or maybe there's some restrictions on which products a customer could actually buy.

## To Union or Union All?

... that is the question. It's a common mistake to use `union` instead of `union all`. The difference is subtle, `union` removes duplicates while `union all` keeps duplicates. If duplicates don't make any difference e.g. your data doesn't have any duplicates between the two things being unioned, then there's no point spending additional compute checking for duplicates. Especially for large unions, this can make a hefty difference in performance, especially when unioning large facts together. Yikes!
