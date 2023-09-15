all_shopify_orders as (
    select
        *,
        row_number() over (partition by shopper_id order by order_created_at) as shopper_order_number
    from
        DBT_PROD.CORE.int_shopify_orders_aggregated
),

first_order_outcomes as (
    select
    all_shopify_orders.shopper_id
    ,max(loop_returns.is_exchange) as is_loop_exchange
    ,max(loop_returns.is_refund) as is_loop_refund
    ,max(loop_returns.is_store_credit) as is_loop_store_credit

    from
        all_shopify_orders

    inner join DBT_PROD.CORE.int_loop_returns_aggregated as loop_returns
    on all_shopify_orders.provider_id = loop_returns.shopify_order_id

    where
        all_shopify_orders.shopper_order_number = 1
    group by 
        1
    ),

first_return_processed as (
    select
        shop_id,
        min(return_processed_at) as first_return_processed_at
    from
        DBT_PROD.CORE.stg_loop__returns
    group by 1
)

, shopper_orders as (
select
   all_shopify_orders.shopper_id
  ,all_shopify_orders.shop_id
  ,all_shopify_orders.merchant_currency_symbol

  ,min(all_shopify_orders.order_created_at)
      as shopper_first_order_at
  ,max(all_shopify_orders.order_created_at)
      as shopper_most_recent_order_at
  ,max(iff(all_shopify_orders.refund_value_usd>0, all_shopify_orders.order_created_at, null))
       as shopper_first_refund_at
  ,min(iff(all_shopify_orders.refund_value_usd>0, all_shopify_orders.order_created_at, null))
       as shopper_most_recent_refund_at
  ,max(all_shopify_orders.refund_value_usd>0)
      as has_refund
  ,sum(all_shopify_orders.line_item_count)
      as number_of_items_purchased
  ,sum(all_shopify_orders.number_of_refunds)
      as number_of_refunds
  ,count(all_shopify_orders.order_id)
      as number_of_orders
  ,sum(all_shopify_orders.refund_value_shopper_currency)
      as total_refund_value_shopper_currency
  ,sum(all_shopify_orders.refund_value_usd)
      as total_refund_value_usd
  ,sum(all_shopify_orders.gross_merchandise_value_shopper_currency)
      as total_gross_merchandise_value_shopper_currency
  ,sum(all_shopify_orders.gross_merchandise_value_usd)
      as total_gross_merchandise_value_usd
  ,sum(all_shopify_orders.total_line_items_price_shopper_currency)
      as total_price_of_all_items_shopper_currency
  ,sum(all_shopify_orders.total_line_items_price_usd)
      as total_price_of_all_items_usd
  ,sum(all_shopify_orders.total_price_shopper_currency)
      as total_amount_spent_shopper_currency
  ,sum(all_shopify_orders.total_price_usd)
      as total_amount_spent_usd
  ,sum(all_shopify_orders.total_price_shop_currency)
      as total_amount_spent_merchant_currency
  ,sum(all_shopify_orders.return_gross_merchandise_value_shopper_currency)
      as total_return_gross_merchandise_value_shopper_currency
  ,sum(all_shopify_orders.return_gross_merchandise_value_usd)
      as total_return_gross_merchandise_value_usd
  ,sum(all_shopify_orders.return_total_shopper_currency)
      as total_return_value_shopper_currency
  ,sum(all_shopify_orders.return_total_usd)
      as total_return_value_usd
  ,sum(all_shopify_orders.total_price_shop_currency)
      as total_amount_spent_shop_currency
  ,sum(all_shopify_orders.gross_merchandise_value_shop_currency)
      as total_gross_merchandise_value_shop_currency
  ,sum(all_shopify_orders.total_line_items_price_shop_currency)
      as total_price_of_all_items_shop_currency
  ,sum(all_shopify_orders.return_gross_merchandise_value_shop_currency)
      as total_return_gross_merchandise_value_shop_currency
  ,sum(all_shopify_orders.return_total_shop_currency)
      as total_return_value_shop_currency
  ,sum(all_shopify_orders.refund_value_shop_currency)
      as total_refund_value_shop_currency
  ,min(all_shopify_orders.first_return_created_at)
      as shopper_first_return_created_at

  ,sum(iff(cast(first_return_processed.first_return_processed_at as datetime) < all_shopify_orders.order_created_at
          ,all_shopify_orders.total_price_shop_currency
          ,0)) as total_amount_spent_after_merchant_first_return_merchant_currency


from all_shopify_orders

left join first_return_processed
    on all_shopify_orders.shop_id = first_return_processed.shop_id

group by 1, 2, 3
)

, distinct_shoppers as (
select
   *
  ,
    
md5(cast(coalesce(cast(email as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as shopper_ecosystem_id
  ,row_number() over (partition by shopper_provider_id order by shopper_updated_at desc) as most_recent_shopper_record
  ,email is null or email = '' as has_no_email

from DBT_PROD.CORE.stg_shopify__shoppers

where is_fivetran_deleted = false

qualify most_recent_shopper_record = 1
)

select
   distinct_shoppers.shopify_shopper_id
  ,distinct_shoppers.shopper_ecosystem_id
  ,
    
md5(cast(coalesce(cast(distinct_shoppers.email as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(shopper_orders.shop_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as shopify_shopper_merchant_ecosystem_id
  ,distinct_shoppers.has_shopper_consented_to_marketing
  ,distinct_shoppers.shopper_marketing_consent_updated_at
  ,distinct_shoppers.shopify_shopper_created_at
  ,distinct_shoppers.shopper_currency
  ,distinct_shoppers.email
  ,distinct_shoppers.first_name
  ,distinct_shoppers.last_name
  ,distinct_shoppers.most_recent_order_id
  ,distinct_shoppers.marketing_opt_in_level
  ,distinct_shoppers.multipass_identifier
  ,distinct_shoppers.note_about_shopper
  ,distinct_shoppers.shopper_phone_number
  ,distinct_shoppers.shopper_provider_created_at
  ,distinct_shoppers.shopper_provider_id
  ,distinct_shoppers.shopper_provider_updated_at
  ,distinct_shoppers.shopper_status
  ,distinct_shoppers.is_tax_exempt
  ,distinct_shoppers.total_spent_shopper_currency
  ,distinct_shoppers.shopper_updated_at
  ,distinct_shoppers.is_email_verified
  ,distinct_shoppers.has_no_email

  ,shopper_orders.merchant_currency_symbol
  ,shopper_orders.shopper_first_order_at
  ,shopper_orders.shopper_most_recent_order_at
  ,shopper_orders.number_of_orders
  ,shopper_orders.number_of_items_purchased
  ,shopper_orders.number_of_refunds
  ,shopper_orders.total_refund_value_shopper_currency
  ,shopper_orders.total_refund_value_usd
  ,shopper_orders.total_gross_merchandise_value_shopper_currency
  ,shopper_orders.total_gross_merchandise_value_usd
  ,shopper_orders.total_price_of_all_items_shopper_currency
  ,shopper_orders.total_price_of_all_items_usd
  ,shopper_orders.total_amount_spent_shopper_currency
  ,shopper_orders.total_amount_spent_usd
  ,shopper_orders.total_amount_spent_merchant_currency
  ,shopper_orders.total_return_gross_merchandise_value_shopper_currency
  ,shopper_orders.total_return_gross_merchandise_value_usd
  ,shopper_orders.total_return_value_shopper_currency
  ,shopper_orders.total_return_value_usd
  ,shopper_orders.total_amount_spent_shop_currency
  ,shopper_orders.total_price_of_all_items_shop_currency
  ,shopper_orders.total_gross_merchandise_value_shop_currency
  ,shopper_orders.total_return_value_shop_currency
  ,shopper_orders.total_refund_value_shop_currency
  ,shopper_orders.total_return_gross_merchandise_value_shop_currency
  ,shopper_orders.shopper_first_order_at = shopper_orders.shopper_first_refund_at as was_shoppers_first_order_refunded
  ,shopper_orders.has_refund
  ,shopper_orders.shopper_first_return_created_at
  ,shopper_orders.total_amount_spent_after_merchant_first_return_merchant_currency

  ,first_order_outcomes.is_loop_exchange as is_first_order_a_loop_exchange
  ,first_order_outcomes.is_loop_refund as is_first_order_a_loop_refund
  ,first_order_outcomes.is_loop_store_credit as is_first_order_a_loop_store_credit

  ,shopper_second_order.order_created_at as shopper_second_order_created_at
  
  ,shopper_third_order.order_created_at as shopper_third_order_created_at

from distinct_shoppers

left join shopper_orders
    on distinct_shoppers.shopify_shopper_id = shopper_orders.shopper_id

left join first_order_outcomes
    on distinct_shoppers.shopify_shopper_id = first_order_outcomes.shopper_id

left join all_shopify_orders as shopper_second_order
    on distinct_shoppers.shopify_shopper_id = shopper_second_order.shopper_id
    and shopper_second_order.shopper_order_number = 2

left join all_shopify_orders as shopper_third_order
    on distinct_shoppers.shopify_shopper_id = shopper_third_order.shopper_id
    and shopper_third_order.shopper_order_number = 3
        )
/* {"app": "dbt", "dbt_snowflake_query_tags_version": "2.3.1", "dbt_version": "1.4.7", "project_name": "loop_returns_snowflake", "target_name": "default", "target_database": "DBT_PROD", "target_schema": "CORE", "invocation_id": "818443b2-9fac-4743-865a-151223c6fcc9", "node_name": "shopify_shoppers_current", "node_alias": "shopify_shoppers_current", "node_package_name": "loop_returns_snowflake", "node_original_file_path": "./models/marts/core/shopify/shopify_shoppers_current.sql", "node_database": "DBT_PROD", "node_schema": "CORE", "node_id": "model.loop_returns_snowflake.shopify_shoppers_current", "node_resource_type": "model", "node_meta": {}, "node_tags": ["daily", "intra-day"], "full_refresh": true, "which": "build", "node_refs": ["int_shopify_orders_aggregated", "int_loop_returns_aggregated", "stg_loop__returns", "stg_shopify__shoppers"], "materialized": "table", "dbt_cloud_project_id": "243184", "dbt_cloud_job_id": "388042", "dbt_cloud_run_id": "191299123", "dbt_cloud_run_reason_category": "scheduled", "dbt_cloud_run_reason": "scheduled"} */;