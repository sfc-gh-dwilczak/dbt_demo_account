{% docs snowflake_usage__metering_history%}
The METERING_HISTORY view in the ACCOUNT_USAGE schema can be used to return 
the hourly credit usage for an account within the last 365 days (1 year).
{% enddocs %}

{% docs snowflake_usage__warehouse_metering_history %}
This Account Usage view can be used to return the hourly credit usage
for a single warehouse (or all the warehouses in your account) within
the last 365 days (1 year).
{% enddocs %}

{% docs snowflake_usage__service_type %}
Type of service that is consuming credits, which can be one of the following:

AUTO_CLUSTERING — Refer to Automatic Clustering.

MATERIALIZED_VIEW — Refer to Working with Materialized Views.

PIPE — Refer to Snowpipe.

QUERY_ACCELERATION — Refer to Using the Query Acceleration Service.

REPLICATION — Refer to Introduction to Replication and Failover.

SEARCH_OPTIMIZATION — Refer to Using the Search Optimization Service.

SERVERLESS_TASK — Refer to Introduction to Tasks.

SNOWPIPE_STREAMING — Refer to Snowpipe Streaming.

WAREHOUSE_METERING — Refer to Overview of Warehouses.

WAREHOUSE_METERING_READER — Refer to Managing reader accounts.
{% enddocs %}

{% docs snowflake_usage__start_time %}
The date and beginning of the hour (in the local time zone) in which the usage took place.
{% enddocs %}

{% docs snowflake_usage__end_time %}
The date and end of the hour (in the local time zone) in which the usage took place.
{% enddocs %}

{% docs snowflake_usage__entity_id %}
Internal/system-generated identifier for the service type.
{% enddocs %}

{% docs snowflake_usage__service_name %}
Name of the service type. When the service type is SNOWPIPE_STREAMING, there are two cost entries. One entry is for the name of the Snowflake Table object and the other one is for the colon separated Snowpipe Streaming, CLIENT_NAME and SNOWFLAKE_PROVIDED_ID.
{% enddocs %}

{% docs snowflake_usage__credits_used_compute %}
Number of credits used by warehouses and serverless compute resources in the hour.
{% enddocs %}

{% docs snowflake_usage__credits_used_cloud_services %}
Number of credits used for cloud services in the hour.
{% enddocs %}

{% docs snowflake_usage__credits_used %}
Total number of credits used for the account in the hour. This is a sum of CREDITS_USED_COMPUTE and CREDITS_USED_CLOUD_SERVICES. This value does not take into account the adjustment for cloud services, and may therefore be greater than your actual credit consumption.
{% enddocs %}

{% docs snowflake_usage__bytes %}
When the service type is auto_clustering, indicates the number of bytes reclustered during the START_TIME and END_TIME window. When the service type is pipe, indicates the number of bytes inserted during the START_TIME and END_TIME window. When the service type is SNOWPIPE_STREAMING, indicates the number of bytes migrated during the START_TIME and END_TIME window.
{% enddocs %}

{% docs snowflake_usage__rows_clustered %}
When the service type is auto_clustering, indicates number of rows reclustered during the START_TIME and END_TIME window. When the service type is SNOWPIPE_STREAMING, indicates the number of rows migrated during the START_TIME and END_TIME window.
{% enddocs %}

{% docs snowflake_usage__files %}
When the service type is pipe, indicates number of files loaded during the START_TIME and END_TIME window. When the service type is SNOWPIPE_STREAMING, this is null.
{% enddocs %}

{% docs snowflake_usage__budget_id %}
Reserved for future use.
{% enddocs %}

{% docs snowflake_usage__warehouse_id %}
Internal/system-generated identifier for the warehouse.
{% enddocs %}

{% docs snowflake_usage__warehouse_name %}
Name of the warehouse.
{% enddocs %}

{% docs snowflake_usage__total_elapsed_seconds %}
End time - start time for in which the usage took place.
{% enddocs %}

{% docs snowflake_usage__longest_elapsed_seconds %}
Longest running in seconds.
{% enddocs %}
