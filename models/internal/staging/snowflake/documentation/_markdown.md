{% docs snowflake__service_type %}
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

{% docs snowflake__start_time %}
The date and beginning of the hour (in the local time zone) in which the usage took place.
{% enddocs %}

{% docs snowflake__end_time %}
The date and end of the hour (in the local time zone) in which the usage took place.
{% enddocs %}

{% docs snowflake__entity_id %}
Internal/system-generated identifier for the service type.
{% enddocs %}

{% docs snowflake__name %}
Name of the service type. When the service type is SNOWPIPE_STREAMING, there are two cost entries. One entry is for the name of the Snowflake Table object and the other one is for the colon separated Snowpipe Streaming, CLIENT_NAME and SNOWFLAKE_PROVIDED_ID.
{% enddocs %}

{% docs snowflake__credits_used_compute %}
Number of credits used by warehouses and serverless compute resources in the hour.
{% enddocs %}

{% docs snowflake__credits_used_cloud_services %}
Number of credits used for cloud services in the hour.
{% enddocs %}

{% docs snowflake__credits_used %}
Total number of credits used for the account in the hour. This is a sum of CREDITS_USED_COMPUTE and CREDITS_USED_CLOUD_SERVICES. This value does not take into account the adjustment for cloud services, and may therefore be greater than your actual credit consumption.
{% enddocs %}

{% docs snowflake__bytes %}
When the service type is auto_clustering, indicates the number of bytes reclustered during the START_TIME and END_TIME window. When the service type is pipe, indicates the number of bytes inserted during the START_TIME and END_TIME window. When the service type is SNOWPIPE_STREAMING, indicates the number of bytes migrated during the START_TIME and END_TIME window.
{% enddocs %}

{% docs snowflake__rows_clustered %}
When the service type is auto_clustering, indicates number of rows reclustered during the START_TIME and END_TIME window. When the service type is SNOWPIPE_STREAMING, indicates the number of rows migrated during the START_TIME and END_TIME window.
{% enddocs %}

{% docs snowflake__files %}
When the service type is pipe, indicates number of files loaded during the START_TIME and END_TIME window. When the service type is SNOWPIPE_STREAMING, this is null.
{% enddocs %}

{% docs snowflake__budget_id %}
Reserved for future use.
{% enddocs %}


