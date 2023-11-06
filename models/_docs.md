{% docs dbt_snapshot__dbt_unique_key %}
Surrogate key including hash(*) for DBT snapshot unique key.
{% enddocs %}

{% docs dbt_snapshot__dbt_scd_id %}
Surrogate key for DBT snapshot internal use.
{% enddocs %}

{% docs dbt_snapshot__dbt_updated_at %}
Snapshot record updated timestamp for DBT snapshot internal use.
{% enddocs %}

{% docs dbt_snapshot__dbt_valid_from %}
Snapshot record insertion/updated-to timestamp.
{% enddocs %}

{% docs dbt_snapshot__dbt_valid_to %}
Snapshot record deletion/updated-from timestamp.
{% enddocs %}

{% docs dbt_snapshot__dwh_effective_from %}
The timestamp a record was ingested to the database.
May not be perfectly accurate for some tables.
{% enddocs %}
