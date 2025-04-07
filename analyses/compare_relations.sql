{% set old_etl_relations = ref('customer_orders') %}
{% set dbt_relations = ref('fct_customer_orders') %}

{{ audit_helper.compare_relations(
    a_relation = old_etl_relations
    , b_relation = dbt_relations
    , primary_key = 'order_id'
) }}