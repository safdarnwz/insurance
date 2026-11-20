{# Generic test: column ki har value > 0 honi chahiye. Fail rows return karta hai. #}
{% test positive_value(model, column_name) %}
select * from {{ model }} where {{ column_name }} <= 0
{% endtest %}
