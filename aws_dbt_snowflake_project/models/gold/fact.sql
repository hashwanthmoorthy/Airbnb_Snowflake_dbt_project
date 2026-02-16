{% set _obt = ref('obt') %}
{% set _dim_listings = ref('dim_listings') %}
{% set _dim_hosts = ref('dim_hosts') %}

{% set configs = [
    {
        "ref" : _obt,
        "columns" : "GOLD_obt.BOOKING_ID, GOLD_obt.LISTING_ID, GOLD_obt.HOST_ID, GOLD_obt.TOTAL_AMOUNT, GOLD_obt.SERVICE_FEE, GOLD_obt.CLEANING_FEE, GOLD_obt.ACCOMMODATES, GOLD_obt.BEDROOMS, GOLD_obt.BATHROOMS, GOLD_obt.PRICE_PER_NIGHT, GOLD_obt.RESPONSE_RATE",
        "alias" : "GOLD_obt"
    },
    { 
        "ref" : _dim_listings,
        "columns" : "",
        "alias" : "DIM_listings",
        "join_condition" : "GOLD_obt.listing_id = DIM_listings.listing_id"
    },
    {
        "ref" : _dim_hosts,
        "columns" : "",
        "alias" : "DIM_hosts",
        "join_condition" : "GOLD_obt.host_id = DIM_hosts.host_id"
    }
] %}

SELECT 
    {{ configs[0]['columns'] }}
FROM
    {% for config in configs %}
        {% if loop.first %}
            {{ config['ref'] }} AS {{ config['alias'] }}
        {% else %}
            LEFT JOIN {{ config['ref'] }} AS {{ config['alias'] }}
            ON {{ config['join_condition'] }}
        {% endif %}
    {% endfor %}