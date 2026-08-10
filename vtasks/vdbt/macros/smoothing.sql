{% macro monthly_grid(relation, date_column, measure, dims=[], fill='zero') %}

    {%- if fill not in ['zero', 'forward'] -%}
        {{ exceptions.raise_compiler_error("monthly_grid: fill must be 'zero' or 'forward', got '" ~ fill ~ "'") }}
    {%- endif -%}

    WITH _source AS (
        SELECT
            date_trunc('month', {{ date_column }}) AS month,
            {% for dim in dims -%}
                {{ dim }},
            {% endfor -%}
            sum({{ measure }}) AS {{ measure }}
        FROM {{ relation }}
        GROUP BY ALL
    ),

    _months AS (
        SELECT unnest(range(
            min(month),
            max(month) + INTERVAL 1 MONTH,
            INTERVAL 1 MONTH
        )) :: date AS month
        FROM _source
    ),

    {% if dims -%}
    _dims AS (
        SELECT DISTINCT {{ dims | join(', ') }} FROM _source
    ),

    _grid AS (
        SELECT * FROM _months CROSS JOIN _dims
    ),
    {%- else -%}
    _grid AS (
        SELECT * FROM _months
    ),
    {%- endif %}

    _joined AS (
        SELECT
            _grid.month,
            {% for dim in dims -%}
                _grid.{{ dim }},
            {% endfor -%}
            _source.{{ measure }} AS _raw_value
        FROM _grid
        LEFT JOIN _source
            ON _grid.month = _source.month
            {% for dim in dims -%}
                AND _grid.{{ dim }} IS NOT DISTINCT FROM _source.{{ dim }}
            {% endfor %}
    ),

    _filled AS (
        SELECT
            month,
            {% for dim in dims -%}
                {{ dim }},
            {% endfor -%}
            {% if fill == 'zero' -%}
                coalesce(_raw_value, 0) AS {{ measure }},
            {%- else -%}
                coalesce(
                    _raw_value,
                    last_value(_raw_value IGNORE NULLS) OVER (
                        {% if dims %}PARTITION BY {{ dims | join(', ') }}{% endif %}
                        ORDER BY month
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ),
                    0
                ) AS {{ measure }},
            {%- endif %}
            _raw_value IS NULL AS is_filled
        FROM _joined
    )

    SELECT * FROM _filled
{% endmacro %}


{% macro savgol_smooth(
    relation,
    measure,
    dims=[],
    window_size=35,
    degree=5,
    post_window=3,
    out_column='trend'
) %}

    {%- if post_window % 2 == 0 -%}
        {{ exceptions.raise_compiler_error("savgol_smooth: post_window must be odd, got " ~ post_window) }}
    {%- endif -%}
    {%- set post_half = (post_window // 2) | int -%}

    WITH _indexed AS (
        SELECT
            *,
            date_diff('month', DATE '1970-01-01', month) AS _m
        FROM {{ relation }}
    ),

    _bounds AS (
        SELECT
            {% for dim in dims -%}
                {{ dim }},
            {% endfor -%}
            min(_m) AS _lo,
            max(_m) AS _hi
        FROM _indexed
        GROUP BY ALL
    ),

    _weights AS (
        SELECT tap_offset, tap_weight
        FROM {{ ref('smoothing_weights') }}
        WHERE kernel = 'savgol'
            AND window_size = {{ window_size }}
            AND degree = {{ degree }}
    ),

    _filtered AS (
        SELECT
            base.month,
            {% for dim in dims -%}
                base.{{ dim }},
            {% endfor -%}
            sum(w.tap_weight * neighbour.{{ measure }}) AS _savgol
        FROM _indexed AS base
        INNER JOIN _bounds AS bounds
            ON TRUE
            {% for dim in dims -%}
                AND base.{{ dim }} IS NOT DISTINCT FROM bounds.{{ dim }}
            {% endfor %}
        CROSS JOIN _weights AS w
        INNER JOIN _indexed AS neighbour
            -- Clamping the index reproduces scipy's mode='nearest' edge padding
            ON neighbour._m = greatest(bounds._lo, least(bounds._hi, base._m + w.tap_offset))
            {% for dim in dims -%}
                AND neighbour.{{ dim }} IS NOT DISTINCT FROM base.{{ dim }}
            {% endfor %}
        GROUP BY ALL
    ),

    _smoothed AS (
        SELECT
            month,
            {% for dim in dims -%}
                {{ dim }},
            {% endfor -%}
            {% if post_window > 1 -%}
            avg(_savgol) OVER (
                {% if dims %}PARTITION BY {{ dims | join(', ') }}{% endif %}
                ORDER BY month
                ROWS BETWEEN {{ post_half }} PRECEDING AND {{ post_half }} FOLLOWING
            ) AS {{ out_column }}
            {%- else -%}
            _savgol AS {{ out_column }}
            {%- endif %}
        FROM _filtered
    )

    SELECT
        source.*,
        round(_smoothed.{{ out_column }}, 2) AS {{ out_column }}
    FROM {{ relation }} AS source
    INNER JOIN _smoothed
        ON source.month = _smoothed.month
        {% for dim in dims -%}
            AND source.{{ dim }} IS NOT DISTINCT FROM _smoothed.{{ dim }}
        {% endfor %}
{% endmacro %}
