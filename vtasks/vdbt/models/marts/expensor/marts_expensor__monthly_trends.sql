{#
    Window 35 / degree 5 reproduces the trend lines of the pre-2023 HTML reports.
    The weights for this pair must exist in the `smoothing_weights` seed; add new
    ones with scripts/gen_smoothing_weights.py.
#}
{% set window_size = 35 %}
{% set degree = 5 %}

WITH smoothed AS (
    {{ savgol_smooth(
        relation=ref('core_expensor__monthly'),
        measure='value_eur',
        dims=['category'],
        window_size=window_size,
        degree=degree,
        post_window=3,
        out_column='trend_eur'
    ) }}
),

final AS (
    SELECT
        -------- dims
        month,
        category,

        -------- measures
        value_eur,
        trend_eur,

        -------- metadata
        is_filled,
        -- The filter is centred, so the newest months only see half a window and
        -- their trend keeps moving as data arrives. This is relative to the end of
        -- the series, not to today: if the pipeline lags, the provisional region
        -- has to move with the data. Flagged rather than dropped so the dashboard
        -- keeps the choice.
        month > (SELECT max(month) FROM smoothed) - INTERVAL {{ window_size // 2 }} MONTH
            AS is_provisional_trend
    FROM smoothed
    ORDER BY ALL
)

SELECT * FROM final
