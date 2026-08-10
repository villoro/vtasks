from pathlib import Path

import numpy as np
import pandas as pd

# Generate the Savitzky-Golay tap weights used by the `savgol_smooth` dbt macro.
#
# A Savitzky-Golay filter fits a polynomial of `degree` over a sliding window of
# `window_size` points and keeps the value of that fit at the centre of the window.
# For a fixed (window_size, degree) the resulting weights are constants: they do not
# depend on the data at all. That is what lets the filter live in SQL as a plain
# weighted sum against a seed table.
#
# Re-run after editing FILTERS, then commit the regenerated CSV:
#
#     python scripts/gen_smoothing_weights.py


# ---------------------------------------------------------------------------
# NOTES FOR THE SMOOTHING BLOG POST
# ---------------------------------------------------------------------------
# These are findings from building this, kept here for whoever revisits
# `villoro.com/src/content/blog/0057-smoothing.mdx`. The post currently claims
# Gaussian and Lowess smoothing are not doable in SQL. That claim is too strong and
# is the main thing worth revising. Numbers below are measured, not estimated: the
# reference series were the 158 monthly incomes/expenses points from the 2025_02
# expensor report, whose shipped trend lines came from
# `savgol_filter(35, 5, mode="nearest")` followed by a 3-point convolution.
#
# 1. Savitzky-Golay is a *linear* filter. Least squares is linear in the data, so the
#    fitted value at the centre of the window is a fixed linear combination of the
#    inputs. The taps depend only on (window_size, degree) and not at all on the
#    data, so they can be precomputed and the filter becomes a weighted sum. That is
#    the whole trick that moves it into SQL. Reproducing the report's curve this way
#    gave 0.00 deviation across every interior point of all three series; only the
#    very first and last point differ, and only because tsmoothie pads the trailing
#    3-point convolution differently.
#
# 2. Gaussian smoothing is a weighted moving average, so the same self-join works
#    with `exp(-0.5 * ((a.m - b.m) / sigma) ** 2)` as the weight. No seed needed,
#    since the weight is computed inline.
#
# 3. Lowess is *also* expressible. A local linear fit has a closed form, so weighted
#    least squares reduces to sums the database already has:
#
#        slope = (Sw*Sxy - Sx*Sy) / (Sw*Sxx - Sx*Sx)
#
#    with Sw = sum(w), Sx = sum(w*x), and so on. Fitting locally at each point needs
#    no iteration. Degree 2 works the same way via Cramer's rule on a 3x3 system.
#    What is genuinely hard in SQL is Lowess's *robustness* iterations, which
#    re-weight residuals repeatedly - that part is a fair thing to call impractical.
#
# 4. The cost argument in the post overstates the problem. A self-join bounded to a
#    window is O(n * w), not O(n^2). At 158 monthly points with a 35-month window it
#    is ~11k intermediate rows and runs in single-digit milliseconds. Daily data is
#    fine too. Only an unbounded window is quadratic.
#
# 5. Edges come out for free. Dividing by `sum(w)` renormalises the truncated kernel
#    at the series ends, and clamping the join index to the first/last row is exactly
#    what scipy calls `mode="nearest"`.
#
# 6. Choosing between them, measured against the savgol reference (MAE in euros over
#    158 months, on series averaging ~2500/month):
#
#        local linear, sigma=4     incomes 103.5   expenses  80.8
#        local linear, sigma=6     incomes 123.3   expenses  84.4
#        local quadratic, sigma=6  incomes  83.4   expenses  70.3
#
#    Plain Gaussian and local linear both lag at turning points, because a straight
#    line fitted through a window cannot bend to a peak. Higher polynomial degree is
#    what buys responsiveness, which is the real reason savgol tracks changes faster.
#
# 7. A caveat the post does not mention anywhere: savgol taps include negative lobes,
#    so an isolated spike in a sparse series rings *negative* on both sides. Smoothed
#    monthly spend on a rarely-used category went to -113 EUR. Lowering the degree
#    does not fix it (tested at (21,3) and (15,2)); it is inherent to the kernel.
#    Gaussian weights are all positive, so a weighted average of non-negative values
#    can never go negative. That is a concrete reason to pick one over the other, and
#    would make a good addition to the comparison section.
# ---------------------------------------------------------------------------


# (window_size, degree) pairs to generate. window_size must be odd.
# window_size ~ 20-25% of the series length is a good starting point, and
# window_size should be at least 4x degree so the fit tracks the trend
# instead of interpolating noise.
FILTERS = [
    (35, 5),  # monthly expensor series (~158 months); matches the pre-2023 reports
]

PATH_SEED = (
    Path(__file__).parent.parent / "vtasks" / "vdbt" / "seeds" / "smoothing_weights.csv"
)


def savgol_weights(window_size, degree):
    """Return the centre-point Savitzky-Golay weights for one (window_size, degree)."""

    if window_size % 2 == 0:
        raise ValueError(f"{window_size=} must be odd")
    if window_size < 4 * degree:
        raise ValueError(f"{window_size=} should be at least 4x {degree=}")

    half = window_size // 2
    offsets = np.arange(-half, half + 1, dtype=float)

    # Design matrix of [1, u, u^2, ... u^degree] evaluated at each offset.
    design = np.vander(offsets, degree + 1, increasing=True)

    # Least-squares solution row for the constant term == the fit at u=0.
    weights = np.linalg.pinv(design)[0]

    # A polynomial fit reproduces a constant signal exactly, so the taps sum to 1.
    assert np.isclose(weights.sum(), 1.0), f"{weights.sum()=}"

    return offsets.astype(int), weights


def main():
    rows = []
    for window_size, degree in FILTERS:
        offsets, weights = savgol_weights(window_size, degree)
        for offset, weight in zip(offsets, weights):
            rows.append(
                {
                    "kernel": "savgol",
                    "window_size": window_size,
                    "degree": degree,
                    "tap_offset": offset,
                    "tap_weight": round(float(weight), 12),
                }
            )

    df = pd.DataFrame(rows)
    df.to_csv(PATH_SEED, index=False)
    print(f"Wrote {len(df)} weights for {len(FILTERS)} filter(s) to {PATH_SEED}")


if __name__ == "__main__":
    main()
