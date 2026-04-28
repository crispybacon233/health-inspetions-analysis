import marimo

__generated_with = "0.23.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import duckdb

    import matplotlib.pyplot as plt
    import seaborn as sns
    import plotly.express as px

    from dotenv import load_dotenv
    load_dotenv()
    return duckdb, pl, plt, sns


@app.cell
def _(duckdb):
    with duckdb.connect('md:food_establishments') as _conn:
        establishments = _conn.sql("""
        SELECT * FROM int.int_establishments_enriched
        """).pl()

        inspections = _conn.sql("""
        SELECT * FROM stg.stg_inspections
        """).pl()

        df = establishments.join(inspections, on='facility_id', how='inner')
    return (df,)


@app.cell
def _():
    category_filter_str = '(?i)school|community|care|clinic|office|dealer'
    return (category_filter_str,)


@app.cell
def _(category_filter_str, df, pl):
    category_agg = (
        df
        .group_by('category')
        .agg(
            count=pl.len(),
            avg_score=pl.col('score').mean(),
            std_score=pl.col('score').std(),
            median_score=pl.col('score').median(),
        )
        .filter(pl.col('count') >= 15)
        .filter(~pl.col('category').str.contains(category_filter_str)) # filter out school, community, care, etc.
        .sort(by='avg_score')
    )
    return (category_agg,)


@app.cell
def _(category_agg, plt, sns):
    sns.regplot(data=category_agg, x='std_score', y='avg_score')
    plt.xlabel('Standard Dev')
    plt.ylabel('Avg Inspection Score')
    plt.title('Inspection Score: Mean vs Stdev Correlation')
    return


@app.cell
def _(category_agg):
    category_agg
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
