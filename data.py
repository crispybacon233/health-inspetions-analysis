import marimo

__generated_with = "0.23.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import duckdb

    import geopandas as gpd
    import shapely

    import matplotlib.pyplot as plt
    import seaborn as sns
    import plotly.express as px

    from dotenv import load_dotenv
    load_dotenv()
    return duckdb, gpd, mo, pl, shapely


@app.cell
def _(duckdb):
    # Convert Lat Long to Geometry in DuckDB
    conn = duckdb.connect('md:food_establishments')
    conn.execute("INSTALL spatial;")
    conn.execute("LOAD spatial;")
    return (conn,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # MotherDuck Geometry Test
    """)
    return


@app.cell
def _(conn):
    establishments = conn.sql("""
    SELECT * FROM int.int_establishments_enriched
    """).pl()

    inspections = conn.sql("""
    SELECT * FROM stg.stg_inspections
    """).pl()

    df = establishments.join(inspections, on='facility_id', how='inner')
    return


@app.cell
def _(conn):
    # convert lat long to proper geometry then upload to motherduck
    conn.sql("""
    CREATE TABLE IF NOT EXISTS geom_test AS
    SELECT
        *,
        ST_point(longitude, latitude) AS geometry
    FROM df
    WHERE TRUE
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
    LIMIT 5
    """)
    return


@app.cell
def _(conn):
    conn.sql("""
    SELECT * FROM geom_test;
    """).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Reading Shape Files
    """)
    return


@app.cell
def _(conn):
    conn.sql("""
    SELECT *
    FROM 'data/roads/tl_2025_48453_roads.shp'
    """)
    return


@app.cell
def _(conn, pl):
    road_names = conn.sql("""
    SELECT FULLNAME
    FROM 'data/roads/tl_2025_48453_roads.shp'
    """).pl()

    (
        road_names
        .with_columns(
            pl.col('FULLNAME')
                .str.split(' ')
                .list.get(-1)
        )
        .group_by('FULLNAME')
        .agg(pl.len())
        .sort(by='len', descending=True)
    )
    return (road_names,)


@app.cell
def _(pl, road_names):
    # Highways
    (
        road_names
        .filter(pl.col('FULLNAME').str.ends_with('Hwy'))
    )
    return


@app.cell
def _():
    return


@app.cell
def _(pl, road_names):
    (
        road_names
        .filter(pl.col('FULLNAME').str.contains('I-'))
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # GeoPandas Test
    """)
    return


@app.cell
def _(conn, gpd, shapely):
    geo_df = conn.sql("""
    SELECT 
        * EXCLUDE (geom),
        ST_AsWKB(geom) AS geometry_wkb
    FROM 'data/roads/tl_2025_48453_roads.shp'
    """).df()

    geo_df["geometry"] = shapely.from_wkb(
        geo_df["geometry_wkb"].map(bytes)
    )

    roads_gdf = gpd.GeoDataFrame(
        geo_df.drop(columns=["geometry_wkb"]),
        geometry="geometry",
        crs="EPSG:4326",
    )
    return (roads_gdf,)


@app.cell
def _(roads_gdf):
    roads_gdf.explore()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
