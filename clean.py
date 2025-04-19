import umap
import hdbscan
import polars as pl

original_items = pl.read_parquet('storage/items_embedded.parquet').with_row_index()

original_items = original_items.with_columns(pl.col('item_type').replace('Не определена', None))
print(original_items.null_count())


for col in ['item_quantity', 'item_price']:
    q1 = original_items.select(pl.col(col).quantile(0.25)).item()
    q3 = original_items.select(pl.col(col).quantile(0.75)).item()

    iqr = q3 - q1

    lower_bound = q1 - 3 * iqr
    upper_bound = q3 + 3 * iqr

    outliers = original_items.filter((pl.col(col) < lower_bound) | (pl.col(col) > upper_bound)).sort(col)

    print(outliers)


items = original_items.unique('item').drop_nulls('item')

X = items['embeddings'].to_numpy() 
y = items['item_type'].cast(pl.Categorical).to_physical().fill_null(-1)

reducer = umap.UMAP(n_components=8, min_dist=0.0, random_state=42)
X_reduced = reducer.fit_transform(X, y)

clusterer = hdbscan.HDBSCAN(min_cluster_size=50, min_samples=30)
clusterer.fit(X_reduced)
clusters = clusterer.labels_

items = items.with_columns(cluster=pl.Series(clusters))
majority_type = (
    items.filter(pl.col('cluster') != -1)
    .group_by('cluster')
    .agg(majority_type=pl.col('item_type').mode().first()) 
)

print(majority_type)

items = items.join(majority_type, on='cluster')
items = items.with_columns(
    item_type=pl.coalesce(
        pl.col('item_type'),
        pl.col('majority_type')
    )
)
items = items.filter(
    (pl.col('cluster') == -1) |
    (pl.col('item_type') != pl.col('majority_type'))
)


(
    original_items
    .update(items, on='index', how='left')
    .drop_nulls(['item', 'item_type'])
    .write_parquet('storage/items_cleaned.parquet')
)


umap_visualizer = umap.UMAP(n_components=2, min_dist=0.1, random_state=42)
X_vis = umap_visualizer.fit_transform(X, y)

import pandas as pd
import plotly.express as px
df_vis = pd.DataFrame({
    'x': X_vis[:, 0],
    'y': X_vis[:, 1],
    'cluster': clusters,
    'item_type': y
})

fig = px.scatter(
    df_vis,
    x='x',
    y='y',
    color=df_vis['cluster'].astype(str),
    hover_data=['item_type'],
    title="2D Visualization of Clustered Items"
)
fig.show()
fig.write_image('plots/clustering_scatter.png')
