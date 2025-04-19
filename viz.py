import umap
import polars as pl
import plotly.express as px
from sentence_transformers import SentenceTransformer

embedder = SentenceTransformer('deepvk/USER2-small')


items = pl.read_parquet('storage/items_embedded.parquet').drop_nulls('item')

bar_df = (
    items
    .group_by("item_type")
    .len()
    .rename({"len": "count"})
    .sort("count", descending=True)
    .to_pandas()
)

fig = px.bar(
    bar_df,
    x="item_type",
    y="count",
    title="Item count by item_type",
    text_auto=True,
    template="plotly_white"
).update_layout(xaxis_title="", yaxis_title="Items")
fig.show()
fig.write_image('plots/item_types_bar.png')


for semi_supervised in [True, False]:
    X = items['embeddings'].to_numpy() 
    
    y = items['item_type'].cast(pl.Categorical).to_physical().fill_null(-1)

    reducer = umap.UMAP(unique=True)
    
    if semi_supervised:
        X_reduced = reducer.fit_transform(X, y)
    else:
        X_reduced = reducer.fit_transform(X)
        

    categories = items['item_type'].cast(pl.Categorical).cat.get_categories().to_list()
    
    y_codes = y.to_numpy()
    labels = ['Unknown' if code == -1 else categories[code] for code in y_codes]

    plot_data = pl.DataFrame({
        "x": X_reduced[:, 0],
        "y": X_reduced[:, 1],
        "item_type": labels,
        "item": items["item"]
    }).to_pandas()

    fig = px.scatter(
        plot_data,
        x="x",
        y="y",
        color="item_type",
        hover_data=["item"],
        title="UMAP Projection of Item Embeddings",
        labels={"x": "UMAP Dimension 1", "y": "UMAP Dimension 2"}
    )

    fig.update_layout(
        legend_title_text='Item Type',
        hovermode='closest',
        template='plotly_white'
    )

    fig.show()
    if semi_supervised:
        fig.write_image('plots/embeds_scatter_semisupervised.png')
    else:
        fig.write_image('plots/embeds_scatter.png')

