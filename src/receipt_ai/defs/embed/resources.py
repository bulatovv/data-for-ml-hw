import dagster as dg
from sentence_transformers import SentenceTransformer
from typing_extensions import override


class EmbedderResource(dg.ConfigurableResource[SentenceTransformer]):
    """Dagster resource for SentenceTransformer embedder."""
    
    model_name: str
   
    @override
    def create_resource(self, context: dg.InitResourceContext) -> SentenceTransformer:
        return SentenceTransformer(self.model_name)


