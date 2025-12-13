import numpy as np
from sklearn.neighbors import NearestNeighbors
import json

X = np.load("gnn-training/graphsage/customer_embeddings.npy")
ids = json.load(open("gnn-training/graphsage/customer_embedding_ids.json"))

knn = NearestNeighbors(n_neighbors=6, metric="cosine")
knn.fit(X)

distances, indices = knn.kneighbors(X)

for i, (dists, nbrs) in enumerate(zip(distances, indices)):
    for dist, j in zip(dists[1:], nbrs[1:]):  # skip self
        if dist < 0.3:  # provisional
            print(ids[i], "≈", ids[j], "dist=", dist)
