import numpy as np
from sklearn.metrics.pairwise import cosine_distances

z = np.load("gnn-training/graphsage/customer_embeddings.npy")

print("Shape:", z.shape)
print("Min value:", z.min(), "Max value:", z.max())

D = cosine_distances(z)
nonzero = D[D > 0]

print("Min non-zero distance:", nonzero.min())
print("Max distance:", D.max())
print("Mean distance:", nonzero.mean())


