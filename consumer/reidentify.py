import json
import hnswlib
import numpy as np

class Reidentify():
    def __init__(self, image_file, max_elements, dim, kd_tree_file):
        self.image_file = image_file
        self.max_elements = max_elements
        self.dim = dim
        self.kd_tree_file = kd_tree_file
        with open(image_file, 'r') as f:
            self.images = json.load(f)
        self.p = hnswlib.Index(space = 'cosine', dim = dim)
        self.p.load_index(kd_tree_file, max_elements = max_elements)
    
    def re_id(self, tracks):
        lst = [i["hash"] for i in tracks]
        lst = np.array(lst)
        labels, distances = self.p.knn_query(lst, k=1)
        distance = distances.ravel()
        index = distance.argmax()
        max_dist = distance[index]
        max_label = labels.ravel()[index]
        return self.images[str(max_label)], max_dist, index
