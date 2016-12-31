import numpy as np

def distance(pt_1, pt_2):
    pt_1 = np.array((pt_1[0], pt_1[1]))
    pt_2 = np.array((pt_2[0], pt_2[1]))
    return np.linalg.norm(pt_1-pt_2)

def closest_node(node, nodes):
    pt = []
    dist = 9999999
    for n in nodes:
        if distance(node, n) <= dist:
            dist = distance(node, n)
            pt = n
    return pt

a = []
for x in range(50000):
    a.append((np.random.randint(0,1000),np.random.randint(0,1000)))

some_pt = (100, 2)

s = closest_node(some_pt, a)
print s