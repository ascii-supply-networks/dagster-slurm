import ray

ray.init()

@ray.remote
def my_function(x):
    return x * 2

futures = [my_function.remote(i) for i in range(4)]
print(ray.get(futures))