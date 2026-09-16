import time
from random import randint


# this function changes return value of passed function and returns tuple 
# so before usage it is important tu unpack tuple 
def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        used_time = time.time() - start_time
        return result, used_time
    return wrapper


# @measure_time
# def generate_numbers(quantity):
#     data = []
#     for i in range(quantity):
#         data.append(randint(2000, 200000))
#     return data

    
# result, used_time = generate_numbers(20)
# print(f"result = {result}")
# print(f"used time = {used_time}")



