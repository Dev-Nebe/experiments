from math import log
from random import randint
from hashlib import sha256, sha384, sha224, sha512, sha1
from collections import Counter
import heapq


data_stream = []

stock_tickers = ['apple', 'google', 'amazon', 'palantir', 'facebook', 'stripe', 'exxon', 'microsoft']

num_unique_stocks = len(stock_tickers)


# generate data stream
for i in range(10000):
    random_index = randint(0, num_unique_stocks-1)

    current_stock = stock_tickers[random_index]

    data_stream.append(current_stock)


# create count min_sketch
hash_functions = [sha256, sha384, sha224, sha512, sha1]
num_hash_functions = len(hash_functions)
num_rows = 5
# num_rows = int(log(num_unique_stocks, 2))
# how do you determine the optimum number of rows you need in
# order to improve the accuracy of your results?

sketch = [[0 for _ in range(num_hash_functions)] for _ in range(num_rows)]


# helper method for hashing
def hash_func_helper(hash_function, event: str):
    """Hashes a given event and returns its row in the sketch"""

    byte_encoded_event = event.encode('utf-8')

    row = hash(hash_function(byte_encoded_event).hexdigest()) % num_rows

    return row


# view empty sketch
# print(sketch)


# process stream
for i in range(len(data_stream)):
    event = data_stream[i]

    for i in range(num_hash_functions):
        
        target_column = i

        target_row = hash_func_helper(hash_functions[i], event)

        sketch[target_row][target_column] += 1


# view populated sketch
# print(sketch)

# get frequency from hashtable
hashtable_count = dict(Counter(data_stream))
print('\nResults from hash table count: \n')
print(hashtable_count)

# get frequency from sketch
# 1. Compute the hash for a given item
# 2. Determine what row the count of that item lives on, 
# based on the output of the hash fucntion
# 3. Get the value of that row and column
# 4. Add it to our min heap
# 5. Repeat for the remaining hash functions
# 6. Determine the count by checking the top of the heap 

count_min_results = dict()

for _ in range(len(stock_tickers)):

    current_stock = stock_tickers[_]

    min_heap = list()

    for i in range(num_hash_functions):
            
        target_column = i

        target_row = hash_func_helper(hash_functions[i], current_stock)

        current_count = sketch[target_row][target_column]

        heapq.heappush(min_heap, current_count)

    count_min_results[current_stock] = (min_heap[0], min_heap[:])
    

print("\n")

print('Results from count min sketch: \n')
print(count_min_results)
