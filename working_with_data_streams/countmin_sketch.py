from random import randint
from hashlib import sha256, sha384, sha224, sha512, sha1
from collections import Counter
import heapq


# COUNTING THE NUMBER OF UNIQUE ITEMS IN A CONTINUOUS STREAM OF DATA


## You'd want to consider a count min sketch when 
## the keys of your hashtable will be too large to fit in memory
## If this isn't the case, then just go with a hash table.
## With a count min sketch, you're essentially combining the counts
## for multiple keys and storing them in one cell in order to conserve space
## This combination of counts done randomly, unintentionally and through collisions
## How can we then distinguish the counts for each of the keys involved in the combination?
## We simply rely on an approximation. This is why we use multiple hash functions.
## Not all will collide so we can take minimum count across all locations where
## the count for each key is stored and use that as an approximation for the frequency of each key.
## In the best case, that minimum value will be pure and not affected by any collision.
## In other cases, there'll still be some collision influencing that minimum count but we
## are confident that the value we have was influenced least by collisions.



# The example below counts the frequency of stocks purchased based on a stream of data
# Since the number of unique stocks is small, we don't need a count min sketch
# A simple hashtable will work in this case. However, this illustrates how 
# a count min sketch would work. The same idea can be applied when 
# the keys for the hash table are too large to fit in memory.


# 1. Simulate/generate data stream

## Each event would be of the form (stock, num_units_transacted)

data_stream = []

stock_tickers = ['apple', 'google', 'amazon', 'palantir', 'facebook', 'stripe', 'exxon', 'microsoft']

num_unique_stocks = len(stock_tickers)

for i in range(10000):
    random_index = randint(0, num_unique_stocks-1)

    current_stock = stock_tickers[random_index]

    amount =  randint(1, 10)

    data_stream.append((current_stock, amount))


# 2. Create the count min_sketch

## Setup hash functions
hash_functions = [sha256, sha384, sha224, sha512, sha1]
num_hash_functions = len(hash_functions)

### helper method for hashing
def hash_func_helper(hash_function, stock_ticker: str):
    """Hashes a given event and returns its row in the sketch"""

    byte_encoded_event = stock_ticker.encode('utf-8')

    row = hash(hash_function(byte_encoded_event).hexdigest()) % num_rows

    return row

## Determine the number of rows
num_rows = 10
# num_rows = int(log(num_unique_stocks, 2))
# how do you determine the optimum number of rows you need in
# order to improve the accuracy of your results?

## Create sketch. A sketch is essentially a 2-D Array / a matrix
sketch = [[0 for _ in range(num_hash_functions)] for _ in range(num_rows)]



# 3. Process the stream stock transation data

for i in range(len(data_stream)):
    
    event = data_stream[i]

    stock = event[0]
    stock_amount = event[1]

    for i in range(num_hash_functions):
        # utilize each of the hash functions to determine
        # where to store the count for a given stock's event
        # within the sketch
        
        target_column = i

        target_row = hash_func_helper(hash_functions[i], stock)

        sketch[target_row][target_column] += stock_amount


# 4. Get frequency using a hashtable (for comparison)

hashtable_count = dict()

for event in data_stream:

    stock = event[0]
    amount = event[1]
    
    if stock in hashtable_count:
        hashtable_count[stock] += amount

    else:
        hashtable_count[stock] = amount

print('\nResults from hash table count: \n')
print(hashtable_count)

# 5. Get frequency from the sketch

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

    count_min_results[current_stock] = (min_heap[0])
    

print('\nResults from count min sketch: \n')
print(count_min_results)


# 6. Results show that the sketch always returns an accurate count when
# the number of rows is at least equal to the number of unique items in
# the data stream. This can be confirmed by comparing the output of the sketch to that of the hash table
# But if we could store that many keys, we wouldn't use the sketch in the first place.
# We're using the sketch cos we want to store less (rows) than the number of unique
# keys within the stream. In that case, we can try to figure out how many rows & hash fucntions
# would give us the right balance of low memory usage and high accuracy. More rows give us a higher
# accuracy but that uses more memory. Fewer rows give a lower accuracy, but uses less memory.
