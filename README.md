# MapReduce

### Complexity (Lab 1 - Part 1)

To find the sequential complexity, we are considering the sum of all Mappers and Reducers running in sequential.
- The space complexity is decided by the size of all key-value pairs input and output by all mappers and reducers.
- The time complexity is determined by the total execution time for all mappers and reducers. 

Assume N documents, M words, total document size S,
> The space and time complexity are both **O(S)**.
