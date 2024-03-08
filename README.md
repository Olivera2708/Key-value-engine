# Key value engine

The key-value engine is a minimalist yet powerful data storage architecture that pairs unique keys with corresponding values, enabling rapid access and manipulation of data. It offers versatility across applications, from optimizing web performance with caching mechanisms to supporting large-scale distributed databases. With its efficient retrieval, scalability, and reliability features, the key-value engine remains indispensable in modern data management, empowering organizations to harness the full potential of their data assets.

## Operations Supported by the System:

- **PUT**: Adds data by accepting a string key and a bit array value, returning a boolean value indicating success.
  
- **GET**: Retrieves information by accepting a string key and returning the corresponding bit array value.
  
- **DELETE**: Removes a record by accepting a string key and returning a boolean value indicating success.
  
- **LIST**: Searches records by prefix, accepting a string prefix and returning a list of values whose keys start with the specified prefix.
  
- **RANGE SCAN**: Searches records within a specified range, accepting minimum and maximum string key values, and returning a list of values whose keys fall within the range.

## Data Structures Used:

- **WAL (Write Ahead Log)**
- **Bloom Filter**
- **B-Tree**
- **Merkle Tree**
- **Memtable**
- **SSTable (Data, Index, and Summary)**
- **Cache**
- **LSM Tree (Log-Structured Merge Tree)**
- **Token Bucket**
- **Skip List**
- **HyperLogLog**
- **Count-Min Sketch**
- **SimHash**
