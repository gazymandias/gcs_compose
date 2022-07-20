**Google Cloud Storage**

*Infinite Compose Accumulator Tree Example*

This example utilises the compose operation in GCS to perform an infinite join of 32 objects across multiple operations.

Credit:

https://cloud.google.com/community/tutorials/cloud-storage-infinite-compose

By default compose is limited to 32 objects in a single operation. Here we use multiple accumulators across the composite objects, accumulating the accumulators until you get to one. This forms a tree-like structure of accumulators:

<img height="200" src="/Users/gareth/PycharmProjects/gcs_compose/compose_accumulator_tree.png" width="200"/>