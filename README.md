# testing sarama cluster message guarantees

heroku kafka:topics:create cluster-test-cyx --partitions 32 --replication-factor 3 --retention-time 24h

## testing theory

1. You define N to be the total number of unique kafka keys
2. You define M to be the total number of bits set (e.g. for a key K, M of 2 would mean
   the following messages):

   ```
   {key=K value=0}
   {key=K value=1}
   ```

3. You consume the topic, and do a SETBIT on a redis key.
4. You verify that all keys end up with all 0..M bits set.

## findings

### consumer=1

With consumer=1, we're consistently able to arrive at the expected outcome, with all N keys
having all M bits set.

### consumer>1

With consumer>1 of 4 successive runs, a minimum of 1 key, and a max of 4 (out
of 100) had missing bits set. The number of bits missing were in the range of
1 to 4 bits.

Tests were done for consumer=2, consumer=3, consumer=4, and the results were
largely similar, that some lossiness was observed.

## future tests

It _might_ be that the implementation of sarama-cluster is at fault here, or not.
A similar test could easily be executed by using the official kafka confluent go library.
