# testing sarama cluster message guarantees

`sarama-cluster-tester produce` writes 1000 messages to the configured topic.

`sarama-cluster-tester stream` prints all of the messsages.

`sarama-clsuter-tester consume GROUP_ID` consumes the messages as a consumer group.

### produce

The producer writes 1000 messages to the topic and then exits.

This seeds the topic for use by consumer tests.

### consume

Each consumer process starts 5 consumer groups.

Four of the consumers are well-behaved, meaning they:

  * consume messages until they're instructed to shut down
  * deliver messages to the reporting routine
  * acknowledge messages after reporting

The other consumer is like the well-behaved ones, except:

  * it consumes 10 messages and then shuts down
  * it does not acknowledge messages after they're reported

A normal run of the consumer at a scale of 1, then, will process around 800
messages (4/5 consumers), then pause for a bit while rebalancing is triggered
due to the death of the bad consumer, and then process the remaining messages.

At the end, it reports:

```
2017/09/08 19:51:17 at=result found=1000 seen=1010
```

Meaning that all 1000 messages were observed, and 10 were re-transmitted after
the bad consumer died.

Running the consumer at a scale of 2 behaves similarly. At the end, the two
consumers report results like:

```
app[consumer.1]: 2017/09/08 19:54:18 at=result found=536 seen=536
app[consumer.2]: 2017/09/08 19:54:18 at=result found=484 seen=484
---
app[consumer.2]: 2017/09/08 19:56:00 at=result found=652 seen=662
app[consumer.1]: 2017/09/08 19:56:00 at=result found=352 seen=358
```

The number of results found is always >= 1000.
