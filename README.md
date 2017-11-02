# artemis-load-generator


Command line to be used for 1 Producer 1 Consumer test:

```
$ java -jar destination-bench.jar --bytes 100 --protocol artemis --url tcp://localhost:61616 ---out /tmp/test.txt --name q6 --iterations 10000000 --runs 1 --warmup 20000

```
Command line to be used for 10 Producer 10 Consumer test on the same queue:

```
$ java -jar destination-bench.jar --bytes 100 --protocol artemis --url tcp://localhost:61616 ---out /tmp/test.txt --name q6 --iterations 10000000 --runs 1 --warmup 20000 --forks 10
```
Command line to be used for 10 Producer 10 Consumer test sharing 5 queues (ie 2 Producers/Consumers for each queue):

```
$ java -jar destination-bench.jar --bytes 100 --protocol artemis --url tcp://localhost:61616 ---out /tmp/test.txt --name q6 --iterations 10000000 --runs 1 --warmup 20000 --forks 10 --destinations 5
```
