# artemis-load-generator


Command line to be used:


```
java -jar destination-bench.jar --bytes 100 --protocol artemis --url tcp://localhost:61616 --name q6 --iterations 10000000 --runs 1 --warmup 20000
java -jar statistics-summary-generator.jar --input /tmp/consumer.dat --iterations 10000000 --runs 1 --warmup 20000 > bounded.txt
```
