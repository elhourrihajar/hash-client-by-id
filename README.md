# hash-client-by-id
hash data of a client by providing the id 
the cmd line in spark-master for hashing the data of client 5 for example is :
./spark-submit --packages "com.github.scopt:scopt_2.12:4.1.0" --class hashClient --master spark://172.31.249.172:7077 /home/nodekamicode/hashClient/target/scala-2.12/hashclient_2.12-1.0.jar -o 2
please refer to the attachement below to see the expected result for hash by client.
