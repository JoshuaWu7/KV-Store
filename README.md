# Something about the A4

## How to run the JAR file

``` shell
java \
-XX:+UseCompressedOops \
-Xmx64m \
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
-jar A4.jar
```

## A brief description of my design choices  

I made several optimizations to improve the performance of KVStore server. Namely:
- A multithreaded approach instead of a single threaded approach. Dedicated threads
for sending and receiving packets to avoid contention on the socket. 
- Pooled objects (e.g byte arrays for incoming packets) are between threads. After
a thread is done handling a packet, the byte array that backs the packet is returned
to the main thread for reuse. I also reuse the backing array in the process of decoding
and encoding the packets (though I could not reuse it for sending the packet out, because
it would be returned and used by another thread.)
- The public Buffer is an example, and it implements an Input/Output stream API so callers/callees 
do not mangle the state. 
- Using an alternate protobuf implementation (gcless) that does not create a bunch of
garbage in memory compared to protobuf. This saves a lot of time during garbage collection.
- Using an alternative map implementation that implements ConcurrentMap, which is 
significantly more memory efficient than the default ConcurrentHashMap
- Small number of threads because the ec2 instance is pretty bad.
- Using the same placement group / zone / instance type for both the client and server improved network
performance (and hence throughput of closed loop clients) by decreasing RTT. 
- The Builder constructor pattern is used to generate the responses. This is useful because
each response has optional fields. I just thought I'd learn something new for fun. It's also neater?
- I used the guava cache.
- the wrappers wrap objects (e.g. byte arrays, messages, payloads)

## Testing
- The test client may or may not be posted on piazza (depending on if I have time.
A description will be provided in the piazza public post as described in @134)
- More details to come (since I am trying to get the early bonus, and according
 to @153, I don't need to submit the client at the same time to get the bonus. )