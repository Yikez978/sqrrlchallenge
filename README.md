Background

In simplest terms, Accumulo is a sorted key/value store.  It stores mappings of keys to values, and all key/value entries are sorted by key. This allows fast indexed access for get and put calls, and also supports quickly scanning all entries starting at or near a specific key. 

Accumulo distributes work across a cluster of servers by designating responsibility for different ranges of keys to different servers.  In Accumulo terminology, each range is a 'tablet', and each server is a 'tablet server'.  A tablet server is responsible for all reads and writes to entries within its ranges of keys. Every tablet is served by exactly one tablet server at a time.  Each tablet server can serve zero to many tablets at once. 

Exercise

For this exercise, write a class that 
takes as input a fixed number of tablets and a set of tablet servers
calculates the ranges that define each tablet
assigns each tablet to a tablet server
You can make a few simplifying assumptions for the purpose of this exercise
The key space is the range of values from 0 to Java's Long.MAX_VALUE 
Ranges should be defined by simply splitting up the key space into equal sized chunks
Once the ranges that define a set of tablets are defined, they do not change
Note that these assumptions are not true in the actual Accumulo implementation! Accumulo's elastic distribution strategy is very robust and flexible.  

After getting the basics in place, add support for adding or removing servers to the list. A change to the list of servers should result in a rebalancing of the load of tablets across the servers such that each tablet server has as close to an equal number of tablets as possible.

Your rebalancing behavior should minimize reassignments of tablets from one server to another. This preference is important to the performance characteristics of Accumulo, and we can discuss the reason why in person. 

Please do not over-optimize your code. For this exercise, we are much more interested in reviewing readable, well-structured code than potentially optimal solutions.  

Example

Input: 4 tablets, 2 tablet severs

There will be four tablets

tablet0 has keys from 0-2305843009213693951
tablet1 has keys from 2305843009213693952-4611686018427387903
tablet2 has keys from 4611686018427387904-6917529027641081855
tablet3 has keys from 6917529027641081856-9223372036854775807

And they could start in the following mapping 

tablet0 --> tabletserver0
tablet1 --> tabletserver1
tablet2 --> tabletserver0
tablet3 --> tabletserver1

If a new tablet server was added, the load balancer would move one of the tablets off onto the new tablet server. The workload would not be exactly equal across servers, because 4 tablets cannot be divided evenly across 3 servers. 

tablet0 --> tabletserver0
tablet1 --> tabletserver1
tablet2 --> tabletserver2
tablet3 --> tabletserver1

Interface

public abstract class Master {
  
  protected int numTablets;
  protected List<String> serverNames; 
  
  public Master(int numTablets, List<String> serverNames) { 
    this. numTablets = numTablets;
    this.serverNames = serverNames;
  }
  
  public abstract String getServerForKey(long key); 

  public abstract void addServer(String serverName);  
  public abstract void removeServer(String serverName);
}
