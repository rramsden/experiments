Symmetric Push-Pull Protocol
============================

SPSP is a Gossip-based algorithm for computing either a sum or an average across every node in a cluster.

Fun Facts
---------

The algorithm takes `2*n` messages where n is the number of nodes to converge on an answer. Its also completely
asynchronous :)

How does it work?
-----------------

SPSP works by using keeping track of two numbers at each node. One acts as a weight which is used to 
figure out how much of the overall sum you have. If you want to learn more read the 
original (whitepaper)[http://www.thinkmind.org/download.php?articleid=ap2ps_2011_2_10_30063] or read
my blog (article)[http://rramsden.ca/blog/2012/06/27/gossiping-in-distributed-systems-part-ii/] on it.
