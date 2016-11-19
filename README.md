# SimpleDB
A database implementation in Java

**SimpleDB** is a multi-user transactional database server written in Java, which interacts with Java client programs via JDBC. SimpleDB uses the Ant build tool to compile the code and run tests. Ant is similar to make, but the build file is written in XML and is somewhat better suited to Java code. Most modern Linux distributions include Ant.

To run the  unit tests use the test build target:

`$ ant runtest -Dtest=TupleTest`



*SimpleDB consists of:*
-----------------------

1) Classes that represent fields, tuples, and tuple schemas.

2) Classes that apply predicates and conditions to tuples; 

3) One or more access methods (e.g., heap files) that store relations on disk and provide a way to iterate through tuples of those relations; 

4) A collection of operator classes (e.g., select, join, insert, delete, etc.) that process tuples; 

5) A buffer pool that caches active tuples and pages in memory and handles concurrency control and transactions (neither of which you need to worry about for this assignment); and, 

6) A catalog that stores information about available tables and their schemas.



*SimpleDB does not include many things that you may think of as being a part of a "database." In particular, SimpleDB does not have:* 
-----------------------

1) Views. 
2) Data types except integers and fixed length strings. 
3) Query optimizer. 
4) Indices.


Most of this Code is a standard API adapted from Prof. Sam Madden's 6.830 class at MIT and the cs544 class at U. Of Washington.

*Changes done from the original API:-*
-------------------------

1) Implemented the getPage(), pinPage(), unpinPage(), and evictPage() methods in src/simpledb/BufferPool.java (not present in the actual code but present as a compiled class in bin)

2) Implemented the getNumEmptySlots(), setSlot() and getSlot() methods in src/simpledb/HeapPage.java (not present in the actual code but present as a compiled class in bin)

3) Implemented SNL, PNL and SMJ Join Algorithms in Join.java ( Also created a new class called pagewiseDBIterator for page wise iteration in PNL )
