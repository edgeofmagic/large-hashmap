A scalable, concurrent hash map implementation incorporating extendible 
hashing<a href="#footnote-1"><sup>[1]</sup></a> and 
hopscotch hashing<a href="#footnote-3"><sup>[3]</sup></a>. These two techniques are employed 
hierarchically&mdash;each of the buckets indexed by the extendible hashing directory is 
treated as an independent hash table, which is managed internally using hopscotch hashing techniques.
Discussions of both hashing techniques use the term *bucket* for concepts that, from the 
perspective of this project, are dissimlar. To avoid confusion, the units indexed by extensible
hashing are called *segments* here. The term *bucket* in this project is consistent with the definition 
used in discussions of hopscotch hashing: a set of map 
entries that share the same hash index within a segment, all located in close proximity 
to that index in the segment's array structure. In addition to establishing a hierarchical structure, these 
hashing techniques address different problems. Extendible hashing manages map growth; hopscotch 
hashing resolves collisions and distributes map entries in the underlying array for 
efficient retrieval.
<h3>Concurrency</h3>
All operations are thread-safe. Retrieval operations do not entail blocking; they 
execute concurrently with update operations. Update operations may block if overlapping updates are
attempted on the same segment. Segmentation is configurable, so the 
probability of update blocking is (to a degree) under the programmer's 
control. Retrievals reflect the result of update operations that complete 
before the onset of the retrieval, and may also reflect the state 
of update operations that complete after the onset of the retrieval. 
Iterators do not throw `java.util.ConcurrentModificationException`.
If an entry is contained in the map prior to the iterator's creation and
it is not removed before the iterator is exhausted, it will be returned by the 
iterator. The entries returned by an iterator may or may not reflect updates
that occur after the iterator's creation.
Iterators themselves are not thread-safe; although iterators can be used 
concurrently with operations on the map and other iterators, an iterator 
should not be used by multiple threads.

<h3>Extendible Hashing</h3>
Extendible hashing allows the 
map to expand gracefully, distributing the cost of resizing into constant-sized 
increments as the map grows. The map is partitioned into fixed-size 
segments. Hash values are mapped to segments through a central directory, 
which is, essentially, a radix search tree flattened into an array.
When a segment reaches the load factor threshold it splits into two 
segments. When a split would exceed directory capacity, the directory 
doubles in size. The current implementation does not merge segments to 
reduce capacity as entries are removed. 
<h4>Concurrency during splits and directory expansion</h4>
Ellis<a href="#footnote-2"><sup>[2]</sup></a> describes strategies for concurrent 
operations on extendible hash tables. The strategy used in this project is informed
by this paper, but does not follow it precisely.
When an update causes a segment to split, the updating thread acquires
a lock on the directory to assign references to the new segments in the 
directory. If a split forces the directory to expand, the updating thread 
keeps the directory locked during expansion. A directory lock will not block 
a concurrent update unless that update forces a segment split.
<h3>Hopscotch Hashing</h3>
This implementation uses Hopscotch hashing
within segments for collision resolution. Hopscotch hashing is similar to 
linear probing, except that colliding map entries all reside in relatively 
close proximity to each other, resulting
in shorter searches and improved cache hit rates. Hopscotch hashing also 
yields good performance at load factors as high as 0.9.<p> 
At present, the hop range (longest distance from the hash index that
a collision can be stored) is set at 32, and the maximum search range
for an empty slot during an add operation is 512.
Hopscotch hashing was designed to support a high degree of concurrency, 
including non-blocking retrieval. This implementation follows the 
concurrency strategies described in the originating papers, with minor 
variations.
<h3>Long Hash Codes</h3>
ConcurrentLargeHashMap is designed to support hash maps that can expand to very 
large size (> 2<sup>32</sup> items). To that end, it uses 64-bit hash codes.
Lacking a universally available 64-bit cognate of `Object.hashCode()`, this project
defines two alternatives for producing 64-bit hash codes from objects.
If circumstances permit, a class to be used as a key may directly implement
the `org.logicmill.util.LargeHashable`interface. In cases where it is not practical or possible to modify or
extend the key class directly, this project relies on *key adapters*&mdash;helper classes
that compute long hash codes for specific key types. Key adapters can often be implemented
concisely with anonymous classes, provided when the map is constructed. Keys of
type `CharSequence` (which includes keys of type `String`) or `byte[]` can be used without a
key adapter.
<h4>SpookyHash</h4>
Because array indices within a segment consist of bit fields
extracted directly from hash code values, it is important to choose a hash 
function that reliably exhibits avalanche and uniform distribution. 
An implementation of Bob Jenkins' SpookyHash V2<a href="#footnote-4"><sup>[4]</sup></a> 
algorithm is included in this project, and is highly recommended. The performance
of SpookyHash, with respect to both speed and the statistical properties of 
the resulting hash codes, campares favorably among
hashing algorithms that produce 64- or 128-bit results.
<p id="footnote-1"><sup>[1]</sup> <a href="http://dx.doi.org/10.1145%2F320083.320092"> Fagin, et al, 
"Extendible Hashing - A Fast Access Method for Dynamic Files", 
ACM TODS Vol. 4 No. 3, Sept. 1979</a></p>
<p id="footnote-2"><sup>[2]</sup> <a href="http://dl.acm.org/citation.cfm?id=588072">
Ellis, "Extendible Hashing for Concurrent Operations and Distributed Data", 
PODS 1983</a></p>
<p id="footnote-3"><sup>[3]</sup> 
<a href="http://mcg.cs.tau.ac.il/papers/disc2008-hopscotch.pdf">
Herlihy, et al, "Hopscotch Hashing", DISC 2008</a>.</p>
<p id="footnote-4"><sup>[4]</sup> <a href="http://burtleburtle.net/bob/hash/spooky.html">
http://burtleburtle.net/bob/hash/spooky.html</a>

