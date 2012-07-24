Sigmatch
--------

Sigmatch is an algorithm for fast scalable multi-pattern matching. It uses
bloomfilters to determine if a text-string possibly matches a pattern or determines
its not in the set. If it detects that a text string may match a pattern we store a list of
possible matches inside a dictionary for quick run-time checking.

    S = sigmatch:new(RegularExpressions)
    sigmatch:match("some text string", S)

NOTE: this module uses a subset language for regular expressions. Meaning you have to keep
your expressions simple since it simply strips out regular expression characters.
Read the white paper below for more detail.

You can find the original whitepaper here
pages.cs.wisc.edu/~jignesh/publ/sigmatch.pdf
