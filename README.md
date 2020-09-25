ccdb
====

Case Context Database

What the what?
--------------

A redis wire compatible, disk backed, data store. Basically if you run it from an nvme cloud instance it's fast enough for most use cases.

Why?
----

Sometimes I want to use redis but I don't have enough memory for it. There are other projects that are similar to this one, but I wanted to try my hand at it.

Where to use:
-------------

* Development
* Places where writes are ok to be slow

Notes
-----

An example config is included, those are the only three options right now.