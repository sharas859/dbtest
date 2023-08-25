# dbtest
Takes large sensor measurement csv files and writes them to a sqlite database as blobs. Columns are grouped in chunks to prevent hitting the column limit. A seperate table keeps track of which indices are in which chunks.
Since this already prevents per column access in the database, chunks can also be gzipped to reduce the required storage space by a factor of ~5x.
