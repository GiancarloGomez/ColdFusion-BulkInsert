# ColdFusion BulkInsert
A CFC that allows you to use [MSSQL's BULK INSERT](https://docs.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql)
by simply following simple naming conventions and an existing [XML Format File](https://docs.microsoft.com/en-us/sql/relational-databases/import-export/use-a-format-file-to-bulk-import-data-sql-server) which can be created manually or by using the [bcp Utility](https://docs.microsoft.com/en-us/sql/tools/bcp-utility)

A special thanks to __Gert Franz__ ([gert_railo](https://twitter.com/gert_railo),[gert_rasia](https://twitter.com/gert_rasia)) as this CFC was inspired by his __"How to make CFML script fast"__ session at
[Into The Box 2017](https://www.intothebox.org/#sessions). He also went above and beyond to review initial parts of the code base
and help further optimize and teach me some additional things. If you want help optimizing your code, he is the man to call!

#### Example Usage
``` php
bulkInsert = new BulkInsert();

// minimum required arguments
bulkInsert.process(
    formatPath  : expandPath("./formats/"),
    dataPath    : expandPath("./temp/"),
    table       : "my_table",
    orderby     : "my_id",
    datasource  : "thesource"
);
```

#### Naming Conventions
The only requirement is that the name of the XML Format file is the same as the table you are working with.
So if your table is called users, then the script is going to look for a file called users.xml within the formatPath
you pass.

#### Arguments
- __formatPath__<br />
The absolute path to the folder where the format files exist
- __dataPath__<br />
The absolute path to the folder where the data files will be generated
- __table__<br />
The SQL table we are working with, the files used this as the name convention as well
- __orderby__<br />
The order used in the query required for paging
- __datasource__<br />
The datasource to pull records from
- __iterator__<br />
The number of records to work with at a time, defaults to 100k
- __doCleanup__<br />
A boolean value to clean up after itself when complete (delete generated file)
- __replaceSQL__<br />
Boolean value to run string replacements on SQL vs ColdFusion
- __singleFieldSQL__<br />
Boolean value to return all data in SQL as 1 field, if set to true replaceSQL is ignored
- __debug__<br />
A boolean value to return debug data
- __debugOnly__<br />
A boolean value to only debug (skips data pull and insert)
- __doInsert__<br />
A boolean value to run the final BULK INSERT statement. Set to false to only generate the file
- __total__<br />
A numeric value ot total records to process, this is handled internally but if a recordcount is requested outside of this or want to force a total you can pass it in


