# Columnstore Indexes Scripts Library (CISL)
![licence badge]][licence]
[![stars badge]][stars]
[![forks badge]][forks]
[![issues badge]][issues]

[licence badge]:https://img.shields.io/badge/License-Apache%202.0-blue.svg
[stars badge]:https://img.shields.io/github/stars/NikoNeugebauer/CISL.svg
[forks badge]:https://img.shields.io/github/forks/NikoNeugebauer/CISL.svg
[issues badge]:https://img.shields.io/github/issues/NikoNeugebauer/CISL.svg

[licence]:https://github.com/NikoNeugebauer/CISL/blob/master/LICENSE
[stars]:https://github.com/NikoNeugebauer/CISL/stargazers
[forks]:https://github.com/NikoNeugebauer/CISL/network
[issues]:https://github.com/NikoNeugebauer/CISL/issues

Welcome to CISL - Microsoft SQL Server Columnstore Indexes Scripts Library

A columnstore index is a technology for storing, retrieving and managing data by using a columnar data format, called a columnstore. More information you can find here: https://msdn.microsoft.com/en-us/library/gg492088.aspx

The library is launched with the very first release on 1st of September 2015. 
The current release (1.4.0) is supporting SQL Server 2012, 2014, 2016 & Azure SQL Database.  

At the moment, the CISL consists from 8 scripts and 9 stored procedures (8 standrad & a maintenance solution).  
Every CISL script comes in 2 variants: a simple executable script & stored procedure.  
Stored Procedures are here so that you can store them inside your database and use more commonly and include them into your own solutions.  

CISL consists from the following scripts:  

1. SQL Info (sqlserver_instance_info.sql) - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session.  

2. Suggested Tables (suggested_tables.sql) - Lists tables which potentially can be interesting for implementing Columnstore Indexes.  

3. Row Groups (row_groups.sql) - Shows detailed information on the Columnstore Row Groups.  

4. Segment Alignment (alignment.sql) - Shows the alignment (ordering) between the different Columnstore Segments.  

5. Fragmentation (fragmentation.sql) - Shows the different types of Columnstore Indexes Fragmentation.  (Note: SQL Server 2012 version does not exist, since Nonclustered Columnstore Indexes in SQL Server 2012 are non-updatable and hence have no fragmentation)

6. Dictionaries (dictionaries.sql) - Shows detailed information about the Columnstore Dictionaries.  

7. Memory (memory.sql) - Shows the content of the Columnstore Object Pool.  

8. Row Groups Details (row_groups_details.sql) - Shows detailed information on the Columnstore Row Groups, listing all individual row groups and filtering them based on different criterias. 


## License
[Apache License 2.0](\LICENSE)
