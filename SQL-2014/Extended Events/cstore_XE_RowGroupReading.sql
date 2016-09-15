/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2012: 
	Extended Events Setup Script for Row Group processing event 'column_store_segment_eliminate'
	Version: 1.4.0, September 2016

	Copyright 2015-2016 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/


IF NOT EXISTS
    (SELECT * FROM sys.dm_xe_sessions
        WHERE name = 'cstore_XE_RowGroupReading')
BEGIN
	--ALTER EVENT SESSION cstore_XE_RowGroupReading
	--		ON SERVER STATE = STOP;

 --   DROP EVENT SESSION cstore_XE_RowGroupReading
 --       ON SERVER;

	CREATE EVENT SESSION [cstore_XE_RowGroupReading] ON SERVER 
		ADD EVENT sqlserver.column_store_rowgroup_read_issued(
			ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.sql_text,sqlserver.username)),
		ADD EVENT sqlserver.column_store_rowgroup_readahead_issued(
			ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.sql_text,sqlserver.username))
		ADD TARGET package0.ring_buffer(SET max_memory=(51200))
		WITH (MAX_MEMORY=51200 KB)
END
GO


