/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Extended Events Setup Script for Row Group processing event 'column_store_rowgroup_read_issued', 'column_store_rowgroup_readahead_issued'
	Version: 1.5.0, August 2017

	Copyright 2015-2017 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

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

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server 2014
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'12'
begin
	set @errorMessage = (N'You are not running a SQL Server 2014. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end


/* Stop Session if it already exists */
IF EXISTS(SELECT *
				FROM sys.server_event_sessions sess
				INNER JOIN sys.dm_xe_sessions actSess
					on sess.NAME = actSess.NAME
				WHERE sess.NAME = 'cstore_XE_RowGroupReading')
BEGIN
	ALTER EVENT SESSION cstore_XE_RowGroupReading
		ON SERVER 
			STATE = STOP;
END

/* Drop the definition of the currently configured XE session */
IF EXISTS
    (SELECT * FROM sys.server_event_sessions sess
        WHERE name = 'cstore_XE_RowGroupReading')
BEGIN

    DROP EVENT SESSION cstore_XE_RowGroupReading
        ON SERVER;
	
END

CREATE EVENT SESSION [cstore_XE_RowGroupReading] ON SERVER 
	ADD EVENT sqlserver.column_store_rowgroup_read_issued(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.column_store_rowgroup_readahead_issued(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.sql_text,sqlserver.username))
	ADD TARGET package0.ring_buffer(SET max_memory=(51200))
	WITH (MAX_MEMORY=51200 KB);

GO


