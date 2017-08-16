/*
	CSIL - Columnstore Indexes Scripts Library for Azure SQL Database: 
	Extended Events Setup Script for Batch Execution Mode events 'batch_hash_join_separate_hash_column,'batch_hash_table_build_bailout'&'potential_batch_mode_colocated_join'
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

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end


/* Stop Session if it already exists */
IF EXISTS(SELECT *
				FROM sys.database_event_sessions sess
				INNER JOIN sys.dm_xe_database_sessions actSess
					on sess.NAME = actSess.NAME
				WHERE sess.NAME = 'cstore_XE_BatchMode')
BEGIN
	ALTER EVENT SESSION cstore_XE_BatchMode
		ON DATABASE 
			STATE = STOP;
END

/* Drop the definition of the currently configured XE session */
IF EXISTS
    (SELECT * FROM sys.database_event_sessions sess
        WHERE name = 'cstore_XE_BatchMode')
BEGIN
    DROP EVENT SESSION cstore_XE_BatchMode
        ON DATABASE;
END

/* Create a new default session */
CREATE EVENT SESSION [cstore_XE_BatchMode] ON DATABASE 
	ADD EVENT sqlserver.batch_hash_join_separate_hash_column(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.batch_hash_table_build_bailout(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.potential_batch_mode_colocated_join(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username))
	ADD TARGET package0.ring_buffer(SET max_memory=(51200))
	WITH (MAX_MEMORY=51200 KB);

GO
/*
	CSIL - Columnstore Indexes Scripts Library for Azure SQL Database: 
	Extended Events Setup Script for Index Creation events 'columnstore_index_rebuild'
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

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end


/* Stop Session if it already exists */
IF EXISTS(SELECT *
				FROM sys.database_event_sessions sess
				INNER JOIN sys.dm_xe_database_sessions actSess
					on sess.NAME = actSess.NAME
				WHERE sess.NAME = 'cstore_XE_IndexCreation')
BEGIN
	ALTER EVENT SESSION cstore_XE_IndexCreation
		ON DATABASE 
			STATE = STOP;
END

/* Drop the definition of the currently configured XE session */
IF EXISTS
    (SELECT * FROM sys.database_event_sessions sess
        WHERE name = 'cstore_XE_IndexCreation')
BEGIN
    DROP EVENT SESSION cstore_XE_IndexCreation
        ON DATABASE;
END

/* Create a new default session */
CREATE EVENT SESSION [cstore_XE_IndexCreation] ON DATABASE 
	ADD EVENT sqlserver.columnstore_index_rebuild(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username))
	ADD TARGET package0.ring_buffer(SET max_memory=(51200))
	WITH (MAX_MEMORY=51200 KB);

GO

/*
	CSIL - Columnstore Indexes Scripts Library for Azure SQL Database: 
	Extended Events Setup Script for Memory events 'column_store_object_pool_hit', 'column_store_object_pool_miss'
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

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end


/* Stop Session if it already exists */
IF EXISTS(SELECT *
				FROM sys.database_event_sessions sess
				INNER JOIN sys.dm_xe_database_sessions actSess
					on sess.NAME = actSess.NAME
				WHERE sess.NAME = 'cstore_XE_Memory')
BEGIN
	ALTER EVENT SESSION cstore_XE_Memory
		ON DATABASE 
			STATE = STOP;
END

/* Drop the definition of the currently configured XE session */
IF EXISTS
    (SELECT * FROM sys.database_event_sessions sess
        WHERE name = 'cstore_XE_Memory')
BEGIN

    DROP EVENT SESSION cstore_XE_Memory
        ON DATABASE;
	
END

/* Create a new default session */
CREATE EVENT SESSION [cstore_XE_Memory] ON DATABASE 
	ADD EVENT sqlserver.column_store_object_pool_hit(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.column_store_object_pool_miss(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username))
	ADD TARGET package0.ring_buffer(SET max_memory=(51200))
	WITH (MAX_MEMORY=51200 KB);

GO



/*
	CSIL - Columnstore Indexes Scripts Library for Azure SQL Database: 
	Extended Events Setup Script for Row Group reading with events
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

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end


/* Stop Session if it already exists */
IF EXISTS(SELECT *
				FROM sys.database_event_sessions sess
				INNER JOIN sys.dm_xe_database_sessions actSess
					on sess.NAME = actSess.NAME
				WHERE sess.NAME = 'cstore_XE_RowGroupReading')
BEGIN
	ALTER EVENT SESSION cstore_XE_Memory
		ON DATABASE 
			STATE = STOP;
END

/* Drop the definition of the currently configured XE session */
IF EXISTS
    (SELECT * FROM sys.database_event_sessions sess
        WHERE name = 'cstore_XE_RowGroupReading')
BEGIN
    DROP EVENT SESSION cstore_XE_RowGroupReading
        ON DATABASE;
END

CREATE EVENT SESSION [cstore_XE_RowGroupReading] ON DATABASE 
	ADD EVENT sqlserver.column_store_rowgroup_read_issued(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.column_store_rowgroup_readahead_issued(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.sql_text,sqlserver.username))
	ADD TARGET package0.ring_buffer(SET max_memory=(51200))
	WITH (MAX_MEMORY=51200 KB);

GO



