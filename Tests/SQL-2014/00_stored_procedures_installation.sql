set xact_abort on;

begin try
	exec dbo.cstore_GetAlignment;
	exec dbo.cstore_GetDictionaries;
	exec dbo.cstore_GetFragmentation;
	exec dbo.cstore_GetMemory;
	exec dbo.cstore_GetRowGroups;
	exec dbo.cstore_GetRowGroupsDetails;
	exec dbo.cstore_GetSQLInfo;
	exec dbo.cstore_SuggestedTables;
	exec dbo.cstore_doMaintenance @execute = 1;
end try
begin catch
	Throw 
end catch

Print 'All Stored Procedure Installed Succesfully'