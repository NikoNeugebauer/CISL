set xact_abort on;

begin try
	exec dbo.cstore_GetAlignment;
	exec dbo.cstore_GetDictionaries;
	exec dbo.cstore_GetMemory;
	exec dbo.cstore_GetRowGroups;
	exec dbo.cstore_GetRowGroupsDetails;
	exec dbo.cstore_GetSQLInfo;
	exec dbo.cstore_SuggestedTables;
end try
begin catch
	Throw 
end catch

Print 'All Stored Procedure Installed Succesfully'