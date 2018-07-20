-- IMPORTANT: Swith to SQLCMD mode using Query/SQLCMD Mode in SSMS menu!

-- Set the path where *.sql files are placed
-- (e.g. the current folder where this file is placed)
:setvar path "C:\GitHub\CISL\Azure\StoredProcs\"

:r	$(path)cstore_GetRowGroups.sql
:r	$(path)cstore_GetRowGroupsDetails.sql
:r	$(path)cstore_GetAlignment.sql
:r	$(path)cstore_GetDictionaries.sql
:r	$(path)cstore_GetFragmentation.sql
:r	$(path)cstore_SuggestedTables.sql
:r	$(path)cstore_doMaintenance.sql

/*
To cleanup:
:setvar path "C:\GitHub\CISL\Azure\StoredProcs\"
:r $(path)cstore_cleanup.sql
*/
