drop table if exists #LocalTempTable;

create table #LocalTempTable(
	c1 int identity(1,1) not null,
	index PK_LocalTempTable clustered columnstore );

set nocount on;

declare @i as int;
declare @max as int;
select @max = isnull(max(C1),0) from #LocalTempTable;
set @i = 1;

begin tran
while @i <= 1048577
begin
	insert into #LocalTempTable
		default values

	set @i = @i + 1;
end;
commit;

alter index PK_LocalTempTable
	on dbo.#LocalTempTable
		reorganize;

---
