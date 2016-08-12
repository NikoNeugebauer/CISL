drop table dbo.TestSuggested01;

create table dbo.TestSuggested01(
	c1 int identity(1,1) not null
);


set nocount on;
declare @i as int;
declare @max as int;
select @max = isnull(max(C1),0) from dbo.TestSuggested01;
set @i = 1;

begin tran
while @i <= 500000
begin
	insert into dbo.TestSuggested01
		default values

	set @i+=1;
end;
commit;

-- 
create clustered index pk_TestSuggested01
	on dbo.TestSuggested01 (c1);

create nonclustered index ix_01_TestSuggested01
	on dbo.TestSuggested01 (c1);

create nonclustered index ix_02_TestSuggested01
	on dbo.TestSuggested01 (c1);

create nonclustered index ix_f01_TestSuggested01
	on dbo.TestSuggested01 (c1)
	where C1 < 100;


select count(*)
	from TestSuggested01

-- ******************************************************************************
create table dbo.TestSuggested02(
	c1 int identity(1,1) not null primary key clustered,
	c2 xml default ('<root>abc</root>')
);

set nocount on;
declare @i as int;
declare @max as int;
select @max = isnull(max(C1),0) from dbo.TestSuggested01;
set @i = 1;

begin tran
while @i <= 500000
begin
	insert into dbo.TestSuggested02
		default values

	set @i+=1;
end;
commit;

create primary xml index xmlix_01_TestSuggested02
	on dbo.TestSuggested02 (c2);

CREATE XML INDEX xmlix_02_TestSuggested02
    ON dbo.TestSuggested02 (c2)
    USING XML INDEX xmlix_01_TestSuggested02 FOR PATH ;

select count(*)
	from TestSuggested02