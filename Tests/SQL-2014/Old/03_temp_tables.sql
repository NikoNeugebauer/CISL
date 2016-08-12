use ContosoRetailDW;

CREATE TABLE #FactOnlineSalesTemp(
	[OnlineSalesKey] [int] NOT NULL,
	[DateKey] [datetime] NOT NULL,
	[StoreKey] [int] NOT NULL,
	[ProductKey] [int] NOT NULL,
	[PromotionKey] [int] NOT NULL,
	[CurrencyKey] [int] NOT NULL,
	[CustomerKey] [int] NOT NULL,
	[SalesOrderNumber] [nvarchar](20) NOT NULL,
	[SalesOrderLineNumber] [int] NULL,
	[SalesQuantity] [int] NOT NULL,
	[SalesAmount] [money] NOT NULL,
	[ReturnQuantity] [int] NOT NULL,
	[ReturnAmount] [money] NULL,
	[DiscountQuantity] [int] NULL,
	[DiscountAmount] [money] NULL,
	[TotalCost] [money] NOT NULL,
	[UnitCost] [money] NULL,
	[UnitPrice] [money] NULL,
	[ETLLoadID] [int] NULL,
	[LoadDate] [datetime] NULL,
	[UpdateDate] [datetime] NULL
);

create clustered columnstore index CCI_FactOnlineSalesTemp
    on #FactOnlineSalesTemp;

insert into #FactOnlineSalesTemp
select top 2000000 OnlineSalesKey, DateKey, StoreKey, ProductKey, PromotionKey, CurrencyKey, CustomerKey, SalesOrderNumber, SalesOrderLineNumber, SalesQuantity, SalesAmount, ReturnQuantity, ReturnAmount, DiscountQuantity, DiscountAmount, TotalCost, UnitCost, UnitPrice, ETLLoadID, LoadDate, UpdateDate
	from dbo.FactOnlineSales;


CREATE TABLE ##FactOnlineSalesGlobalTemp(
	[OnlineSalesKey] [int] NOT NULL,
	[DateKey] [datetime] NOT NULL,
	[StoreKey] [int] NOT NULL,
	[ProductKey] [int] NOT NULL,
	[PromotionKey] [int] NOT NULL,
	[CurrencyKey] [int] NOT NULL,
	[CustomerKey] [int] NOT NULL,
	[SalesOrderNumber] [nvarchar](20) NOT NULL,
	[SalesOrderLineNumber] [int] NULL,
	[SalesQuantity] [int] NOT NULL,
	[SalesAmount] [money] NOT NULL,
	[ReturnQuantity] [int] NOT NULL,
	[ReturnAmount] [money] NULL,
	[DiscountQuantity] [int] NULL,
	[DiscountAmount] [money] NULL,
	[TotalCost] [money] NOT NULL,
	[UnitCost] [money] NULL,
	[UnitPrice] [money] NULL,
	[ETLLoadID] [int] NULL,
	[LoadDate] [datetime] NULL,
	[UpdateDate] [datetime] NULL
);

create clustered columnstore index CCI_FactOnlineSalesGlobalTemp
    on ##FactOnlineSalesGlobalTemp;

insert into ##FactOnlineSalesGlobalTemp
select top 2000000 OnlineSalesKey, DateKey, StoreKey, ProductKey, PromotionKey, CurrencyKey, CustomerKey, SalesOrderNumber, SalesOrderLineNumber, SalesQuantity, SalesAmount, ReturnQuantity, ReturnAmount, DiscountQuantity, DiscountAmount, TotalCost, UnitCost, UnitPrice, ETLLoadID, LoadDate, UpdateDate
	from dbo.FactOnlineSales;

exec dbo.cstore_GetRowGRoups @tableName = 'Temp'
