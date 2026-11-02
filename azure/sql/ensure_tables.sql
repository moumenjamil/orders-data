-- orders_daily on Azure (T4): the tables the pipeline writes, created once.
-- raw.orders keeps one slice per day (dt), replaced when that day runs again;
-- <target>.orders_daily is rebuilt from the run's day (dev, ci or prod).
IF SCHEMA_ID('raw') IS NULL EXEC('CREATE SCHEMA raw');
IF OBJECT_ID('raw.orders') IS NULL
    CREATE TABLE raw.orders (
        id          BIGINT,
        customer_id BIGINT,
        status      NVARCHAR(20),
        amount      DECIMAL(12, 2),
        payment_id  NVARCHAR(64),
        created_at  DATETIME2,
        updated_at  DATETIME2,
        dt          DATE,
        _loaded_at  DATETIME2
    );

DECLARE @s SYSNAME;
DECLARE targets CURSOR LOCAL FOR SELECT v FROM (VALUES ('dev'), ('ci'), ('prod')) t(v);
OPEN targets;
FETCH NEXT FROM targets INTO @s;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF SCHEMA_ID(@s) IS NULL EXEC('CREATE SCHEMA ' + @s);
    IF OBJECT_ID(@s + '.orders_daily') IS NULL
        EXEC('CREATE TABLE ' + @s + '.orders_daily (
            order_date            DATE,
            order_count           INT,
            paid_order_count      INT,
            total_amount          DECIMAL(14, 2),
            paid_amount           DECIMAL(14, 2),
            cancelled_order_count INT,
            cancelled_amount      DECIMAL(14, 2),
            avg_order_amount      DECIMAL(12, 2)
        )');
    -- the biggest order of the day (added after the table first existed)
    IF COL_LENGTH(@s + '.orders_daily', 'max_order_amount') IS NULL
        EXEC('ALTER TABLE ' + @s + '.orders_daily ADD max_order_amount DECIMAL(12, 2)');
    FETCH NEXT FROM targets INTO @s;
END
CLOSE targets;
DEALLOCATE targets;
