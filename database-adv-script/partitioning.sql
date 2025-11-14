-- ====================================================
-- SQL SERVER TABLE PARTITIONING STRATEGY FOR AIRBNB BOOKINGS
-- Native SQL Server partitioning implementation
-- ====================================================

-- SECTION 1: CURRENT TABLE ANALYSIS AND SETUP
-- Analyze existing data and prepare for partitioning

-- Check if main bookings table exists, if not create it
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'bookings_main')
BEGIN
    CREATE TABLE bookings_main (
        booking_id INT IDENTITY(1,1) PRIMARY KEY,
        guest_id INT NOT NULL,
        property_id INT NOT NULL,
        check_in DATE NOT NULL,
        check_out DATE NOT NULL,
        total_amount DECIMAL(10,2) NOT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE()
    );
END

-- Create index on check_in date for partitioning key
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_bookings_main_check_in')
BEGIN
    CREATE INDEX idx_bookings_main_check_in ON bookings_main(check_in);
END

-- Analyze data distribution by year for partitioning strategy
SELECT 
    YEAR(check_in) as booking_year,
    COUNT(*) as booking_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bookings_main), 2) as percentage
FROM bookings_main
GROUP BY YEAR(check_in)
ORDER BY booking_year;

-- SECTION 2: CREATE PARTITION FUNCTION AND SCHEME
-- Set up native SQL Server partitioning

-- Create partition function by year
IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'pf_booking_years')
BEGIN
    CREATE PARTITION FUNCTION pf_booking_years (DATE)
    AS RANGE RIGHT FOR VALUES (
        '2020-01-01',
        '2021-01-01', 
        '2022-01-01',
        '2023-01-01',
        '2024-01-01',
        '2025-01-01'
    );
END

-- Create partition scheme
IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'ps_booking_years')
BEGIN
    CREATE PARTITION SCHEME ps_booking_years
    AS PARTITION pf_booking_years
    ALL TO ([PRIMARY]);
END

-- SECTION 3: CREATE PARTITIONED TABLE
-- Create main partitioned table

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'bookings_partitioned')
BEGIN
    CREATE TABLE bookings_partitioned (
        booking_id INT IDENTITY(1,1),
        guest_id INT NOT NULL,
        property_id INT NOT NULL,
        check_in DATE NOT NULL,
        check_out DATE NOT NULL,
        total_amount DECIMAL(10,2) NOT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE(),
        CONSTRAINT pk_bookings_partitioned PRIMARY KEY (booking_id, check_in)
    ) ON ps_booking_years(check_in);
END

-- SECTION 4: CREATE PARTITION ALIGNED INDEXES
-- Optimize indexes for partitioned table

-- Index on check_in (partition key)
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_bookings_part_check_in')
BEGIN
    CREATE INDEX idx_bookings_part_check_in ON bookings_partitioned(check_in)
    ON ps_booking_years(check_in);
END

-- Index on guest_id
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_bookings_part_guest_id')
BEGIN
    CREATE INDEX idx_bookings_part_guest_id ON bookings_partitioned(guest_id)
    ON ps_booking_years(check_in);
END

-- Index on property_id
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_bookings_part_property_id')
BEGIN
    CREATE INDEX idx_bookings_part_property_id ON bookings_partitioned(property_id)
    ON ps_booking_years(check_in);
END

-- Index on status
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_bookings_part_status')
BEGIN
    CREATE INDEX idx_bookings_part_status ON bookings_partitioned(status)
    ON ps_booking_years(check_in);
END

-- SECTION 5: DATA MIGRATION TO PARTITIONED TABLE
-- Migrate existing data to partitioned table

-- Disable constraints for faster load
ALTER TABLE bookings_partitioned NOCHECK CONSTRAINT ALL;

-- Migrate data with explicit partition mapping
INSERT INTO bookings_partitioned (guest_id, property_id, check_in, check_out, total_amount, status, created_at, updated_at)
SELECT guest_id, property_id, check_in, check_out, total_amount, status, created_at, updated_at
FROM bookings_main;

-- Re-enable constraints
ALTER TABLE bookings_partitioned CHECK CONSTRAINT ALL;

-- SECTION 6: CREATE PARTITION MANAGEMENT VIEWS
-- Views to help manage and monitor partitions

-- View to show partition information
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_partition_info')
    DROP VIEW v_partition_info;
GO

CREATE VIEW v_partition_info
AS
SELECT 
    p.partition_number,
    f.name AS function_name,
    r.boundary_id,
    r.value AS boundary_value,
    p.rows AS row_count
FROM sys.partitions p
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
JOIN sys.partition_functions f ON ps.function_id = f.function_id
LEFT JOIN sys.partition_range_values r ON f.function_id = r.function_id AND r.boundary_id = p.partition_number
WHERE p.object_id = OBJECT_ID('bookings_partitioned')
    AND i.type IN (0, 1)  -- Heap or clustered index
ORDER BY p.partition_number;
GO

-- View for partition-specific queries
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_bookings_by_year')
    DROP VIEW v_bookings_by_year;
GO

CREATE VIEW v_bookings_by_year
AS
SELECT 
    booking_id,
    guest_id,
    property_id,
    check_in,
    check_out,
    total_amount,
    status,
    YEAR(check_in) AS booking_year,
    $PARTITION.pf_booking_years(check_in) AS partition_number
FROM bookings_partitioned;
GO

-- SECTION 7: PARTITION MAINTENANCE UTILITIES
-- Stored procedures for partition management

-- Procedure to get partition details
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_get_partition_details')
    DROP PROCEDURE sp_get_partition_details;
GO

CREATE PROCEDURE sp_get_partition_details
AS
BEGIN
    -- Show partition distribution
    SELECT 
        partition_number,
        boundary_value,
        row_count,
        CASE 
            WHEN boundary_value IS NULL THEN 'Future partitions'
            ELSE CONVERT(VARCHAR(10), boundary_value, 120)
        END AS partition_range
    FROM v_partition_info
    ORDER BY partition_number;
    
    -- Show data distribution by year
    SELECT 
        YEAR(check_in) AS booking_year,
        COUNT(*) AS booking_count,
        $PARTITION.pf_booking_years(check_in) AS partition_number
    FROM bookings_partitioned
    GROUP BY YEAR(check_in), $PARTITION.pf_booking_years(check_in)
    ORDER BY booking_year;
END
GO

-- Procedure to switch in new partition
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_create_new_partition')
    DROP PROCEDURE sp_create_new_partition;
GO

CREATE PROCEDURE sp_create_new_partition
    @new_boundary_date DATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Split the partition function to create new partition
    DECLARE @split_sql NVARCHAR(1000);
    SET @split_sql = 'ALTER PARTITION SCHEME ps_booking_years NEXT USED [PRIMARY];';
    EXEC sp_executesql @split_sql;
    
    SET @split_sql = 'ALTER PARTITION FUNCTION pf_booking_years() SPLIT RANGE (''' + 
                     CONVERT(VARCHAR(10), @new_boundary_date, 120) + ''');';
    EXEC sp_executesql @split_sql;
    
    PRINT 'New partition created for boundary: ' + CONVERT(VARCHAR(10), @new_boundary_date, 120);
END
GO

-- SECTION 8: QUERY PERFORMANCE TESTING
-- Test partition-aware queries

-- Test 1: Query against specific partition (partition elimination)
SELECT 'Test 1 - Partition Elimination' as test_description;
SELECT 
    booking_id,
    check_in,
    total_amount
FROM bookings_partitioned
WHERE check_in >= '2024-01-01' AND check_in < '2025-01-01'
    AND status = 'confirmed'
ORDER BY check_in DESC;

-- Check if partition elimination occurred
SELECT 'Partition Elimination Check' as check_type;
EXEC sp_get_partition_details;

-- Test 2: Cross-partition query
SELECT 'Test 2 - Cross Partition Query' as test_description;
SELECT 
    YEAR(check_in) AS booking_year,
    COUNT(*) AS booking_count,
    SUM(total_amount) AS total_revenue
FROM bookings_partitioned
WHERE check_in >= '2020-01-01' 
    AND status = 'confirmed'
GROUP BY YEAR(check_in)
ORDER BY booking_year;

-- Test 3: Complex join with partition elimination
SELECT 'Test 3 - Complex Join with Partitioning' as test_description;
SELECT 
    b.booking_id,
    b.check_in,
    b.total_amount,
    g.first_name AS guest_name,
    p.title AS property_title
FROM bookings_partitioned b
INNER JOIN users g ON b.guest_id = g.user_id
INNER JOIN properties p ON b.property_id = p.property_id
WHERE b.check_in >= '2024-01-01' AND b.check_in < '2025-01-01'
    AND b.status = 'confirmed'
ORDER BY b.check_in DESC;

-- SECTION 9: PARTITION MAINTENANCE AND MONITORING
-- Regular maintenance tasks

-- Update partition statistics
UPDATE STATISTICS bookings_partitioned WITH FULLSCAN;

-- Check partition health
DBCC SHOW_STATISTICS ('bookings_partitioned', 'idx_bookings_part_check_in');

-- Monitor partition usage
SELECT 
    OBJECT_NAME(p.object_id) AS table_name,
    p.partition_number,
    p.rows,
    au.total_pages,
    au.used_pages
FROM sys.partitions p
JOIN sys.allocation_units au ON p.hobt_id = au.container_id
WHERE OBJECT_NAME(p.object_id) = 'bookings_partitioned'
ORDER BY p.partition_number;

-- SECTION 10: ARCHIVING STRATEGY
-- Archive old partitions

-- Create archive table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'bookings_archive')
BEGIN
    CREATE TABLE bookings_archive (
        booking_id INT,
        guest_id INT NOT NULL,
        property_id INT NOT NULL,
        check_in DATE NOT NULL,
        check_out DATE NOT NULL,
        total_amount DECIMAL(10,2) NOT NULL,
        status VARCHAR(20) NOT NULL,
        created_at DATETIME2,
        updated_at DATETIME2,
        archived_at DATETIME2 DEFAULT GETDATE()
    );
END

-- Archive old data (example: archive 2020 data)
INSERT INTO bookings_archive (booking_id, guest_id, property_id, check_in, check_out, total_amount, status, created_at, updated_at)
SELECT booking_id, guest_id, property_id, check_in, check_out, total_amount, status, created_at, updated_at
FROM bookings_partitioned
WHERE check_in < '2021-01-01';

-- Switch out old partition (after archiving)
-- Note: This requires careful planning and testing
/*
ALTER TABLE bookings_partitioned 
SWITCH PARTITION 1 TO bookings_archive;
*/

-- SECTION 11: PERFORMANCE COMPARISON
-- Compare partitioned vs non-partitioned performance

-- Partitioned table query
SELECT 'Partitioned Table' as table_type, COUNT(*) as row_count
FROM bookings_partitioned 
WHERE check_in >= '2024-01-01' AND check_in < '2025-01-01'
    AND status = 'confirmed';

-- Non-partitioned table query
SELECT 'Non-Partitioned Table' as table_type, COUNT(*) as row_count
FROM bookings_main 
WHERE check_in >= '2024-01-01' AND check_in < '2025-01-01'
    AND status = 'confirmed';

-- SECTION 12: APPLICATION INTEGRATION
-- How applications should use partitioned tables

-- Example: Insert new booking
/*
INSERT INTO bookings_partitioned (guest_id, property_id, check_in, check_out, total_amount, status)
VALUES (123, 456, '2024-06-15', '2024-06-20', 750.00, 'confirmed');
*/

-- Example: Query recent bookings with partition elimination
/*
SELECT * FROM bookings_partitioned 
WHERE check_in >= '2024-01-01' AND check_in < '2025-01-01'
AND guest_id = 123
ORDER BY check_in DESC;
*/

-- Example: Cross-partition report
/*
SELECT 
    YEAR(check_in) as year,
    COUNT(*) as bookings,
    SUM(total_amount) as revenue
FROM bookings_partitioned
WHERE check_in >= '2020-01-01'
GROUP BY YEAR(check_in)
ORDER BY year;
*/

-- SECTION 13: FINAL VERIFICATION AND CLEANUP
-- Verify partitioning setup

-- Execute partition details procedure
EXEC sp_get_partition_details;

-- Verify data integrity
SELECT 
    'Data Integrity Check' as check_type,
    (SELECT COUNT(*) FROM bookings_partitioned) as partitioned_count,
    (SELECT COUNT(*) FROM bookings_main) as main_count;

-- Display setup completion message
SELECT 'SQL Server partitioning setup complete' as status;
SELECT 
    COUNT(*) as total_partitioned_bookings,
    MIN(check_in) as earliest_booking,
    MAX(check_in) as latest_booking
FROM bookings_partitioned;