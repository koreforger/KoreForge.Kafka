SET NOCOUNT ON;

IF DB_ID('MultiAppSettings') IS NULL
BEGIN
    CREATE DATABASE MultiAppSettings;
END
GO

USE MultiAppSettings;
GO

IF OBJECT_ID('dbo.Settings', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Settings
    (
        ApplicationId NVARCHAR(100) NOT NULL,
        InstanceId NVARCHAR(100) NOT NULL,
        [Key] NVARCHAR(200) NOT NULL,
        [Value] NVARCHAR(MAX) NULL,
        CONSTRAINT PK_Settings PRIMARY KEY (ApplicationId, InstanceId, [Key])
    );
END
GO

MERGE dbo.Settings AS target
USING (VALUES
    ('Publisher', 'localhost', 'Publisher:ProfileName', 'SamplePublisher'),
    ('Publisher', 'localhost', 'Publisher:Topic', 'sample-messages'),
    ('Publisher', 'localhost', 'Publisher:TargetRecordsPerSecond', '1'),
    ('Publisher', 'localhost', 'Publisher:PayloadBytes', '120000'),
    ('ConsumerApi', 'localhost', 'Consumer:ProfileName', 'SampleConsumer'),
    ('ConsumerApi', 'localhost', 'Consumer:Topics:0', 'sample-messages')
) AS source (ApplicationId, InstanceId, [Key], [Value])
ON target.ApplicationId = source.ApplicationId
   AND target.InstanceId = source.InstanceId
   AND target.[Key] = source.[Key]
WHEN NOT MATCHED THEN
    INSERT (ApplicationId, InstanceId, [Key], [Value])
    VALUES (source.ApplicationId, source.InstanceId, source.[Key], source.[Value]);
GO
