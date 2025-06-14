-- 1. Создание таблиц БД
-----------------------------------------------------------------------------

-- Таблица Clients для хранения данных о клиентах
CREATE TABLE Clients (
    ClientID INT IDENTITY(1,1) PRIMARY KEY,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email NVARCHAR(100) UNIQUE NOT NULL,
    Phone NVARCHAR(20),
    CreditScore INT NOT NULL,
    CONSTRAINT CHK_CreditScore CHECK (CreditScore BETWEEN 300 AND 850)
);

-- Таблица LoanApplications для хранения данных о заявках
CREATE TABLE LoanApplications (
    ApplicationID INT IDENTITY(1,1) PRIMARY KEY,
    ClientID INT NOT NULL,
    ApplicationDate DATETIME DEFAULT GETDATE(),
    Status NVARCHAR(20) NOT NULL DEFAULT 'Submitted',
    LoanAmount DECIMAL(18,2) NOT NULL,
    LoanTerm INT NOT NULL,
    Purpose NVARCHAR(100) NOT NULL,
    CONSTRAINT FK_Client FOREIGN KEY (ClientID) REFERENCES Clients(ClientID),
    CONSTRAINT CHK_Status CHECK (Status IN ('Submitted', 'Processing', 'Approved', 'Rejected')),
    CONSTRAINT CHK_LoanAmount CHECK (LoanAmount > 0),
    CONSTRAINT CHK_LoanTerm CHECK (LoanTerm > 0)
);

-- Таблица для логов системы
CREATE TABLE ApplicationLogs (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    ApplicationID INT NULL,
    LogDate DATETIME DEFAULT GETDATE() NOT NULL,
    LogType NVARCHAR(50) NOT NULL,
    LogMessage NVARCHAR(MAX) NOT NULL,
    Details NVARCHAR(MAX) NULL
);

-- Таблица для хранения истории изменений статусов
CREATE TABLE ApplicationStatusHistory (
    HistoryID INT IDENTITY(1,1) PRIMARY KEY,
    ApplicationID INT NOT NULL,
    OldStatus NVARCHAR(20) NULL,
    NewStatus NVARCHAR(20) NOT NULL,
    ChangeDate DATETIME DEFAULT GETDATE(),
    ChangeReason NVARCHAR(255) NULL,
    ChangedBy NVARCHAR(128) NULL, -- Кто изменил статус
    CONSTRAINT FK_Application FOREIGN KEY (ApplicationID) REFERENCES LoanApplications(ApplicationID)
);

-- Новая таблица для журнала аудита
CREATE TABLE AuditLog (
    AuditID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(128) NOT NULL, -- Имя изменяемой таблицы
    RecordID INT NOT NULL, -- ID изменяемой записи
    ActionType NVARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    ActionDate DATETIME DEFAULT GETDATE() NOT NULL,
    UserName NVARCHAR(128) NULL, -- Пользователь, выполнивший действие
    OldValues NVARCHAR(MAX) NULL, -- Старые значения в JSON
    NewValues NVARCHAR(MAX) NULL, -- Новые значения в JSON
    IPAddress NVARCHAR(45) NULL -- IP-адрес
);

-- Триггер для защиты журнала аудита
CREATE TRIGGER tr_PreventAuditLogChanges
ON AuditLog
INSTEAD OF DELETE, UPDATE
AS
BEGIN
    RAISERROR('Audit log entries cannot be modified or deleted.', 16, 1);
    ROLLBACK TRANSACTION;
END;
GO

-- 2. Настройка Service Broker
-----------------------------------------------------------------------------

-- Включение Service Broker
IF (SELECT is_broker_enabled FROM sys.databases WHERE name = DB_NAME()) = 0
BEGIN
    ALTER DATABASE CURRENT SET ENABLE_BROKER;
END

-- Создание типов сообщений
IF NOT EXISTS (SELECT * FROM sys.service_message_types WHERE name = 'LoanApplicationMessage')
BEGIN
    CREATE MESSAGE TYPE [LoanApplicationMessage] VALIDATION = WELL_FORMED_XML;
END

-- Создание контракта
IF NOT EXISTS (SELECT * FROM sys.service_contracts WHERE name = 'LoanApplicationContract')
BEGIN
    CREATE CONTRACT [LoanApplicationContract]
    (
        [LoanApplicationMessage] SENT BY INITIATOR
    );
END

-- Создание очереди для обработки заявок
IF NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = 'LoanApplicationQueue')
BEGIN
    CREATE QUEUE LoanApplicationQueue
    WITH STATUS = ON,
    RETENTION = OFF,
    ACTIVATION (
        STATUS = ON,
        PROCEDURE_NAME = ProcessLoanApplication,
        MAX_QUEUE_READERS = 5,
        EXECUTE AS OWNER
    );
END

-- Создание службы
IF NOT EXISTS (SELECT * FROM sys.services WHERE name = 'LoanApplicationService')
BEGIN
    CREATE SERVICE [LoanApplicationService] 
    ON QUEUE LoanApplicationQueue ([LoanApplicationContract]);
END

-- 3. Обновленная процедура для логирования изменений статуса
-----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE LogStatusChange
    @ApplicationID INT,
    @OldStatus NVARCHAR(20),
    @NewStatus NVARCHAR(20),
    @ChangeReason NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @UserName NVARCHAR(128) = SYSTEM_USER;
    
    INSERT INTO ApplicationStatusHistory (
        ApplicationID, OldStatus, NewStatus, 
        ChangeReason, ChangedBy
    )
    VALUES (
        @ApplicationID, @OldStatus, @NewStatus, 
        @ChangeReason, @UserName
    );
    
    INSERT INTO ApplicationLogs (
        ApplicationID, LogType, LogMessage, Details
    )
    VALUES (
        @ApplicationID, 
        'StatusChange', 
        'Status changed from ' + ISNULL(@OldStatus, 'NULL') + ' to ' + @NewStatus,
        'Changed by: ' + @UserName + '. Reason: ' + ISNULL(@ChangeReason, 'Not specified')
    );
    
    -- Также логируем в аудит
    INSERT INTO AuditLog (
        TableName, RecordID, ActionType, UserName,
        OldValues, NewValues
    )
    VALUES (
        'LoanApplications', 
        @ApplicationID, 
        'UPDATE', 
        @UserName,
        JSON_QUERY('{"Status":"' + ISNULL(@OldStatus, 'NULL') + '"}'),
        JSON_QUERY('{"Status":"' + @NewStatus + '"}')
    );
END;
GO

-- 3. Хранимая процедура для подачи заявки
-----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE SubmitLoanApplication
    @FirstName NVARCHAR(50),
    @LastName NVARCHAR(50),
    @Email NVARCHAR(100),
    @Phone NVARCHAR(20),
    @CreditScore INT,
    @LoanDetails NVARCHAR(MAX), 
    @ApplicationID INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ClientID INT;
    DECLARE @ConversationHandle UNIQUEIDENTIFIER;
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @LoanAmount DECIMAL(18,2);
    DECLARE @LoanTerm INT;
    DECLARE @Purpose NVARCHAR(100);
    BEGIN TRY
        BEGIN TRY
            SET @LoanAmount = JSON_VALUE(@LoanDetails, '$.loanAmount');
            SET @LoanTerm = JSON_VALUE(@LoanDetails, '$.loanTerm');
            SET @Purpose = JSON_VALUE(@LoanDetails, '$.purpose');
        END TRY
        BEGIN CATCH
            INSERT INTO ApplicationLogs (LogType, LogMessage, Details)
            VALUES ('Validation', 'Invalid JSON format', @LoanDetails);  
            RAISERROR('Invalid JSON format for loan details.', 16, 1);
            RETURN;
        END CATCH
        BEGIN TRANSACTION;

        -- Валидация данных
        IF @CreditScore < 300 OR @CreditScore > 850
        BEGIN
            INSERT INTO ApplicationLogs (LogType, LogMessage, Details)
            VALUES ('Validation', 'Invalid credit score', 
                   'CreditScore: ' + CAST(@CreditScore AS NVARCHAR(10)));  
            RAISERROR('Invalid credit score. Must be between 300 and 850.', 16, 1);
            RETURN;
        END           
        IF @LoanAmount <= 0 OR @LoanTerm <= 0
        BEGIN
            INSERT INTO ApplicationLogs (LogType, LogMessage, Details)
            VALUES ('Validation', 'Invalid loan parameters', 
                   'Amount: ' + CAST(@LoanAmount AS NVARCHAR(20)) + 
                   ', Term: ' + CAST(@LoanTerm AS NVARCHAR(10)));
            RAISERROR('Loan amount and term must be positive values.', 16, 1);
            RETURN;
        END   

        -- Обновление или добавление клиента
        MERGE INTO Clients AS target
        USING (SELECT @Email AS Email) AS source
        ON target.Email = source.Email
        WHEN MATCHED THEN
            UPDATE SET 
                FirstName = @FirstName,
                LastName = @LastName,
                Phone = @Phone,
                CreditScore = @CreditScore
        WHEN NOT MATCHED THEN
            INSERT (FirstName, LastName, Email, Phone, CreditScore)
            VALUES (@FirstName, @LastName, @Email, @Phone, @CreditScore);        
        SET @ClientID = SCOPE_IDENTITY();
        IF @ClientID IS NULL
            SELECT @ClientID = ClientID FROM Clients WHERE Email = @Email;   
        INSERT INTO ApplicationLogs (LogType, LogMessage, Details)
        VALUES ('Client', 'Client data processed', 
               'ClientID: ' + CAST(@ClientID AS NVARCHAR(10)) + 
               ', Email: ' + @Email);               

        -- Добавление заявки 
        INSERT INTO LoanApplications (ClientID, LoanAmount, LoanTerm, Purpose)
        VALUES (@ClientID, @LoanAmount, @LoanTerm, @Purpose);    
        SET @ApplicationID = SCOPE_IDENTITY();
        INSERT INTO ApplicationLogs (ApplicationID, LogType, LogMessage, Details)
        VALUES (@ApplicationID, 'Application', 'New application submitted', 
               'LoanAmount: ' + CAST(@LoanAmount AS NVARCHAR(20)) + 
               ', Term: ' + CAST(@LoanTerm AS NVARCHAR(10)));  
			   
        -- Отправка сообщения в Service Broker
        BEGIN DIALOG @ConversationHandle
        FROM SERVICE [LoanApplicationService]
        TO SERVICE 'LoanApplicationService'
        ON CONTRACT [LoanApplicationContract]
        WITH ENCRYPTION = OFF;
        SEND ON CONVERSATION @ConversationHandle
        MESSAGE TYPE [LoanApplicationMessage]
        ('<ApplicationID>' + CAST(@ApplicationID AS NVARCHAR(10)) + '</ApplicationID>');     
        INSERT INTO ApplicationLogs (ApplicationID, LogType, LogMessage)
        VALUES (@ApplicationID, 'ServiceBroker', 'Message sent to Service Broker');       
        COMMIT TRANSACTION;
        SELECT @ApplicationID AS ApplicationID, 'Submitted' AS Status;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        SET @ErrorMessage = ERROR_MESSAGE();
        INSERT INTO ApplicationLogs (LogType, LogMessage, Details)
        VALUES ('Error', 'Error submitting application', @ErrorMessage);
        THROW;
    END CATCH;
END;
GO

-- 4. Хранимая процедура для обработки заявок (активируется Service Broker)
-----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE ProcessLoanApplication
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ConversationHandle UNIQUEIDENTIFIER;
    DECLARE @MessageBody XML;
    DECLARE @MessageTypeName NVARCHAR(256);
    DECLARE @ApplicationID INT;
    DECLARE @LoanAmount DECIMAL(18,2);
    DECLARE @LoanTerm INT;
    DECLARE @CreditScore INT;
    DECLARE @Status NVARCHAR(20);
    DECLARE @ClientID INT;
    DECLARE @OldStatus NVARCHAR(20);
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @ChangeReason NVARCHAR(255);    
    BEGIN TRY
        WHILE 1=1
        BEGIN
            BEGIN TRANSACTION; 
            WAITFOR (
                RECEIVE TOP(1)
                    @ConversationHandle = conversation_handle,
                    @MessageBody = message_body,
                    @MessageTypeName = message_type_name
                FROM LoanApplicationQueue
            ), TIMEOUT 1000;               
            IF @@ROWCOUNT = 0
            BEGIN
                COMMIT TRANSACTION;
                BREAK;
            END           
            INSERT INTO ApplicationLogs (ApplicationID, LogType, LogMessage)
            VALUES (NULL, 'Info', 'Message received from Service Broker');           
            IF @MessageTypeName = 'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
            BEGIN
                INSERT INTO ApplicationLogs (LogType, LogMessage)
                VALUES ('Info', 'EndDialog message received');
                END CONVERSATION @ConversationHandle;
            END
            ELSE IF @MessageTypeName = 'LoanApplicationMessage'
            BEGIN
                BEGIN TRY
                    SET @ApplicationID = @MessageBody.value('(/ApplicationID)[1]', 'INT');                         
                    INSERT INTO ApplicationLogs (ApplicationID, LogType, LogMessage)
                    VALUES (@ApplicationID, 'Info', 'Start processing application');                  
                    SELECT 
                        @LoanAmount = LoanAmount,
                        @LoanTerm = LoanTerm,
                        @ClientID = ClientID,
                        @OldStatus = Status
                    FROM LoanApplications
                    WHERE ApplicationID = @ApplicationID;                  
                    SELECT @CreditScore = CreditScore
                    FROM Clients
                    WHERE ClientID = @ClientID;                    
                    UPDATE LoanApplications
                    SET Status = 'Processing'
                    WHERE ApplicationID = @ApplicationID;             
                    EXEC LogStatusChange 
                        @ApplicationID = @ApplicationID,
                        @OldStatus = @OldStatus,
                        @NewStatus = 'Processing',
                        @ChangeReason = 'Started processing';                    
                    DECLARE @ProcessingDetails NVARCHAR(500) = 
                        'LoanAmount: ' + CAST(@LoanAmount AS NVARCHAR(20)) + 
                        ', Term: ' + CAST(@LoanTerm AS NVARCHAR(10)) + 
                        ', CreditScore: ' + CAST(@CreditScore AS NVARCHAR(10));
                    
                    INSERT INTO ApplicationLogs (ApplicationID, LogType, LogMessage, Details)
                    VALUES (@ApplicationID, 'Info', 'Application processing', @ProcessingDetails);
                    
                    IF @CreditScore > 600 AND @LoanAmount < 10000
                    BEGIN
                        SET @Status = 'Approved';
                    END
                    ELSE
                    BEGIN
                        SET @Status = 'Rejected';
                    END                   
                    UPDATE LoanApplications
                    SET Status = @Status
                    WHERE ApplicationID = @ApplicationID;                   
                    EXEC LogStatusChange 
                        @ApplicationID = @ApplicationID,
                        @OldStatus = 'Processing',
                        @NewStatus = @Status,
                        @ChangeReason = 'Automated decision';                 
                    INSERT INTO ApplicationLogs (ApplicationID, LogType, LogMessage)
                    VALUES (@ApplicationID, 'Info', 'Application processed. Status: ' + @Status);    
                END TRY
                BEGIN CATCH
                    SET @ErrorMessage = ERROR_MESSAGE();
                    SET @ErrorSeverity = ERROR_SEVERITY();
                    SET @ErrorState = ERROR_STATE();          
                    INSERT INTO ApplicationLogs (ApplicationID, LogType, LogMessage, Details)
                    VALUES (@ApplicationID, 'Error', 'Error processing application', @ErrorMessage);
                    
                    UPDATE LoanApplications
                    SET Status = 'Rejected'
                    WHERE ApplicationID = @ApplicationID;
                    SET @ChangeReason = 'Error during processing: ' + @ErrorMessage;                 
                    EXEC LogStatusChange 
                        @ApplicationID = @ApplicationID,
                        @OldStatus = 'Processing',
                        @NewStatus = 'Rejected',
                        @ChangeReason = @ChangeReason;
                END CATCH   
                END CONVERSATION @ConversationHandle;
            END
            ELSE
            BEGIN
                INSERT INTO ApplicationLogs (LogType, LogMessage, Details)
                VALUES ('Warning', 'Unknown message type received', @MessageTypeName); 
                END CONVERSATION @ConversationHandle;
            END
            COMMIT TRANSACTION;
        END
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @ErrorSeverity = ERROR_SEVERITY();
        SET @ErrorState = ERROR_STATE();
        INSERT INTO ApplicationLogs (LogType, LogMessage, Details)
        VALUES ('Error', 'Critical error in ProcessLoanApplication', @ErrorMessage);
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;
GO


-- 5. Триггеры для аудита изменений в таблицах
-----------------------------------------------------------------------------

-- Триггер для аудита таблицы Clients
CREATE OR ALTER TRIGGER tr_AuditClients
ON Clients
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ActionType NVARCHAR(20);
    DECLARE @UserName NVARCHAR(128) = SYSTEM_USER;
    
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        SET @ActionType = 'UPDATE';
    ELSE IF EXISTS (SELECT * FROM inserted)
        SET @ActionType = 'INSERT';
    ELSE
        SET @ActionType = 'DELETE';
    
    -- Для операций UPDATE и DELETE сохраняем старые значения
    IF @ActionType IN ('UPDATE', 'DELETE')
    BEGIN
        INSERT INTO AuditLog (
            TableName, RecordID, ActionType, UserName,
            OldValues, NewValues
        )
        SELECT 
            'Clients', 
            d.ClientID, 
            @ActionType, 
            @UserName,
            (SELECT * FROM deleted FOR JSON AUTO),
            CASE WHEN @ActionType = 'UPDATE' THEN (SELECT * FROM inserted FOR JSON AUTO) ELSE NULL END
        FROM deleted d;
    END
    
    -- Для INSERT сохраняем только новые значения
    IF @ActionType = 'INSERT'
    BEGIN
        INSERT INTO AuditLog (
            TableName, RecordID, ActionType, UserName,
            NewValues
        )
        SELECT 
            'Clients', 
            i.ClientID, 
            @ActionType, 
            @UserName,
            (SELECT * FROM inserted FOR JSON AUTO)
        FROM inserted i;
    END
END;
GO

-- Триггер для аудита таблицы LoanApplications
CREATE OR ALTER TRIGGER tr_AuditLoanApplications
ON LoanApplications
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ActionType NVARCHAR(20);
    DECLARE @UserName NVARCHAR(128) = SYSTEM_USER;
    
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        SET @ActionType = 'UPDATE';
    ELSE IF EXISTS (SELECT * FROM inserted)
        SET @ActionType = 'INSERT';
    ELSE
        SET @ActionType = 'DELETE';
    
    -- Для операций UPDATE и DELETE сохраняем старые значения
    IF @ActionType IN ('UPDATE', 'DELETE')
    BEGIN
        INSERT INTO AuditLog (
            TableName, RecordID, ActionType, UserName,
            OldValues, NewValues
        )
        SELECT 
            'LoanApplications', 
            d.ApplicationID, 
            @ActionType, 
            @UserName,
            (SELECT * FROM deleted FOR JSON AUTO),
            CASE WHEN @ActionType = 'UPDATE' THEN (SELECT * FROM inserted FOR JSON AUTO) ELSE NULL END
        FROM deleted d;
    END
    
    -- Для INSERT сохраняем только новые значения
    IF @ActionType = 'INSERT'
    BEGIN
        INSERT INTO AuditLog (
            TableName, RecordID, ActionType, UserName,
            NewValues
        )
        SELECT 
            'LoanApplications', 
            i.ApplicationID, 
            @ActionType, 
            @UserName,
            (SELECT * FROM inserted FOR JSON AUTO)
        FROM inserted i;
    END
END;
GO

-----------------------------------------------------------------------------
-- Пример использования
DECLARE @AppID INT;

EXEC SubmitLoanApplication
    @FirstName = 'Иван',
    @LastName = 'Иванов',
    @Email = 'ivan@example.com',
    @Phone = '+79161234567',
    @CreditScore = 750,
    @LoanDetails = '{"loanAmount": 5000, "loanTerm": 12, "purpose": "Потребительский кредит"}',
    @ApplicationID = @AppID OUTPUT;

-- Просмотр логов аудита
SELECT * FROM AuditLog ORDER BY ActionDate DESC;
SELECT * FROM ApplicationStatusHistory ORDER BY ChangeDate DESC;
SELECT * FROM ApplicationLogs ORDER BY LogDate DESC;