### Test list
 - [] There are two instances of OwnSpace\
and both are performing insert into same table\
then when getting all data from this table\
there should be only one record
 - [] There is a unique constraint on some field\
and two spaces are writing data with same field value\
then both operations should succeed
    - Here I should check constraints on single column
    - constrain spanning multiple columns
 - [] Test checking that table/column name matching some keyword does not cause errors


### Test cases for dealing with foreign keys and unique constraints/unique indexes

```sql
CREATE TABLE TableA(
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Code INT NOT NULL    
);
CREATE UNIQUE INDEX IX_Code ON TableA(Code);

CREATE TABLE TableB(
    Id INT IDENTITY(1,1) PRIMARY KEY,
    TabelACode INT NOT NULL
    CONSTRAINT FK_TableB_TableA_TabelACode FOREIGN KEY(TabelACode) REFERENCES TableA(Code)
);

EXEC sp_fkeys @pktable_name='TableA', @pktable_owner = 'dbo'

DROP INDEX IX_Code ON TableA;


CREATE DATABASE Explore2;
CREATE TABLE TableA(
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Code INT NOT NULL,
    SpaceId UNIQUEIDENTIFIER NOT NULL,
    CONSTRAINT AK_Code UNIQUE (Code, SpaceId)
);

CREATE TABLE TableB(
    Id INT IDENTITY(1,1) PRIMARY KEY,
    TabelACode INT NOT NULL,
    SpaceId UNIQUEIDENTIFIER NOT NULL,
    CONSTRAINT FK_TableB_TableA_TabelACode FOREIGN KEY(TabelACode, SpaceId) REFERENCES TableA(Code, SpaceId)
);

CREATE DATABASE Explore3;
CREATE TABLE TableA(
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Code INT NOT NULL,
    SpaceId UNIQUEIDENTIFIER NOT NULL,
    CONSTRAINT AK_Code UNIQUE (Code)
);

CREATE TABLE TableB(
    Id INT IDENTITY(1,1) PRIMARY KEY,
    TabelACode INT NOT NULL,
    SpaceId UNIQUEIDENTIFIER NOT NULL,
    CONSTRAINT FK_TableB_TableA_TabelACode FOREIGN KEY(TabelACode) REFERENCES TableA(Code)
);

ALTER TABLE TableB
DROP CONSTRAINT FK_TableB_TableA_TabelACode

ALTER TABLE TableA
DROP CONSTRAINT AK_Code

ALTER TABLE TableA
ADD CONSTRAINT AK_Code UNIQUE (Code, SpaceId);

ALTER TABLE TableB
ADD CONSTRAINT FK_TableB_TableA_TabelACode FOREIGN KEY(TabelACode, SpaceId) REFERENCES TableA(Code, SpaceId);
```