CREATE TABLE INPUT
(
  ID   INT PRIMARY KEY         NOT NULL IDENTITY,
  PATH NVARCHAR(512),
  MODE VARCHAR(8) DEFAULT 'R'  NOT NULL
);
CREATE UNIQUE INDEX INPUT_ID_uindex
  ON INPUT (ID);

CREATE TABLE [FILE]
(
  ID         BIGINT PRIMARY KEY     NOT NULL IDENTITY,
  DIGEST     CHAR(40)               NOT NULL,
  STATUS     VARCHAR(16) DEFAULT '' NOT NULL,
  REGISTERED DATETIME
);
CREATE UNIQUE INDEX FILE_DIGEST_uindex
  ON [FILE] (DIGEST);

CREATE TABLE FILE_PATH
(
  FILE_ID BIGINT,
  PATH    NVARCHAR(MAX),
  NAME    NVARCHAR(128),
  DIGEST  VARCHAR(40),
  CONSTRAINT FILE_PATH_FILE_ID_fk FOREIGN KEY (FILE_ID) REFERENCES [FILE] (ID)
);
CREATE INDEX FILE_PATH_NAME_index
  ON FILE_PATH (NAME);
CREATE INDEX FILE_PATH_FILE_ID_index
  ON FILE_PATH (FILE_ID);
CREATE INDEX FILE_PATH_DIGEST_index
  ON FILE_PATH (DIGEST);

CREATE TABLE ISA
(
  ID       BIGINT PRIMARY KEY     NOT NULL IDENTITY,
  DIGEST   CHAR(40)               NOT NULL,
  DOC_TYPE VARCHAR(8),
  DATE8    VARCHAR(8),
  TIME8    VARCHAR(8),
  STATUS   VARCHAR(16) DEFAULT '' NOT NULL
);
CREATE UNIQUE INDEX ISA_ID_uindex
  ON ISA (ID);
CREATE UNIQUE INDEX ISA_DIGEST_uindex
  ON ISA (DIGEST);
CREATE INDEX ISA_ID_DIGEST_index
  ON ISA (ID, DIGEST);

CREATE TABLE R_ISA_FILE
(
  ISA_ID  BIGINT NOT NULL,
  FILE_ID BIGINT NOT NULL,
  CONSTRAINT R_ISA_FILE_FILE_ID_pk PRIMARY KEY (ISA_ID, FILE_ID),
  CONSTRAINT R_ISA_FILE_ISA_ID_fk FOREIGN KEY (ISA_ID) REFERENCES ISA (ID),
  CONSTRAINT R_ISA_FILE_FILE_ID_fk FOREIGN KEY (FILE_ID) REFERENCES [FILE] (ID)
);

CREATE TABLE SESSION
(
  ID       INT PRIMARY KEY NOT NULL IDENTITY,
  UUID     NVARCHAR(64),
  STARTED  DATETIME,
  FINISHED DATETIME
);
CREATE UNIQUE INDEX SESSION_UUID_uindex
  ON SESSION (UUID);

CREATE TABLE LOG
(
  ID          BIGINT PRIMARY KEY NOT NULL IDENTITY,
  LEVEL       INT DEFAULT 0      NOT NULL,
  SESSION_ID  INT                NOT NULL,
  FILE_ID     BIGINT,
  ISA_ID      BIGINT,
  MESSAGE     NVARCHAR(MAX),
  ACTION      NVARCHAR(16),
  DETAILS     NTEXT,
  DETAILS_XML XML,
  CONSTRAINT LOG_SESSION_ID_fk FOREIGN KEY (SESSION_ID) REFERENCES SESSION (ID),
  CONSTRAINT LOG_FILE_ID_fk FOREIGN KEY (FILE_ID) REFERENCES [FILE] (ID),
  CONSTRAINT LOG_ISA_ID_fk FOREIGN KEY (ISA_ID) REFERENCES ISA (ID)
);
