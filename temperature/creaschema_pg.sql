  CREATE SCHEMA temperature;
  CREATE TABLE TEMPERATURE.COUNTRY
    (
      IDCOUNTRY SERIAL,
      COUNTRY   VARCHAR(50),
      CONSTRAINT un_IDCOUNTRY UNIQUE(IDCOUNTRY),
      CONSTRAINT pk_COUNTRY PRIMARY KEY (COUNTRY)
    );
  CREATE TABLE TEMPERATURE.STATE
    (
      IDSTATE SERIAL,
      STATE     VARCHAR(40),
      IDCOUNTRY INTEGER NOT NULL,
      CONSTRAINT un_IDSTATE UNIQUE(IDSTATE),
      CONSTRAINT pk_STATE PRIMARY KEY (STATE,IDCOUNTRY),
      CONSTRAINT fk_STATE_COUNTRY FOREIGN KEY (IDCOUNTRY) REFERENCES TEMPERATURE.COUNTRY(IDCOUNTRY)
    );
  CREATE TABLE TEMPERATURE.CITY
    (
      IDCITY    SERIAL,
      CITY      VARCHAR(30),
      IDCOUNTRY INTEGER NOT NULL,
      LATITUDE  NUMERIC(11,7),
      LONGITUDE NUMERIC(11,7),
      CONSTRAINT un_IDCITY UNIQUE(IDCITY),
      CONSTRAINT pk_CITY PRIMARY KEY (CITY, IDCOUNTRY,LATITUDE,LONGITUDE),
      CONSTRAINT fk_CITY_COUNTRY FOREIGN KEY (IDCOUNTRY) REFERENCES TEMPERATURE.COUNTRY(IDCOUNTRY)
    );
  CREATE TABLE TEMPERATURE.TEMP_CITY
    (
      IDTEMPCITY  SERIAL,
      DATA        DATE,
      TEMPERATURE NUMERIC(10,6),
      ERROR       NUMERIC(10,6),
      IDCITY      INTEGER NOT NULL,
      CONSTRAINT un_IDTEMPCITY UNIQUE(IDTEMPCITY),
      CONSTRAINT pk_TEMPCITY PRIMARY KEY (DATA, IDCITY),
      CONSTRAINT fk_TEMPCITY_CITY FOREIGN KEY (IDCITY) REFERENCES TEMPERATURE.CITY(IDCITY)
    );
  CREATE TABLE TEMPERATURE.TEMP_MAJ_CITY
    (
      IDTEMPMAJCITY SERIAL,
      DATA          DATE,
      TEMPERATURE   NUMERIC(10,6),
      ERROR         NUMERIC(10,6),
      IDCITY        INTEGER NOT NULL,
      CONSTRAINT un_IDTEMPMAJCITY UNIQUE(IDTEMPMAJCITY),
      CONSTRAINT pk_TEMPMAJCITY PRIMARY KEY (DATA, IDCITY),
      CONSTRAINT fk_TEMPMAJCITY_CITY FOREIGN KEY (IDCITY) REFERENCES TEMPERATURE.CITY(IDCITY)
    );
  CREATE TABLE TEMPERATURE.TEMP_COUNTRY
    (
      IDTEMPCOUNTRY SERIAL,
      DATA          DATE,
      TEMPERATURE   NUMERIC(10,6),
      ERROR         NUMERIC(10,6),
      IDCOUNTRY     INTEGER NOT NULL,
      CONSTRAINT un_IDTEMPCOUNTRY UNIQUE(IDTEMPCOUNTRY),
      CONSTRAINT pk_TEMPCOUNTRY PRIMARY KEY (DATA, IDCOUNTRY),
      CONSTRAINT fk_TEMPCOUNTRY_COUNTRY FOREIGN KEY (IDCOUNTRY) REFERENCES TEMPERATURE.COUNTRY(IDCOUNTRY)
    );
  CREATE TABLE TEMPERATURE.TEMP_STATE
    (
      IDTEMPSTATE SERIAL,
      DATA        DATE,
      TEMPERATURE NUMERIC(10,6),
      ERROR       NUMERIC(10,6),
      IDSTATE     INTEGER NOT NULL,
      CONSTRAINT un_IDTEMPSTATE UNIQUE(IDTEMPSTATE),
      CONSTRAINT pk_TEMPSTATE PRIMARY KEY (DATA, IDSTATE),
      CONSTRAINT fk_TEMPSTATE_STATE FOREIGN KEY (IDSTATE) REFERENCES TEMPERATURE.STATE(IDSTATE)
    );

