DROP TABLE util.PARAMS;

CREATE TABLE util.EXT_PARAMS(
  NAME        varchar2(100),
  VALUE       varchar2(4000),
  DESCRIPTION varchar2(4000)
) noparallel;

CREATE TABLE util.TABLE_NAME(
  
) 
TABLESPACE TEST_STG_DATA
PARALLEL NOLOGGING COMPRESS
PARTITION BY LIST (PART_NUM) (  
  PARTITION p1  VALUES (1)  COMPRESS,
  PARTITION p2  VALUES (2)  COMPRESS,
  PARTITION p3  VALUES (3)  COMPRESS,
  PARTITION p4  VALUES (4)  COMPRESS,
  PARTITION p5  VALUES (5)  COMPRESS,
  PARTITION p6  VALUES (6)  COMPRESS,
  PARTITION p7  VALUES (7)  COMPRESS,
  PARTITION p8  VALUES (8)  COMPRESS,
  PARTITION p9  VALUES (9)  COMPRESS,
  PARTITION p10 VALUES (10) COMPRESS,
  PARTITION p11 VALUES (11) COMPRESS,
  PARTITION p12 VALUES (12) COMPRESS,
  PARTITION p13 VALUES (13) COMPRESS,
  PARTITION p14 VALUES (14) COMPRESS,
  PARTITION p15 VALUES (15) COMPRESS,
  PARTITION p16 VALUES (16) COMPRESS,
  PARTITION p17 VALUES (17) COMPRESS,
  PARTITION p18 VALUES (18) COMPRESS,
  PARTITION p19 VALUES (19) COMPRESS,
  PARTITION p20 VALUES (20) COMPRESS,
  PARTITION p21 VALUES (21) COMPRESS,
  PARTITION p22 VALUES (22) COMPRESS,
  PARTITION p23 VALUES (23) COMPRESS,
  PARTITION p24 VALUES (24) COMPRESS,
  PARTITION p25 VALUES (25) COMPRESS,
  PARTITION p26 VALUES (26) COMPRESS,
  PARTITION p27 VALUES (27) COMPRESS,
  PARTITION p28 VALUES (28) COMPRESS,
  PARTITION p29 VALUES (29) COMPRESS,
  PARTITION p30 VALUES (30) COMPRESS,
  PARTITION p31 VALUES (31) COMPRESS,
  PARTITION p32 VALUES (32) COMPRESS,
  PARTITION p0  VALUES (default) COMPRESS
);
