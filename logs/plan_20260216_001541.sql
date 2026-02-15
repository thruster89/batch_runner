-- ======================================
-- FILE: 01_a1.sql
-- PARAMS:
--   clsYymm = 202512

select * from CONTRACT_INFO
where TO_CHAR(CONTRACT_DATE, 'YYYYMM') = 202512

-- ======================================
-- FILE: 02_a2.sql
-- PARAMS:
--   clsYymm = 202512

select * from DEPOSIT_INFO
where TO_CHAR(DEPOSIT_DATE, 'YYYYMM') = 202512

-- ======================================
-- FILE: 03_a3.sql
-- PARAMS:
--   clsYymm = 202512

SELECT
    plyno,
    ins_st,
    ins_clstr,
    an_py_stdt,    
    reg_dtm,
    upd_dtm
FROM
    contract
WHERE  TO_CHAR(ins_st, 'YYYYMM') = '202512'

-- ======================================
-- FILE: 04_a4y.sql
-- PARAMS:
--   clsYymm = 202512

SELECT
    plyno,
    ins_st,
    ins_clstr,
    an_py_stdt,    
    reg_dtm,
    upd_dtm
FROM
    contract
WHERE  TO_CHAR(ins_st, 'YYYYMM') = ':clsYymm_bf'

