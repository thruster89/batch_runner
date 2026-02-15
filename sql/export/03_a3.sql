SELECT
    plyno,
    ins_st,
    ins_clstr,
    an_py_stdt,    
    reg_dtm,
    upd_dtm
FROM
    contract
WHERE  TO_CHAR(ins_st, 'YYYYMM') = ':clsYymm'
    ;