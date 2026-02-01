select * from DEPOSIT_INFO
where TO_CHAR(DEPOSIT_DATE, 'YYYYMM') = :clsYymm