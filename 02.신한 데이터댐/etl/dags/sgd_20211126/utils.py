# -*- coding: utf-8 -*-

from airflow.exceptions import AirflowException


# 이용목적
SGD_UP_CODES = {
    "i": "igd",  # 통합데이터
    "a": "acd",  # 동의고객데이터
}
# PGM 적재구분
SGD_PT_CODES = {
    "u": "unload",  # 파일 Unload
    "l": "load",    # 파일 Load
    "b": "batch",   # 배치(L1/L2 처리)
}
# 그룹사 구분
SGD_CP_CODES = {
    "b": "shb",   # 은행
    "c": "shc",   # 카드
    "i": "shi",   # 금융투자
    "l": "shl",   # 라이프
    "t": "total",  # 통합 (L1/L2) 처리 등
}
# 적재 시점
SGD_TM_CODES = {
    "d": "day",
    "m": "month",
    "w": "week",
    "q": "quarter",
    "h": "half",
}
# 테이블 적재 유형
SGD_TL_CODES = {
    "a": "append",
    "o": "overwrite",
    "m": "merge",
}


def valid_table_load_type(table_load_type):
    return table_load_type in SGD_TL_CODES


def valid_company_code(company_code):
    return company_code in SGD_CP_CODES.values()


def valid_use_purpose(use_purpose):
    return use_purpose in SGD_UP_CODES.values()


def valid_time_interval(tm_code):
    return tm_code in SGD_TM_CODES


def valid_program_type(pt_code):
    return pt_code in SGD_PT_CODES


def parse_pgm_id(pgm_id):
    # ex) pgm_id = 'ILBD_DWA_JOB_DATE_TG'
    fields = pgm_id.lower().split('_')
    tbl_nm = '_'.join(fields[1:len(fields) - 1])

    up_cd = fields[0][0]
    pt_cd = fields[0][1]
    cp_cd = fields[0][2]
    tm_cd = fields[0][3]
    tg_cd = fields[len(fields) - 1]

    tags = [SGD_UP_CODES[up_cd], SGD_PT_CODES[pt_cd], SGD_CP_CODES[cp_cd], SGD_TM_CODES[tm_cd]]

    return up_cd, pt_cd, cp_cd, tm_cd, tbl_nm, tg_cd, tags
