# -*- coding: utf-8 -*-


# 기준일 = '{{ dag.timezone.convert(execution_date).strftime("%Y%m%d") }}'


P_CED = '{{ dag.timezone.convert(execution_date).strftime("%Y%m%d") }}'  # 기준일
P_TA_YM = '{{ dag.timezone.convert(execution_date).strftime("%Y%m") }}'  # 기준년월
P_CED_BF_D = '{{ (dag.timezone.convert(execution_date)).subtract(days=1).strftime("%Y%m%d") }}'  # 기준일전일
P_M1_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=1).strftime("%Y%m") }}'  # 1개월전기준년월

SGD_DATE = {
    'P_CED' : P_CED,  # 기준일
    'P_TA_YM': P_TA_YM,  # 기준년월
    'P_CED_BF_D' : P_CED_BF_D,  # 기준일전일
    'P_M1_BF_YM' : P_M1_BF_YM  # 1개월전기준년월
}


def date_cd(date_input):
    return SGD_DATE[date_input]




