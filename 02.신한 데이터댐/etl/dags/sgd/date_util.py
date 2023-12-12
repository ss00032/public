# -*- coding: utf-8 -*-

import pendulum
from datetime import date, timedelta

# 기준일 = '{{ dag.timezone.convert(execution_date).strftime("%Y%m%d") }}'

P_CED = '{{ dag.timezone.convert(execution_date).strftime("%Y%m%d") }}'  # 기준일 YYYYMMDD
P_CED_BF_D = '{{ (dag.timezone.convert(execution_date)).subtract(days=1).strftime("%Y%m%d") }}'  # 기준일전일 YYYYMMDD
P_CED_NDY = '{{ (dag.timezone.convert(execution_date)).add(days=1).strftime("%Y%m%d") }}'  # 기준일익일 YYYYMMDD
P_TA_M = '{{ (dag.timezone.convert(execution_date)).strftime("%m") }}'  # 기준월 MM
P_TA_YM = '{{ dag.timezone.convert(execution_date).strftime("%Y%m") }}'  # 기준년월 YYYYMM
P_TA_YM_STD = '{{ dag.timezone.convert(execution_date).start_of("month").strftime("%Y%m%d") }}'  # 기준년월시작일자 YYYYMMDD
P_TA_YM_EDD = '{{ dag.timezone.convert(execution_date).end_of("month").strftime("%Y%m%d") }}' # 기준년월종료일자 YYYYMMDD
P_TA_Y_STM = '{{ dag.timezone.convert(execution_date).start_of("year").strftime("%Y%m") }}' # 기준년시작월 YYYY01
P_TA_Y_EDM = '{{ dag.timezone.convert(execution_date).end_of("year").strftime("%Y%m") }}' # 기준년종료월 YYYY12
P_BY_STM = '{{ dag.timezone.convert(execution_date).subtract(years=1).start_of("year").strftime("%Y%m") }}' # 전년시작월 YYYY01
P_BY_EDM = '{{ dag.timezone.convert(execution_date).subtract(years=1).end_of("year").strftime("%Y%m") }}'  # 전년종료월 YYYY12
P_TA_YM_MNH_STD = '{{ dag.timezone.convert(execution_date).add(months=1).start_of("month").strftime("%Y%m%d") }}'  # 기준년월익월시작일자 YYYYMMDD
P_TA_YM_MNH_EDD = '{{ dag.timezone.convert(execution_date).add(months=1).end_of("month").strftime("%Y%m%d") }}'  # 기준년월익월종료일자 YYYYMMDD
P_M1_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=1).strftime("%Y%m") }}'  # 1개월전기준년월 YYYYMM
P_M2_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=2).strftime("%Y%m") }}'  # 2개월전기준년월 YYYYMM
P_M3_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=3).strftime("%Y%m") }}'  # 3개월전기준년월 YYYYMM
P_M4_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=4).strftime("%Y%m") }}'  # 4개월전기준년월 YYYYMM
P_M5_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=5).strftime("%Y%m") }}'  # 5개월전기준년월 YYYYMM
P_M6_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=6).strftime("%Y%m") }}'  # 6개월전기준년월 YYYYMM
P_M7_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=7).strftime("%Y%m") }}'  # 7개월전기준년월 YYYYMM
P_M8_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=8).strftime("%Y%m") }}'  # 8개월전기준년월 YYYYMM
P_M9_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=9).strftime("%Y%m") }}'  # 9개월전기준년월 YYYYMM
P_M10_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=10).strftime("%Y%m") }}'  # 10개월전기준년월 YYYYMM
P_M11_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=11).strftime("%Y%m") }}'  # 11개월전기준년월 YYYYMM
P_M12_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=12).strftime("%Y%m") }}'  # 12개월전기준년월 YYYYMM
P_M13_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=13).strftime("%Y%m") }}'  # 13개월전기준년월 YYYYMM
P_M14_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=14).strftime("%Y%m") }}'  # 14개월전기준년월 YYYYMM
P_M15_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=15).strftime("%Y%m") }}'  # 15개월전기준년월 YYYYMM
P_M16_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=16).strftime("%Y%m") }}'  # 16개월전기준년월 YYYYMM
P_M17_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=17).strftime("%Y%m") }}'  # 17개월전기준년월 YYYYMM
P_M18_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=18).strftime("%Y%m") }}'  # 18개월전기준년월 YYYYMM
P_M19_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=19).strftime("%Y%m") }}'  # 19개월전기준년월 YYYYMM
P_M20_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=20).strftime("%Y%m") }}'  # 20개월전기준년월 YYYYMM
P_M21_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=21).strftime("%Y%m") }}'  # 21개월전기준년월 YYYYMM
P_M22_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=22).strftime("%Y%m") }}'  # 22개월전기준년월 YYYYMM
P_M23_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=23).strftime("%Y%m") }}'  # 23개월전기준년월 YYYYMM
P_M24_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=24).strftime("%Y%m") }}'  # 24개월전기준년월 YYYYMM
P_M35_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=35).strftime("%Y%m") }}'  # 35개월전기준년월 YYYYMM
P_M47_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=47).strftime("%Y%m") }}'  # 47개월전기준년월 YYYYMM
P_M59_BF_YM = '{{ (dag.timezone.convert(execution_date)).subtract(months=59).strftime("%Y%m") }}'  # 59개월전기준년월 YYYYMM


SGD_DATE = {
    'P_CED' : P_CED,
    'P_CED_BF_D' : P_CED_BF_D,
    'P_CED_NDY' : P_CED_NDY,
    'P_TA_M' : P_TA_M,
    'P_TA_YM' : P_TA_YM,
    'P_TA_YM_STD' : P_TA_YM_STD,
    'P_TA_YM_EDD' : P_TA_YM_EDD,
    'P_TA_Y_STM' : P_TA_Y_STM,
    'P_TA_Y_EDM' : P_TA_Y_EDM,
    'P_BY_STM' : P_BY_STM,
    'P_BY_EDM' : P_BY_EDM,
    'P_TA_YM_MNH_STD' : P_TA_YM_MNH_STD,
    'P_TA_YM_MNH_EDD' : P_TA_YM_MNH_EDD,
    'P_M1_BF_YM' : P_M1_BF_YM,
    'P_M2_BF_YM' : P_M2_BF_YM,
    'P_M3_BF_YM' : P_M3_BF_YM,
    'P_M4_BF_YM' : P_M4_BF_YM,
    'P_M5_BF_YM' : P_M5_BF_YM,
    'P_M6_BF_YM' : P_M6_BF_YM,
    'P_M7_BF_YM' : P_M7_BF_YM,
    'P_M8_BF_YM' : P_M8_BF_YM,
    'P_M9_BF_YM' : P_M9_BF_YM,
    'P_M10_BF_YM' : P_M10_BF_YM,
    'P_M11_BF_YM' : P_M11_BF_YM,
    'P_M12_BF_YM' : P_M12_BF_YM,
    'P_M13_BF_YM' : P_M13_BF_YM,
    'P_M14_BF_YM' : P_M14_BF_YM,
    'P_M15_BF_YM' : P_M15_BF_YM,
    'P_M16_BF_YM' : P_M16_BF_YM,
    'P_M17_BF_YM' : P_M17_BF_YM,
    'P_M18_BF_YM' : P_M18_BF_YM,
    'P_M19_BF_YM' : P_M19_BF_YM,
    'P_M20_BF_YM' : P_M20_BF_YM,
    'P_M21_BF_YM' : P_M21_BF_YM,
    'P_M22_BF_YM' : P_M22_BF_YM,
    'P_M23_BF_YM' : P_M23_BF_YM,
    'P_M24_BF_YM' : P_M24_BF_YM,
    'P_M35_BF_YM' : P_M35_BF_YM,
    'P_M47_BF_YM' : P_M47_BF_YM,
    'P_M59_BF_YM' : P_M59_BF_YM
}


def date_cd(date_input):
    return SGD_DATE[date_input]


def get_current_dt(fmt_string='%Y%m%d'):
    return pendulum.now('Asia/Seoul').strftime(fmt_string)


