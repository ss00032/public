CREATE OR REPLACE
PROCEDURE        SP_LOG_MRT (
                            IN_BAT_WRK_ID              VARCHAR,      -- BATCH작업ID
                            IN_BAT_WRK_NM              VARCHAR,      -- BATCH작업명
                            IN_BAT_WRK_CRI_YMD         VARCHAR,      -- BATCH작업기준일자
                            IN_BAT_WRK_STEP_SNO        VARCHAR,      -- BATCH작업단계일련번호
                            IN_BAT_WRK_BGN_DTLDTM      VARCHAR,      -- BATCH작업시작상세일시
                            IN_BAT_UNT_WRK_BGN_DTLDTM  VARCHAR,      -- BATCH단위작업시작상세일시
                            IN_BAT_WRK_PRCS_CNT        INTEGER,      -- BATCH작업처리건수
                            IN_BAT_WRK_RLT_CD          VARCHAR,      -- BATCH작업결과코드
                            IN_BAT_WRK_ERR_VAL         VARCHAR,      -- BATCH작업오류값
                            IN_BAT_WRK_MSG_CNTN        VARCHAR,      -- BATCH작업메시지내용
                            IN_BAT_IPT_PRMT_CNTN       VARCHAR,      -- BATCH입력파라미터내용
                            IN_JOB_END_YN              VARCHAR,      -- 작업종료여부
                            OUT_CD                 OUT VARCHAR,      -- 결과코드
                            OUT_VALUE              OUT VARCHAR,      -- 결과값
                            OUT_MSG                OUT VARCHAR       -- 결과메시지
                            )
    LANGUAGE plpgsql
AS $$
/*============================================================================*/
/*  프로그램  ID: SP_LOG_MRT                                                  */
/*  프로그램  명: 프로그램작업로그 적재 PROCEDURE                             */
/*  프로그램설명: SCM_BATCH작업로그_마스터/SCM_BATCH작업로그_상세 적재        */
/*  작   성   자: 분석계                                                      */
/*  작 성  일 자: 2023. 07. 04                                                */
/*  수 정  내 역:                                                             */
/*============================================================================*/

/*----------------------------------------------------------------------------*/
/* [주요기능 설명]                                                            */
/*----------------------------------------------------------------------------*/
/* 1. 집계LOG 생성                                                            */
/*----------------------------------------------------------------------------*/

    /*------------------------------------------------------------------------*/
    /* [변수선언]                                                             */
    /*------------------------------------------------------------------------*/
DECLARE
    V_RUNTIME   VARCHAR(20);
    V_RUNTIME_1 VARCHAR(20);
    V_VALUE     VARCHAR(40);
    V_MSG       VARCHAR(1000);
    V_ERR_CD    VARCHAR(1000);
    V_ERR_MSG   VARCHAR(1000);
    V_CNT       INT;
    V_CED       VARCHAR(8);
    END_DTM     VARCHAR(50);

BEGIN

    /*------------------------------------------------------------------------*/
    /* 기준년,반기,분기,년월,일자 셋팅                                        */
    /*------------------------------------------------------------------------*/

    SELECT CASE WHEN SUBSTR(IN_BAT_WRK_ID,3,1) = 'Y'
                THEN SUBSTR(IN_BAT_WRK_CRI_YMD,1,4)
                WHEN SUBSTR(IN_BAT_WRK_ID,3,1) = 'H'
                THEN CASE WHEN SUBSTR(IN_BAT_WRK_CRI_YMD,5,2) IN ('01','02','03','04','05','06')
                          THEN SUBSTR(IN_BAT_WRK_CRI_YMD,1,4)||'1'
                          ELSE SUBSTR(IN_BAT_WRK_CRI_YMD,1,4)||'2'
                     END
                WHEN SUBSTR(IN_BAT_WRK_ID,3,1) = 'Q'
                THEN CASE WHEN SUBSTR(IN_BAT_WRK_CRI_YMD,5,2) IN ('01','02','03')
                          THEN SUBSTR(IN_BAT_WRK_CRI_YMD,1,4)||'1'
                          WHEN SUBSTR(IN_BAT_WRK_CRI_YMD,5,2) IN ('04','05','06')
                          THEN SUBSTR(IN_BAT_WRK_CRI_YMD,1,4)||'2'
                          WHEN SUBSTR(IN_BAT_WRK_CRI_YMD,5,2) IN ('07','08','09')
                          THEN SUBSTR(IN_BAT_WRK_CRI_YMD,1,4)||'3'
                          ELSE SUBSTR(IN_BAT_WRK_CRI_YMD,1,4)||'4'
                     END
                WHEN SUBSTR(IN_BAT_WRK_ID,3,1) = 'M'
                THEN SUBSTR(IN_BAT_WRK_CRI_YMD,1,6)
                ELSE IN_BAT_WRK_CRI_YMD
           END
      INTO V_CED;
    /*------------------------------------------------------------------------*/
    /* 실행시간 처리                                                          */
    /*------------------------------------------------------------------------*/

    /* 프로그램 최초 실행 시간 */
    SELECT TO_CHAR(AGE(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', TO_TIMESTAMP(IN_BAT_WRK_BGN_DTLDTM, 'YYYYMMDDHH24MISSUS')),'HH24:MI:SS')
    --REPLACE(SUBSTR(TO_CHAR(SYSDATE - CAST(IN_BAT_WRK_BGN_DTLDTM AS TIMESTAMP),'YYYYMMDD HH24MISS'),12,8),':','')
      INTO V_RUNTIME_1;

    /* 집계단계별 실행 시간    */
    SELECT TO_CHAR(AGE(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', TO_TIMESTAMP(IN_BAT_UNT_WRK_BGN_DTLDTM, 'YYYYMMDDHH24MISSUS')),'HH24:MI:SS')
    --REPLACE(SUBSTR(TO_CHAR(SYSDATE - CAST(IN_BAT_UNT_WRK_BGN_DTLDTM AS TIMESTAMP),'YYYYMMDD HH24MISS'),12,8),':','')
      INTO V_RUNTIME;

    IF IN_BAT_WRK_RLT_CD <> '1' THEN -- 작업성공 OR 시작
        /*------------------------------------------------------------------------*/
        /* SCM_BAT_WRK_LOG_M 기준년,반기,분기,년월,일자별로 최근 작업 1 ROW만 존재*/
        /*------------------------------------------------------------------------*/

        /* 프로그램단위 로그                          */
        /* 배치프로그램 로그테이블 적재여부           */


        SELECT COUNT(1)
          INTO V_CNT
          FROM ansor.SCM_BAT_WRK_LOG_M
         WHERE BAT_WRK_ID      = IN_BAT_WRK_ID
           AND BAT_WRK_CRI_YMD = V_CED;

             IF V_CNT = 0 AND IN_JOB_END_YN = 'Y' THEN

             /* SCM_BATCH작업로그_마스터                   */
             /* 작업프로그램내의 최초작업이 최종작업인 경우*/

                       INSERT
                         INTO ansor.SCM_BAT_WRK_LOG_M                                               -- SCM_BATCH작업로그_마스터
                            ( BAT_WRK_ID                                                            -- BATCH작업ID
                            , BAT_WRK_CRI_YMD                                                       -- BATCH작업기준일자
                            , BAT_WRK_BGN_DTLDTM                                                    -- BATCH작업시작상세일시
                            , ETL_WRK_DTM                                                           -- ETL작업일시
                            , BAT_WRK_NM                                                            -- BATCH작업명
                            , BAT_WRK_STEP_SNO                                                      -- BATCH작업단계일련번호
                            , BAT_WRK_END_DTLDTM                                                    -- BATCH작업종료상세일시
                            , BAT_WRK_TM                                                            -- BATCH작업시각
                            , BAT_WRK_PRCS_CNT                                                      -- BATCH작업처리건수
                            , BAT_WRK_RLT_CD                                                        -- BATCH작업결과코드
                            , BAT_WRK_ERR_VAL                                                       -- BATCH작업오류값
                            , BAT_WRK_MSG_CNTN                                                      -- BATCH작업메시지내용
                            , BAT_IPT_PRMT_CNTN                                                     -- BATCH입력파라미터내용
                            )
                       SELECT IN_BAT_WRK_ID                                                         -- BATCH작업ID
                            , V_CED                                                                 -- BATCH작업기준일자
                            , IN_BAT_WRK_BGN_DTLDTM                                                 -- BATCH작업시작상세일시
                            , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'                           -- ETL작업일시
                            , IN_BAT_WRK_NM                                                         -- BATCH작업명
                            , IN_BAT_WRK_STEP_SNO                                                   -- BATCH작업단계일련번호
                            , SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17)     -- BATCH작업종료상세일시
                            , V_RUNTIME_1                                                           -- BATCH작업시각
                            , IN_BAT_WRK_PRCS_CNT                                                   -- BATCH작업처리건수
                            , 'S'                                                                   -- BATCH작업결과코드
                            , NVL(IN_BAT_WRK_ERR_VAL,' ')                                           -- BATCH작업오류값
                            , NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                                       -- BATCH작업메시지내용
                            , NVL(IN_BAT_IPT_PRMT_CNTN,' ')                                         -- BATCH입력파라미터내용
                            ;

             ELSIF V_CNT = 0 AND IN_JOB_END_YN = 'N' THEN

             /* SCM_BATCH작업로그_마스터                   */
             /* 작업프로그램내의 집계시작 로그             */


                       INSERT
                         INTO ansor.SCM_BAT_WRK_LOG_M                                               -- SCM_BATCH작업로그_마스터
                            ( BAT_WRK_ID                                                            -- BATCH작업ID
                            , BAT_WRK_CRI_YMD                                                       -- BATCH작업기준일자
                            , BAT_WRK_BGN_DTLDTM                                                    -- BATCH작업시작상세일시
                            , ETL_WRK_DTM                                                           -- ETL작업일시
                            , BAT_WRK_NM                                                            -- BATCH작업명
                            , BAT_WRK_STEP_SNO                                                      -- BATCH작업단계일련번호
                            , BAT_WRK_END_DTLDTM                                                    -- BATCH작업종료상세일시
                            , BAT_WRK_TM                                                            -- BATCH작업시각
                            , BAT_WRK_PRCS_CNT                                                      -- BATCH작업처리건수
                            , BAT_WRK_RLT_CD                                                        -- BATCH작업결과코드
                            , BAT_WRK_ERR_VAL                                                       -- BATCH작업오류값
                            , BAT_WRK_MSG_CNTN                                                      -- BATCH작업메시지내용
                            , BAT_IPT_PRMT_CNTN                                                     -- BATCH입력파라미터내용
                            )
                       SELECT IN_BAT_WRK_ID                                                         -- BATCH작업ID
                            , V_CED                                                                 -- BATCH작업기준일자
                            , IN_BAT_WRK_BGN_DTLDTM                                                 -- BATCH작업시작상세일시
                            , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'                           -- ETL작업일시
                            , IN_BAT_WRK_NM                                                         -- BATCH작업명
                            , IN_BAT_WRK_STEP_SNO                                                   -- BATCH작업단계일련번호
                            , SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17)     -- BATCH작업종료상세일시
                            , ' '                                                                   -- BATCH작업시각
                            , 0                                                                     -- BATCH작업처리건수
                            , 'R'                                                                   -- BATCH작업결과코드
                            , NVL(IN_BAT_WRK_ERR_VAL,' ')                                           -- BATCH작업오류값
                            , NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                                       -- BATCH작업메시지내용
                            , NVL(IN_BAT_IPT_PRMT_CNTN,' ')                                         -- BATCH입력파라미터내용
                            ;

             ELSIF V_CNT = 1 AND IN_JOB_END_YN = 'N' THEN

             /* SCM_BATCH작업로그_마스터                   */
             /* 배치프로그램내의 중간작업에 대한 시작 로그 */


                       UPDATE ansor.SCM_BAT_WRK_LOG_M                                               -- SCM_BATCH작업로그_마스터
                          SET BAT_WRK_NM = IN_BAT_WRK_NM                                            -- BATCH작업명
                            , BAT_WRK_STEP_SNO = IN_BAT_WRK_STEP_SNO                                -- BATCH작업단계일련번호
                            , BAT_WRK_BGN_DTLDTM = IN_BAT_WRK_BGN_DTLDTM                            -- BATCH작업시작상세일시
                            , ETL_WRK_DTM = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'             -- ETL작업일시
                            , BAT_WRK_END_DTLDTM = SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17) -- BATCH작업종료상세일시
                            , BAT_WRK_TM = V_RUNTIME_1                                              -- BATCH작업시각
                            , BAT_WRK_PRCS_CNT = IN_BAT_WRK_PRCS_CNT                                -- BATCH작업처리건수
                            , BAT_WRK_RLT_CD = 'R'                                                  -- BATCH작업결과코드
                            , BAT_WRK_ERR_VAL = NVL(IN_BAT_WRK_ERR_VAL,' ')                         -- BATCH작업오류값
                            , BAT_WRK_MSG_CNTN = NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                    -- BATCH작업메시지내용
                        WHERE BAT_WRK_ID = IN_BAT_WRK_ID                                            -- BATCH작업ID
                          AND BAT_WRK_CRI_YMD = V_CED;                                              -- BATCH작업기준일자

             ELSIF V_CNT = 1 AND IN_JOB_END_YN = 'Y' THEN

             /* SCM_BATCH작업로그_마스터                   */
             /* 배치프로그램내의 최종작업에 대한 성공 로그 */


                      UPDATE ansor.SCM_BAT_WRK_LOG_M                                                -- SCM_BATCH작업로그_마스터
                         SET BAT_WRK_NM = IN_BAT_WRK_NM                                             -- BATCH작업명
                           , BAT_WRK_STEP_SNO = IN_BAT_WRK_STEP_SNO                                 -- BATCH작업단계일련번호
                           , BAT_WRK_BGN_DTLDTM = IN_BAT_WRK_BGN_DTLDTM                             -- BATCH작업시작상세일시
                           , ETL_WRK_DTM = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'              -- ETL작업일시
                           , BAT_WRK_END_DTLDTM = SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17) -- BATCH작업종료상세일시
                           , BAT_WRK_TM = V_RUNTIME_1                                               -- BATCH작업시각
                           , BAT_WRK_PRCS_CNT = IN_BAT_WRK_PRCS_CNT                                 -- BATCH작업처리건수
                           , BAT_WRK_RLT_CD = 'S'                                                   -- BATCH작업결과코드
                           , BAT_WRK_ERR_VAL = NVL(IN_BAT_WRK_ERR_VAL,' ')                          -- BATCH작업오류값
                           , BAT_WRK_MSG_CNTN = NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                     -- BATCH작업메시지내용
                       WHERE BAT_WRK_ID = IN_BAT_WRK_ID                                             -- BATCH작업ID
                         AND BAT_WRK_CRI_YMD = V_CED;                                               -- BATCH작업기준일자

             END IF;

        /*------------------------------------------------------------------------*/
        /* SCM_BAT_WRK_LOG_D STEP 별로 INSERT                                            */
        /*------------------------------------------------------------------------*/

        /* SCM_BATCH작업로그_상세                     */
        /* 배치프로그램내의 STEP에 대한 성공 로그     */


        INSERT
          INTO ansor.SCM_BAT_WRK_LOG_D                                                              -- SCM_BATCH작업로그_상세
             ( BAT_WRK_ID                                                                           -- BATCH작업ID
             , BAT_WRK_CRI_YMD                                                                      -- BATCH작업기준일자
             , BAT_WRK_BGN_DTLDTM                                                                   -- BATCH작업시작상세일시
             , BAT_WRK_STEP_SNO                                                                     -- BATCH작업단계일련번호
             , ETL_WRK_DTM                                                                          -- ETL작업일시
             , BAT_WRK_NM                                                                           -- BATCH작업명
             , BAT_UNT_WRK_BGN_DTLDTM                                                               -- BATCH단위작업시작상세일시
             , BAT_UNT_WRK_END_DTLDTM                                                               -- BATCH단위작업종료상세일시
             , BAT_WRK_TM                                                                           -- BATCH작업시각
             , BAT_WRK_PRCS_CNT                                                                     -- BATCH작업처리건수
             , BAT_WRK_RLT_CD                                                                       -- BATCH작업결과코드
             , BAT_WRK_ERR_VAL                                                                      -- BATCH작업오류값
             , BAT_WRK_MSG_CNTN                                                                     -- BATCH작업메시지내용
             )
        SELECT IN_BAT_WRK_ID                                                                        -- BATCH작업ID
             , V_CED                                                                                -- BATCH작업기준일자
             , IN_BAT_WRK_BGN_DTLDTM                                                                -- BATCH작업시작상세일시
             , IN_BAT_WRK_STEP_SNO                                                                  -- BATCH작업단계일련번호
             , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'                                          -- ETL작업일시
             , IN_BAT_WRK_NM                                                                        -- BATCH작업명
             , IN_BAT_UNT_WRK_BGN_DTLDTM                                                            -- BATCH단위작업시작상세일시
             , SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17) -- BATCH단위작업종료상세일시
             , V_RUNTIME                                                                            -- BATCH작업시각
             , IN_BAT_WRK_PRCS_CNT                                                                  -- BATCH작업처리건수
             , 'S'                                                                                  -- BATCH작업결과코드
             , NVL(IN_BAT_WRK_ERR_VAL,' ')                                                          -- BATCH작업오류값
             , NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                                                      -- BATCH작업메시지내용
             ;

        --COMMIT;


    /*----------------------------------------------------------------------------*/
    /*--------------------------------적재 에러로그-------------------------------*/
    /*----------------------------------------------------------------------------*/
    ELSE

           /* SCM_BATCH작업로그_마스터                   */
           /* 배치프로그램 로그테이블 적재여부           */

           SELECT COUNT(1)
             INTO V_CNT
             FROM ansor.SCM_BAT_WRK_LOG_M                                                           -- SCM_BATCH작업로그_마스터
            WHERE BAT_WRK_ID       = IN_BAT_WRK_ID                                                  -- BATCH작업ID
              AND BAT_WRK_CRI_YMD  = V_CED;                                                         -- BATCH작업기준일자

           IF V_CNT = 0 THEN

           /* SCM_BATCH작업로그_마스터                   */
           /* 배치프로그램내의 최초작업에 대한 에러 로그 */

                      INSERT
                        INTO ansor.SCM_BAT_WRK_LOG_M                                                -- SCM_BATCH작업로그_마스터
                           ( BAT_WRK_ID                                                             -- BATCH작업ID
                           , BAT_WRK_CRI_YMD                                                        -- BATCH작업기준일자
                           , BAT_WRK_BGN_DTLDTM                                                     -- BATCH작업시작상세일시
                           , ETL_WRK_DTM                                                            -- ETL작업일시
                           , BAT_WRK_NM                                                             -- BATCH작업명
                           , BAT_WRK_STEP_SNO                                                       -- BATCH작업단계일련번호
                           , BAT_WRK_END_DTLDTM                                                     -- BATCH작업종료상세일시
                           , BAT_WRK_TM                                                             -- BATCH작업시각
                           , BAT_WRK_PRCS_CNT                                                       -- BATCH작업처리건수
                           , BAT_WRK_RLT_CD                                                         -- BATCH작업결과코드
                           , BAT_WRK_ERR_VAL                                                        -- BATCH작업오류값
                           , BAT_WRK_MSG_CNTN                                                       -- BATCH작업메시지내용
                           , BAT_IPT_PRMT_CNTN                                                      -- BATCH입력파라미터내용
                           )
                      SELECT IN_BAT_WRK_ID                                                          -- BATCH작업ID
                           , V_CED                                                                  -- BATCH작업기준일자
                           , IN_BAT_WRK_BGN_DTLDTM                                                  -- BATCH작업시작상세일시
                           , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'                            -- ETL작업일시
                           , IN_BAT_WRK_NM                                                          -- BATCH작업명
                           , IN_BAT_WRK_STEP_SNO                                                    -- BATCH작업단계일련번호
                           , SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17)      -- BATCH작업종료상세일시
                           , V_RUNTIME_1                                                            -- BATCH작업시각
                           , 0                                                                      -- BATCH작업처리건수
                           , 'E'                                                                    -- BATCH작업결과코드
                           , NVL(IN_BAT_WRK_ERR_VAL,' ')                                            -- BATCH작업오류값
                           , NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                                        -- BATCH작업메시지내용
                           , NVL(IN_BAT_IPT_PRMT_CNTN,' ')                                          -- BATCH입력파라미터내용
                           ;

           ELSIF IN_JOB_END_YN = 'Y' THEN

           /* SCM_BATCH작업로그_마스터                   */
           /* 배치프로그램내의 최종작업에 대한 에러 로그 */


                       UPDATE ansor.SCM_BAT_WRK_LOG_M                                               -- SCM_BATCH작업로그_마스터
                          SET BAT_WRK_NM             = IN_BAT_WRK_NM                                -- BATCH작업명
                            , BAT_WRK_STEP_SNO       = IN_BAT_WRK_STEP_SNO                          -- BATCH작업단계일련번호
                            , BAT_WRK_BGN_DTLDTM  = IN_BAT_WRK_BGN_DTLDTM                           -- BATCH작업시작상세일시
                            , ETL_WRK_DTM = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'             -- ETL작업일시
                            , BAT_WRK_END_DTLDTM  = SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17) -- BATCH작업종료상세일시
                            , BAT_WRK_TM       = V_RUNTIME_1                                        -- BATCH작업시각
                            , BAT_WRK_PRCS_CNT = 0                                                  -- BATCH작업처리건수
                            , BAT_WRK_RLT_CD   = 'E'                                                -- BATCH작업결과코드
                            , BAT_WRK_ERR_VAL  = NVL(IN_BAT_WRK_ERR_VAL,' ')                        -- BATCH작업오류값
                            , BAT_WRK_MSG_CNTN = NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                    -- BATCH작업메시지내용
                        WHERE BAT_WRK_ID       = IN_BAT_WRK_ID                                      -- BATCH작업ID
                          AND BAT_WRK_CRI_YMD  = V_CED;                                             -- BATCH작업기준일자

           END IF;

           /* SCM_BATCH작업로그_상세                     */
           /* 배치프로그램내의 STEP에 대한 에러 로그     */


           INSERT
             INTO ansor.SCM_BAT_WRK_LOG_D                                                           -- SCM_BATCH작업로그_상세
                ( BAT_WRK_ID                                                                        -- BATCH작업ID
                , BAT_WRK_CRI_YMD                                                                   -- BATCH작업기준일자
                , BAT_WRK_BGN_DTLDTM                                                                -- BATCH작업시작상세일시
                , BAT_WRK_STEP_SNO                                                                  -- BATCH작업단계일련번호
                , ETL_WRK_DTM                                                                       -- ETL작업일시
                , BAT_WRK_NM                                                                        -- BATCH작업명
                , BAT_UNT_WRK_BGN_DTLDTM                                                            -- BATCH단위작업시작상세일시
                , BAT_UNT_WRK_END_DTLDTM                                                            -- BATCH단위작업종료상세일시
                , BAT_WRK_TM                                                                        -- BATCH작업시각
                , BAT_WRK_PRCS_CNT                                                                  -- BATCH작업처리건수
                , BAT_WRK_RLT_CD                                                                    -- BATCH작업결과코드
                , BAT_WRK_ERR_VAL                                                                   -- BATCH작업오류값
                , BAT_WRK_MSG_CNTN                                                                  -- BATCH작업메시지내용
                )
           SELECT IN_BAT_WRK_ID                                                                     -- BATCH작업ID
                , V_CED                                                                             -- BATCH작업기준일자
                , IN_BAT_WRK_BGN_DTLDTM                                                             -- BATCH작업시작상세일시
                , IN_BAT_WRK_STEP_SNO                                                               -- BATCH작업단계일련번호
                , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'                                       -- ETL작업일시
                , IN_BAT_WRK_NM                                                                     -- BATCH작업명
                , IN_BAT_UNT_WRK_BGN_DTLDTM                                                         -- BATCH단위작업시작상세일시
                , SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17)                 -- BATCH단위작업종료상세일시
                , V_RUNTIME                                                                         -- BATCH작업시각
                , 0                                                                                 -- BATCH작업처리건수
                , 'E'                                                                               -- BATCH작업결과코드
                , NVL(IN_BAT_WRK_ERR_VAL,' ')                                                       -- BATCH작업오류값
                , NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                                                   -- BATCH작업메시지내용
                ;

             --COMMIT;

    END IF;

OUT_MSG := END_DTM;

END;
$$;