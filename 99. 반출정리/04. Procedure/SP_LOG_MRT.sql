CREATE OR REPLACE
PROCEDURE        SP_LOG_MRT (
                            IN_BAT_WRK_ID              VARCHAR,      -- BATCH�۾�ID
                            IN_BAT_WRK_NM              VARCHAR,      -- BATCH�۾���
                            IN_BAT_WRK_CRI_YMD         VARCHAR,      -- BATCH�۾���������
                            IN_BAT_WRK_STEP_SNO        VARCHAR,      -- BATCH�۾��ܰ��Ϸù�ȣ
                            IN_BAT_WRK_BGN_DTLDTM      VARCHAR,      -- BATCH�۾����ۻ��Ͻ�
                            IN_BAT_UNT_WRK_BGN_DTLDTM  VARCHAR,      -- BATCH�����۾����ۻ��Ͻ�
                            IN_BAT_WRK_PRCS_CNT        INTEGER,      -- BATCH�۾�ó���Ǽ�
                            IN_BAT_WRK_RLT_CD          VARCHAR,      -- BATCH�۾�����ڵ�
                            IN_BAT_WRK_ERR_VAL         VARCHAR,      -- BATCH�۾�������
                            IN_BAT_WRK_MSG_CNTN        VARCHAR,      -- BATCH�۾��޽�������
                            IN_BAT_IPT_PRMT_CNTN       VARCHAR,      -- BATCH�Է��Ķ���ͳ���
                            IN_JOB_END_YN              VARCHAR,      -- �۾����Ῡ��
                            OUT_CD                 OUT VARCHAR,      -- ����ڵ�
                            OUT_VALUE              OUT VARCHAR,      -- �����
                            OUT_MSG                OUT VARCHAR       -- ����޽���
                            )
    LANGUAGE plpgsql
AS $$
/*============================================================================*/
/*  ���α׷�  ID: SP_LOG_MRT                                                  */
/*  ���α׷�  ��: ���α׷��۾��α� ���� PROCEDURE                             */
/*  ���α׷�����: SCM_BATCH�۾��α�_������/SCM_BATCH�۾��α�_�� ����        */
/*  ��   ��   ��: �м���                                                      */
/*  �� ��  �� ��: 2023. 07. 04                                                */
/*  �� ��  �� ��:                                                             */
/*============================================================================*/

/*----------------------------------------------------------------------------*/
/* [�ֿ��� ����]                                                            */
/*----------------------------------------------------------------------------*/
/* 1. ����LOG ����                                                            */
/*----------------------------------------------------------------------------*/

    /*------------------------------------------------------------------------*/
    /* [��������]                                                             */
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
    /* ���س�,�ݱ�,�б�,���,���� ����                                        */
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
    /* ����ð� ó��                                                          */
    /*------------------------------------------------------------------------*/

    /* ���α׷� ���� ���� �ð� */
    SELECT TO_CHAR(AGE(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', TO_TIMESTAMP(IN_BAT_WRK_BGN_DTLDTM, 'YYYYMMDDHH24MISSUS')),'HH24:MI:SS')
    --REPLACE(SUBSTR(TO_CHAR(SYSDATE - CAST(IN_BAT_WRK_BGN_DTLDTM AS TIMESTAMP),'YYYYMMDD HH24MISS'),12,8),':','')
      INTO V_RUNTIME_1;

    /* ����ܰ躰 ���� �ð�    */
    SELECT TO_CHAR(AGE(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', TO_TIMESTAMP(IN_BAT_UNT_WRK_BGN_DTLDTM, 'YYYYMMDDHH24MISSUS')),'HH24:MI:SS')
    --REPLACE(SUBSTR(TO_CHAR(SYSDATE - CAST(IN_BAT_UNT_WRK_BGN_DTLDTM AS TIMESTAMP),'YYYYMMDD HH24MISS'),12,8),':','')
      INTO V_RUNTIME;

    IF IN_BAT_WRK_RLT_CD <> '1' THEN -- �۾����� OR ����
        /*------------------------------------------------------------------------*/
        /* SCM_BAT_WRK_LOG_M ���س�,�ݱ�,�б�,���,���ں��� �ֱ� �۾� 1 ROW�� ����*/
        /*------------------------------------------------------------------------*/

        /* ���α׷����� �α�                          */
        /* ��ġ���α׷� �α����̺� ���翩��           */


        SELECT COUNT(1)
          INTO V_CNT
          FROM ansor.SCM_BAT_WRK_LOG_M
         WHERE BAT_WRK_ID      = IN_BAT_WRK_ID
           AND BAT_WRK_CRI_YMD = V_CED;

             IF V_CNT = 0 AND IN_JOB_END_YN = 'Y' THEN

             /* SCM_BATCH�۾��α�_������                   */
             /* �۾����α׷����� �����۾��� �����۾��� ���*/

                       INSERT
                         INTO ansor.SCM_BAT_WRK_LOG_M                                               -- SCM_BATCH�۾��α�_������
                            ( BAT_WRK_ID                                                            -- BATCH�۾�ID
                            , BAT_WRK_CRI_YMD                                                       -- BATCH�۾���������
                            , BAT_WRK_BGN_DTLDTM                                                    -- BATCH�۾����ۻ��Ͻ�
                            , ETL_WRK_DTM                                                           -- ETL�۾��Ͻ�
                            , BAT_WRK_NM                                                            -- BATCH�۾���
                            , BAT_WRK_STEP_SNO                                                      -- BATCH�۾��ܰ��Ϸù�ȣ
                            , BAT_WRK_END_DTLDTM                                                    -- BATCH�۾�������Ͻ�
                            , BAT_WRK_TM                                                            -- BATCH�۾��ð�
                            , BAT_WRK_PRCS_CNT                                                      -- BATCH�۾�ó���Ǽ�
                            , BAT_WRK_RLT_CD                                                        -- BATCH�۾�����ڵ�
                            , BAT_WRK_ERR_VAL                                                       -- BATCH�۾�������
                            , BAT_WRK_MSG_CNTN                                                      -- BATCH�۾��޽�������
                            , BAT_IPT_PRMT_CNTN                                                     -- BATCH�Է��Ķ���ͳ���
                            )
                       SELECT IN_BAT_WRK_ID                                                         -- BATCH�۾�ID
                            , V_CED                                                                 -- BATCH�۾���������
                            , IN_BAT_WRK_BGN_DTLDTM                                                 -- BATCH�۾����ۻ��Ͻ�
                            , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'                           -- ETL�۾��Ͻ�
                            , IN_BAT_WRK_NM                                                         -- BATCH�۾���
                            , IN_BAT_WRK_STEP_SNO                                                   -- BATCH�۾��ܰ��Ϸù�ȣ
                            , SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17)     -- BATCH�۾�������Ͻ�
                            , V_RUNTIME_1                                                           -- BATCH�۾��ð�
                            , IN_BAT_WRK_PRCS_CNT                                                   -- BATCH�۾�ó���Ǽ�
                            , 'S'                                                                   -- BATCH�۾�����ڵ�
                            , NVL(IN_BAT_WRK_ERR_VAL,' ')                                           -- BATCH�۾�������
                            , NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                                       -- BATCH�۾��޽�������
                            , NVL(IN_BAT_IPT_PRMT_CNTN,' ')                                         -- BATCH�Է��Ķ���ͳ���
                            ;

             ELSIF V_CNT = 0 AND IN_JOB_END_YN = 'N' THEN

             /* SCM_BATCH�۾��α�_������                   */
             /* �۾����α׷����� ������� �α�             */


                       INSERT
                         INTO ansor.SCM_BAT_WRK_LOG_M                                               -- SCM_BATCH�۾��α�_������
                            ( BAT_WRK_ID                                                            -- BATCH�۾�ID
                            , BAT_WRK_CRI_YMD                                                       -- BATCH�۾���������
                            , BAT_WRK_BGN_DTLDTM                                                    -- BATCH�۾����ۻ��Ͻ�
                            , ETL_WRK_DTM                                                           -- ETL�۾��Ͻ�
                            , BAT_WRK_NM                                                            -- BATCH�۾���
                            , BAT_WRK_STEP_SNO                                                      -- BATCH�۾��ܰ��Ϸù�ȣ
                            , BAT_WRK_END_DTLDTM                                                    -- BATCH�۾�������Ͻ�
                            , BAT_WRK_TM                                                            -- BATCH�۾��ð�
                            , BAT_WRK_PRCS_CNT                                                      -- BATCH�۾�ó���Ǽ�
                            , BAT_WRK_RLT_CD                                                        -- BATCH�۾�����ڵ�
                            , BAT_WRK_ERR_VAL                                                       -- BATCH�۾�������
                            , BAT_WRK_MSG_CNTN                                                      -- BATCH�۾��޽�������
                            , BAT_IPT_PRMT_CNTN                                                     -- BATCH�Է��Ķ���ͳ���
                            )
                       SELECT IN_BAT_WRK_ID                                                         -- BATCH�۾�ID
                            , V_CED                                                                 -- BATCH�۾���������
                            , IN_BAT_WRK_BGN_DTLDTM                                                 -- BATCH�۾����ۻ��Ͻ�
                            , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'                           -- ETL�۾��Ͻ�
                            , IN_BAT_WRK_NM                                                         -- BATCH�۾���
                            , IN_BAT_WRK_STEP_SNO                                                   -- BATCH�۾��ܰ��Ϸù�ȣ
                            , SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17)     -- BATCH�۾�������Ͻ�
                            , ' '                                                                   -- BATCH�۾��ð�
                            , 0                                                                     -- BATCH�۾�ó���Ǽ�
                            , 'R'                                                                   -- BATCH�۾�����ڵ�
                            , NVL(IN_BAT_WRK_ERR_VAL,' ')                                           -- BATCH�۾�������
                            , NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                                       -- BATCH�۾��޽�������
                            , NVL(IN_BAT_IPT_PRMT_CNTN,' ')                                         -- BATCH�Է��Ķ���ͳ���
                            ;

             ELSIF V_CNT = 1 AND IN_JOB_END_YN = 'N' THEN

             /* SCM_BATCH�۾��α�_������                   */
             /* ��ġ���α׷����� �߰��۾��� ���� ���� �α� */


                       UPDATE ansor.SCM_BAT_WRK_LOG_M                                               -- SCM_BATCH�۾��α�_������
                          SET BAT_WRK_NM = IN_BAT_WRK_NM                                            -- BATCH�۾���
                            , BAT_WRK_STEP_SNO = IN_BAT_WRK_STEP_SNO                                -- BATCH�۾��ܰ��Ϸù�ȣ
                            , BAT_WRK_BGN_DTLDTM = IN_BAT_WRK_BGN_DTLDTM                            -- BATCH�۾����ۻ��Ͻ�
                            , ETL_WRK_DTM = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'             -- ETL�۾��Ͻ�
                            , BAT_WRK_END_DTLDTM = SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17) -- BATCH�۾�������Ͻ�
                            , BAT_WRK_TM = V_RUNTIME_1                                              -- BATCH�۾��ð�
                            , BAT_WRK_PRCS_CNT = IN_BAT_WRK_PRCS_CNT                                -- BATCH�۾�ó���Ǽ�
                            , BAT_WRK_RLT_CD = 'R'                                                  -- BATCH�۾�����ڵ�
                            , BAT_WRK_ERR_VAL = NVL(IN_BAT_WRK_ERR_VAL,' ')                         -- BATCH�۾�������
                            , BAT_WRK_MSG_CNTN = NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                    -- BATCH�۾��޽�������
                        WHERE BAT_WRK_ID = IN_BAT_WRK_ID                                            -- BATCH�۾�ID
                          AND BAT_WRK_CRI_YMD = V_CED;                                              -- BATCH�۾���������

             ELSIF V_CNT = 1 AND IN_JOB_END_YN = 'Y' THEN

             /* SCM_BATCH�۾��α�_������                   */
             /* ��ġ���α׷����� �����۾��� ���� ���� �α� */


                      UPDATE ansor.SCM_BAT_WRK_LOG_M                                                -- SCM_BATCH�۾��α�_������
                         SET BAT_WRK_NM = IN_BAT_WRK_NM                                             -- BATCH�۾���
                           , BAT_WRK_STEP_SNO = IN_BAT_WRK_STEP_SNO                                 -- BATCH�۾��ܰ��Ϸù�ȣ
                           , BAT_WRK_BGN_DTLDTM = IN_BAT_WRK_BGN_DTLDTM                             -- BATCH�۾����ۻ��Ͻ�
                           , ETL_WRK_DTM = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'              -- ETL�۾��Ͻ�
                           , BAT_WRK_END_DTLDTM = SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17) -- BATCH�۾�������Ͻ�
                           , BAT_WRK_TM = V_RUNTIME_1                                               -- BATCH�۾��ð�
                           , BAT_WRK_PRCS_CNT = IN_BAT_WRK_PRCS_CNT                                 -- BATCH�۾�ó���Ǽ�
                           , BAT_WRK_RLT_CD = 'S'                                                   -- BATCH�۾�����ڵ�
                           , BAT_WRK_ERR_VAL = NVL(IN_BAT_WRK_ERR_VAL,' ')                          -- BATCH�۾�������
                           , BAT_WRK_MSG_CNTN = NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                     -- BATCH�۾��޽�������
                       WHERE BAT_WRK_ID = IN_BAT_WRK_ID                                             -- BATCH�۾�ID
                         AND BAT_WRK_CRI_YMD = V_CED;                                               -- BATCH�۾���������

             END IF;

        /*------------------------------------------------------------------------*/
        /* SCM_BAT_WRK_LOG_D STEP ���� INSERT                                            */
        /*------------------------------------------------------------------------*/

        /* SCM_BATCH�۾��α�_��                     */
        /* ��ġ���α׷����� STEP�� ���� ���� �α�     */


        INSERT
          INTO ansor.SCM_BAT_WRK_LOG_D                                                              -- SCM_BATCH�۾��α�_��
             ( BAT_WRK_ID                                                                           -- BATCH�۾�ID
             , BAT_WRK_CRI_YMD                                                                      -- BATCH�۾���������
             , BAT_WRK_BGN_DTLDTM                                                                   -- BATCH�۾����ۻ��Ͻ�
             , BAT_WRK_STEP_SNO                                                                     -- BATCH�۾��ܰ��Ϸù�ȣ
             , ETL_WRK_DTM                                                                          -- ETL�۾��Ͻ�
             , BAT_WRK_NM                                                                           -- BATCH�۾���
             , BAT_UNT_WRK_BGN_DTLDTM                                                               -- BATCH�����۾����ۻ��Ͻ�
             , BAT_UNT_WRK_END_DTLDTM                                                               -- BATCH�����۾�������Ͻ�
             , BAT_WRK_TM                                                                           -- BATCH�۾��ð�
             , BAT_WRK_PRCS_CNT                                                                     -- BATCH�۾�ó���Ǽ�
             , BAT_WRK_RLT_CD                                                                       -- BATCH�۾�����ڵ�
             , BAT_WRK_ERR_VAL                                                                      -- BATCH�۾�������
             , BAT_WRK_MSG_CNTN                                                                     -- BATCH�۾��޽�������
             )
        SELECT IN_BAT_WRK_ID                                                                        -- BATCH�۾�ID
             , V_CED                                                                                -- BATCH�۾���������
             , IN_BAT_WRK_BGN_DTLDTM                                                                -- BATCH�۾����ۻ��Ͻ�
             , IN_BAT_WRK_STEP_SNO                                                                  -- BATCH�۾��ܰ��Ϸù�ȣ
             , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'                                          -- ETL�۾��Ͻ�
             , IN_BAT_WRK_NM                                                                        -- BATCH�۾���
             , IN_BAT_UNT_WRK_BGN_DTLDTM                                                            -- BATCH�����۾����ۻ��Ͻ�
             , SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17) -- BATCH�����۾�������Ͻ�
             , V_RUNTIME                                                                            -- BATCH�۾��ð�
             , IN_BAT_WRK_PRCS_CNT                                                                  -- BATCH�۾�ó���Ǽ�
             , 'S'                                                                                  -- BATCH�۾�����ڵ�
             , NVL(IN_BAT_WRK_ERR_VAL,' ')                                                          -- BATCH�۾�������
             , NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                                                      -- BATCH�۾��޽�������
             ;

        --COMMIT;


    /*----------------------------------------------------------------------------*/
    /*--------------------------------���� �����α�-------------------------------*/
    /*----------------------------------------------------------------------------*/
    ELSE

           /* SCM_BATCH�۾��α�_������                   */
           /* ��ġ���α׷� �α����̺� ���翩��           */

           SELECT COUNT(1)
             INTO V_CNT
             FROM ansor.SCM_BAT_WRK_LOG_M                                                           -- SCM_BATCH�۾��α�_������
            WHERE BAT_WRK_ID       = IN_BAT_WRK_ID                                                  -- BATCH�۾�ID
              AND BAT_WRK_CRI_YMD  = V_CED;                                                         -- BATCH�۾���������

           IF V_CNT = 0 THEN

           /* SCM_BATCH�۾��α�_������                   */
           /* ��ġ���α׷����� �����۾��� ���� ���� �α� */

                      INSERT
                        INTO ansor.SCM_BAT_WRK_LOG_M                                                -- SCM_BATCH�۾��α�_������
                           ( BAT_WRK_ID                                                             -- BATCH�۾�ID
                           , BAT_WRK_CRI_YMD                                                        -- BATCH�۾���������
                           , BAT_WRK_BGN_DTLDTM                                                     -- BATCH�۾����ۻ��Ͻ�
                           , ETL_WRK_DTM                                                            -- ETL�۾��Ͻ�
                           , BAT_WRK_NM                                                             -- BATCH�۾���
                           , BAT_WRK_STEP_SNO                                                       -- BATCH�۾��ܰ��Ϸù�ȣ
                           , BAT_WRK_END_DTLDTM                                                     -- BATCH�۾�������Ͻ�
                           , BAT_WRK_TM                                                             -- BATCH�۾��ð�
                           , BAT_WRK_PRCS_CNT                                                       -- BATCH�۾�ó���Ǽ�
                           , BAT_WRK_RLT_CD                                                         -- BATCH�۾�����ڵ�
                           , BAT_WRK_ERR_VAL                                                        -- BATCH�۾�������
                           , BAT_WRK_MSG_CNTN                                                       -- BATCH�۾��޽�������
                           , BAT_IPT_PRMT_CNTN                                                      -- BATCH�Է��Ķ���ͳ���
                           )
                      SELECT IN_BAT_WRK_ID                                                          -- BATCH�۾�ID
                           , V_CED                                                                  -- BATCH�۾���������
                           , IN_BAT_WRK_BGN_DTLDTM                                                  -- BATCH�۾����ۻ��Ͻ�
                           , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'                            -- ETL�۾��Ͻ�
                           , IN_BAT_WRK_NM                                                          -- BATCH�۾���
                           , IN_BAT_WRK_STEP_SNO                                                    -- BATCH�۾��ܰ��Ϸù�ȣ
                           , SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17)      -- BATCH�۾�������Ͻ�
                           , V_RUNTIME_1                                                            -- BATCH�۾��ð�
                           , 0                                                                      -- BATCH�۾�ó���Ǽ�
                           , 'E'                                                                    -- BATCH�۾�����ڵ�
                           , NVL(IN_BAT_WRK_ERR_VAL,' ')                                            -- BATCH�۾�������
                           , NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                                        -- BATCH�۾��޽�������
                           , NVL(IN_BAT_IPT_PRMT_CNTN,' ')                                          -- BATCH�Է��Ķ���ͳ���
                           ;

           ELSIF IN_JOB_END_YN = 'Y' THEN

           /* SCM_BATCH�۾��α�_������                   */
           /* ��ġ���α׷����� �����۾��� ���� ���� �α� */


                       UPDATE ansor.SCM_BAT_WRK_LOG_M                                               -- SCM_BATCH�۾��α�_������
                          SET BAT_WRK_NM             = IN_BAT_WRK_NM                                -- BATCH�۾���
                            , BAT_WRK_STEP_SNO       = IN_BAT_WRK_STEP_SNO                          -- BATCH�۾��ܰ��Ϸù�ȣ
                            , BAT_WRK_BGN_DTLDTM  = IN_BAT_WRK_BGN_DTLDTM                           -- BATCH�۾����ۻ��Ͻ�
                            , ETL_WRK_DTM = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'             -- ETL�۾��Ͻ�
                            , BAT_WRK_END_DTLDTM  = SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17) -- BATCH�۾�������Ͻ�
                            , BAT_WRK_TM       = V_RUNTIME_1                                        -- BATCH�۾��ð�
                            , BAT_WRK_PRCS_CNT = 0                                                  -- BATCH�۾�ó���Ǽ�
                            , BAT_WRK_RLT_CD   = 'E'                                                -- BATCH�۾�����ڵ�
                            , BAT_WRK_ERR_VAL  = NVL(IN_BAT_WRK_ERR_VAL,' ')                        -- BATCH�۾�������
                            , BAT_WRK_MSG_CNTN = NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                    -- BATCH�۾��޽�������
                        WHERE BAT_WRK_ID       = IN_BAT_WRK_ID                                      -- BATCH�۾�ID
                          AND BAT_WRK_CRI_YMD  = V_CED;                                             -- BATCH�۾���������

           END IF;

           /* SCM_BATCH�۾��α�_��                     */
           /* ��ġ���α׷����� STEP�� ���� ���� �α�     */


           INSERT
             INTO ansor.SCM_BAT_WRK_LOG_D                                                           -- SCM_BATCH�۾��α�_��
                ( BAT_WRK_ID                                                                        -- BATCH�۾�ID
                , BAT_WRK_CRI_YMD                                                                   -- BATCH�۾���������
                , BAT_WRK_BGN_DTLDTM                                                                -- BATCH�۾����ۻ��Ͻ�
                , BAT_WRK_STEP_SNO                                                                  -- BATCH�۾��ܰ��Ϸù�ȣ
                , ETL_WRK_DTM                                                                       -- ETL�۾��Ͻ�
                , BAT_WRK_NM                                                                        -- BATCH�۾���
                , BAT_UNT_WRK_BGN_DTLDTM                                                            -- BATCH�����۾����ۻ��Ͻ�
                , BAT_UNT_WRK_END_DTLDTM                                                            -- BATCH�����۾�������Ͻ�
                , BAT_WRK_TM                                                                        -- BATCH�۾��ð�
                , BAT_WRK_PRCS_CNT                                                                  -- BATCH�۾�ó���Ǽ�
                , BAT_WRK_RLT_CD                                                                    -- BATCH�۾�����ڵ�
                , BAT_WRK_ERR_VAL                                                                   -- BATCH�۾�������
                , BAT_WRK_MSG_CNTN                                                                  -- BATCH�۾��޽�������
                )
           SELECT IN_BAT_WRK_ID                                                                     -- BATCH�۾�ID
                , V_CED                                                                             -- BATCH�۾���������
                , IN_BAT_WRK_BGN_DTLDTM                                                             -- BATCH�۾����ۻ��Ͻ�
                , IN_BAT_WRK_STEP_SNO                                                               -- BATCH�۾��ܰ��Ϸù�ȣ
                , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'                                       -- ETL�۾��Ͻ�
                , IN_BAT_WRK_NM                                                                     -- BATCH�۾���
                , IN_BAT_UNT_WRK_BGN_DTLDTM                                                         -- BATCH�����۾����ۻ��Ͻ�
                , SUBSTRING(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDDHH24MISSUS'),1,17)                 -- BATCH�����۾�������Ͻ�
                , V_RUNTIME                                                                         -- BATCH�۾��ð�
                , 0                                                                                 -- BATCH�۾�ó���Ǽ�
                , 'E'                                                                               -- BATCH�۾�����ڵ�
                , NVL(IN_BAT_WRK_ERR_VAL,' ')                                                       -- BATCH�۾�������
                , NVL(IN_BAT_WRK_MSG_CNTN   ,' ')                                                   -- BATCH�۾��޽�������
                ;

             --COMMIT;

    END IF;

OUT_MSG := END_DTM;

END;
$$;