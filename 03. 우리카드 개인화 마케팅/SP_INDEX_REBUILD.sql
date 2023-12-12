CREATE OR REPLACE PROCEDURE PMPOWN.SP_INDEX_REBUILD(
                            MSGCODE         OUT VARCHAR2,                                           /* OUT 결과코드 */
                            IN_TBL_OWR      IN  VARCHAR2,                                           /* IN 테이블오너 */
                            IN_TBL_NM       IN  VARCHAR2                                            /* IN 테이블명 */
                            ) IS
/*------------------------------------------------------------------------------------------------*/
/*  프로시저명   : SP_INDEX_REBUILD                                                               */
/*  내용  설명   : 마케팅DB EXC 테이블 REBUILD를 위한 PROCEDURE                                   */
/*                                                                                                */
/*  INPUT        : 테이블오너, 테이블명                                                           */
/*  OUTPUT       : 결과코드                                                                       */
/*  작   성   자 : 우리카드 마케팅DB팀                                                            */
/*  작 성  일 자 : 2022. 09. 01                                                                   */
/*  수 정  내 역 :                                                                                */
/*------------------------------------------------------------------------------------------------*/
/* -- 사용예제
  DECLARE
    MSGCODE    VARCHAR2(200);
    IN_TBL_OWR VARCHAR2(200) := 'PMPOWN';
    IN_TBL_NM  VARCHAR2(200) := 'TEST1';
  BEGIN
    SP_INDEX_REBUILD ( MSGCODE, IN_TBL_OWR, IN_TBL_NM );
  END;

 -- PROCEDURE내  사용예 :
    SP_INDEX_REBUILD (P_RTN,P_TAR_OWNER,P_TAR_TBL_ID1);

    IF P_RTN <> '0' THEN
       P_SSU_CD         := '1';
       P_BAT_ERO_CD     := P_RTN;
       P_BAT_ERO_MSG_TT := 'EXC TABLE CREATION ERROR';
       RAISE USER_EXCEPTION;
    END IF;
*/


    V_WFG_CD           VARCHAR2(2) := '01';                                                         /* 그룹내기관코드 */
    V_PROCESS_NO       VARCHAR2(1);                                                                 /* 프로세스번호 */
    V_TABLE_EXIST_YN   VARCHAR2(1);                                                                 /* 테이블존재여부 */
    V_PT_EXIST_YN      VARCHAR2(1);                                                                 /* 파티션존재여부 */
    V_PT_EXIST_CNT     NUMBER       := 0;                                                           /* 파티션수 */
    V_IND_EXIST_YN     VARCHAR2(1);                                                                 /* 인덱스존재여부 */
    V_IX_IND_EXIST_YN  VARCHAR2(1);                                                                 /* IX 인덱스존재여부 */
    V_IND_EXIST_CNT    NUMBER       := 0;                                                           /* 인덱스수 */
    V_IX_IND_EXIST_CNT NUMBER       := 0;                                                           /* IX 인덱스수 */
    V_IX_NUM           NUMBER := 1;                                                                 /* IX 인덱스 반복수 */
    L_SQL              VARCHAR2(3000);                                                              /* 실행SQL */
    L_SQLERRM          VARCHAR2(3000);                                                              /* SQL오류메시지 */
    V_RSLT_MSG         VARCHAR2(3000);                                                              /* 수행결과메시지 */
    LOCK_CONFLICT      EXCEPTION;                                                                   /* LOCK오류 */
    NO_INDEX_NAME      EXCEPTION;                                                                   /* INDEX NOT FOUND 오류 */
    V_UPPER_TBL_OWR    VARCHAR2(20);                                                                /* TABLE OWNER 대문자 */
    V_UPPER_TBL_NM     VARCHAR2(50);                                                                /* 테이블명 대문자 */

BEGIN

    V_UPPER_TBL_OWR := UPPER(IN_TBL_OWR);                                                           /* INPUT 테이블오너 대문자처리 */
    V_UPPER_TBL_NM  := UPPER(IN_TBL_NM);                                                            /* INPUT 테이블명 대문자처리 */

    /*--------------------------------------------------------------------------------------------*/
    /*  EXC 테이블 생성여부 확인                                                                  */
    /*--------------------------------------------------------------------------------------------*/
    V_PROCESS_NO := '1';                                                                            /* 프로세스번호(1:EXC 테이블 존재체크) */

    SELECT 'Y'
      INTO V_TABLE_EXIST_YN                                                                         /* 테이블존재여부 */
      FROM ALL_TABLES                                                                               /* ALL_TABLES */
     WHERE OWNER      = V_UPPER_TBL_OWR                                                             /* OWNER명 */
       AND TABLE_NAME = V_UPPER_TBL_NM                                                              /* 테이블명 */
          ;

    /*--------------------------------------------------------------------------------------------*/
    /*  인덱스 존재 여부 확인                                                                     */
    /*--------------------------------------------------------------------------------------------*/
    V_PROCESS_NO := '2';                                                                            /* 프로세스번호(2:파티션존재여부체크 프로세스) */

    SELECT COUNT(1)
      INTO V_IND_EXIST_CNT
      FROM ALL_INDEXES
     WHERE OWNER      = V_UPPER_TBL_OWR
       AND TABLE_NAME = V_UPPER_TBL_NM
       AND INDEX_NAME LIKE '%UX_' || V_UPPER_TBL_NM || '_01%'
    ;

    SELECT COUNT(1)
      INTO V_IX_IND_EXIST_CNT
      FROM ALL_INDEXES
     WHERE OWNER      = V_UPPER_TBL_OWR
       AND TABLE_NAME = V_UPPER_TBL_NM
       AND INDEX_NAME LIKE '%IX_' || V_UPPER_TBL_NM || '_N%'
    ;

    IF V_IND_EXIST_CNT = 0 THEN                                                                     /* INDEX = 0 */
       V_IND_EXIST_YN := 'N';
    ELSE
        SELECT 'Y'
          INTO V_IND_EXIST_YN                                                                       /* 인덱스존재여부 셋팅 */
          FROM ALL_INDEXES                                                                          /* ALL_TAB_PARTITIONS */
         WHERE TABLE_OWNER    = V_UPPER_TBL_OWR                                                     /* OWNER명 */
           AND TABLE_NAME     = V_UPPER_TBL_NM                                                      /* 테이블명 */
           AND INDEX_NAME LIKE '%UX_' || V_UPPER_TBL_NM || '_01%'
              ;
    END IF;

    IF V_IX_IND_EXIST_CNT = 0 THEN                                                                  /* INDEX = 0 */
        V_IX_IND_EXIST_YN := 'N';
    ELSE
        SELECT 'Y'
          INTO V_IX_IND_EXIST_YN                                                                    /* 인덱스존재여부 셋팅 */
          FROM ALL_INDEXES                                                                          /* ALL_TAB_PARTITIONS */
         WHERE TABLE_OWNER    = V_UPPER_TBL_OWR                                                     /* OWNER명 */
           AND TABLE_NAME     = V_UPPER_TBL_NM                                                      /* 테이블명 */
           AND INDEX_NAME LIKE '%IX_' || V_UPPER_TBL_NM || '_N01%'
              ;
    END IF;
    /*--------------------------------------------------------------------------------------------*/
    /*  REBUILD 실행                                                                              */
    /*--------------------------------------------------------------------------------------------*/
    V_PROCESS_NO := '3';                                                                            /* 프로세스번호(3:파티션TRUNCATE실행 프로세스) */

    CASE WHEN V_IND_EXIST_YN = 'Y' THEN                                                             /* 인덱스가 존재할 경우 */
              L_SQL :='ALTER INDEX ' || V_UPPER_TBL_OWR || '.UX_' || V_UPPER_TBL_NM || '_01 REBUILD';
              EXECUTE IMMEDIATE L_SQL;
         ELSE
              DBMS_OUTPUT.PUT_LINE('UX 인덱스 미존재');
    END CASE;
    CASE WHEN V_IX_IND_EXIST_YN = 'Y' THEN                                                             /* IX 인덱스가 존재할 경우 */
              WHILE(V_IX_NUM <= V_IX_IND_EXIST_CNT)
              LOOP
                  L_SQL :='ALTER INDEX ' || V_UPPER_TBL_OWR || '.IX_' || V_UPPER_TBL_NM || '_N' || TO_CHAR(V_IX_NUM,'FM00') ||' REBUILD';
                  EXECUTE IMMEDIATE L_SQL;
                  V_IX_NUM := V_IX_NUM+1;
              END LOOP
              ;
         ELSE
              DBMS_OUTPUT.PUT_LINE('IX 인덱스 미존재');
    END CASE;
    IF SQLCODE = -54 THEN
        RAISE LOCK_CONFLICT;                                                                        /* 테이블 LOCK EXCEPTION */
    END IF;
    MSGCODE    := SQLCODE;                                                                          /* SQLCODE */
    V_RSLT_MSG := '정상 종료';                                                                      /* 수행결과(정상종료) */
EXCEPTION
    WHEN NO_INDEX_NAME THEN
         MSGCODE    := 'ERR';
         L_SQLERRM  := SQLERRM;
         V_RSLT_MSG := '(' || V_UPPER_TBL_NM || ')테이블은 INDEX 미존재';
    WHEN LOCK_CONFLICT THEN
         MSGCODE    := SQLCODE;
         L_SQLERRM  := SQLERRM;
         V_RSLT_MSG := '(' || V_UPPER_TBL_NM || ')테이블 LOCK';
         COMMIT;
    WHEN OTHERS THEN
         MSGCODE   := SQLCODE;
         L_SQLERRM := SQLERRM;
RAISE;
         IF V_PROCESS_NO = '1' THEN
             V_RSLT_MSG := '(' || V_UPPER_TBL_NM || ')EXC_테이블 미존재';
         ELSIF V_PROCESS_NO = '2' THEN
             V_RSLT_MSG := '(' || V_UPPER_TBL_NM || ')INDEX 미존재';
         ELSIF V_PROCESS_NO = '3' THEN
             V_RSLT_MSG := '(' || V_UPPER_TBL_NM || ')REBUILD 수행 중 오류';
         END IF;
END;
/

GRANT EXECUTE ON PMPOWN.SP_INDEX_REBUILD TO RL_PMP_IUD;
CREATE OR REPLACE PUBLIC SYNONYM SP_INDEX_REBUILD FOR PMPOWN.SP_INDEX_REBUILD;