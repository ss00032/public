CREATE OR REPLACE PROCEDURE PMPOWN.SP_INDEX_REBUILD(
                            MSGCODE         OUT VARCHAR2,                                           /* OUT ����ڵ� */
                            IN_TBL_OWR      IN  VARCHAR2,                                           /* IN ���̺���� */
                            IN_TBL_NM       IN  VARCHAR2                                            /* IN ���̺�� */
                            ) IS
/*------------------------------------------------------------------------------------------------*/
/*  ���ν�����   : SP_INDEX_REBUILD                                                               */
/*  ����  ����   : ������DB EXC ���̺� REBUILD�� ���� PROCEDURE                                   */
/*                                                                                                */
/*  INPUT        : ���̺����, ���̺��                                                           */
/*  OUTPUT       : ����ڵ�                                                                       */
/*  ��   ��   �� : �츮ī�� ������DB��                                                            */
/*  �� ��  �� �� : 2022. 09. 01                                                                   */
/*  �� ��  �� �� :                                                                                */
/*------------------------------------------------------------------------------------------------*/
/* -- ��뿹��
  DECLARE
    MSGCODE    VARCHAR2(200);
    IN_TBL_OWR VARCHAR2(200) := 'PMPOWN';
    IN_TBL_NM  VARCHAR2(200) := 'TEST1';
  BEGIN
    SP_INDEX_REBUILD ( MSGCODE, IN_TBL_OWR, IN_TBL_NM );
  END;

 -- PROCEDURE��  ��뿹 :
    SP_INDEX_REBUILD (P_RTN,P_TAR_OWNER,P_TAR_TBL_ID1);

    IF P_RTN <> '0' THEN
       P_SSU_CD         := '1';
       P_BAT_ERO_CD     := P_RTN;
       P_BAT_ERO_MSG_TT := 'EXC TABLE CREATION ERROR';
       RAISE USER_EXCEPTION;
    END IF;
*/


    V_WFG_CD           VARCHAR2(2) := '01';                                                         /* �׷쳻����ڵ� */
    V_PROCESS_NO       VARCHAR2(1);                                                                 /* ���μ�����ȣ */
    V_TABLE_EXIST_YN   VARCHAR2(1);                                                                 /* ���̺����翩�� */
    V_PT_EXIST_YN      VARCHAR2(1);                                                                 /* ��Ƽ�����翩�� */
    V_PT_EXIST_CNT     NUMBER       := 0;                                                           /* ��Ƽ�Ǽ� */
    V_IND_EXIST_YN     VARCHAR2(1);                                                                 /* �ε������翩�� */
    V_IX_IND_EXIST_YN  VARCHAR2(1);                                                                 /* IX �ε������翩�� */
    V_IND_EXIST_CNT    NUMBER       := 0;                                                           /* �ε����� */
    V_IX_IND_EXIST_CNT NUMBER       := 0;                                                           /* IX �ε����� */
    V_IX_NUM           NUMBER := 1;                                                                 /* IX �ε��� �ݺ��� */
    L_SQL              VARCHAR2(3000);                                                              /* ����SQL */
    L_SQLERRM          VARCHAR2(3000);                                                              /* SQL�����޽��� */
    V_RSLT_MSG         VARCHAR2(3000);                                                              /* �������޽��� */
    LOCK_CONFLICT      EXCEPTION;                                                                   /* LOCK���� */
    NO_INDEX_NAME      EXCEPTION;                                                                   /* INDEX NOT FOUND ���� */
    V_UPPER_TBL_OWR    VARCHAR2(20);                                                                /* TABLE OWNER �빮�� */
    V_UPPER_TBL_NM     VARCHAR2(50);                                                                /* ���̺�� �빮�� */

BEGIN

    V_UPPER_TBL_OWR := UPPER(IN_TBL_OWR);                                                           /* INPUT ���̺���� �빮��ó�� */
    V_UPPER_TBL_NM  := UPPER(IN_TBL_NM);                                                            /* INPUT ���̺�� �빮��ó�� */

    /*--------------------------------------------------------------------------------------------*/
    /*  EXC ���̺� �������� Ȯ��                                                                  */
    /*--------------------------------------------------------------------------------------------*/
    V_PROCESS_NO := '1';                                                                            /* ���μ�����ȣ(1:EXC ���̺� ����üũ) */

    SELECT 'Y'
      INTO V_TABLE_EXIST_YN                                                                         /* ���̺����翩�� */
      FROM ALL_TABLES                                                                               /* ALL_TABLES */
     WHERE OWNER      = V_UPPER_TBL_OWR                                                             /* OWNER�� */
       AND TABLE_NAME = V_UPPER_TBL_NM                                                              /* ���̺�� */
          ;

    /*--------------------------------------------------------------------------------------------*/
    /*  �ε��� ���� ���� Ȯ��                                                                     */
    /*--------------------------------------------------------------------------------------------*/
    V_PROCESS_NO := '2';                                                                            /* ���μ�����ȣ(2:��Ƽ�����翩��üũ ���μ���) */

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
          INTO V_IND_EXIST_YN                                                                       /* �ε������翩�� ���� */
          FROM ALL_INDEXES                                                                          /* ALL_TAB_PARTITIONS */
         WHERE TABLE_OWNER    = V_UPPER_TBL_OWR                                                     /* OWNER�� */
           AND TABLE_NAME     = V_UPPER_TBL_NM                                                      /* ���̺�� */
           AND INDEX_NAME LIKE '%UX_' || V_UPPER_TBL_NM || '_01%'
              ;
    END IF;

    IF V_IX_IND_EXIST_CNT = 0 THEN                                                                  /* INDEX = 0 */
        V_IX_IND_EXIST_YN := 'N';
    ELSE
        SELECT 'Y'
          INTO V_IX_IND_EXIST_YN                                                                    /* �ε������翩�� ���� */
          FROM ALL_INDEXES                                                                          /* ALL_TAB_PARTITIONS */
         WHERE TABLE_OWNER    = V_UPPER_TBL_OWR                                                     /* OWNER�� */
           AND TABLE_NAME     = V_UPPER_TBL_NM                                                      /* ���̺�� */
           AND INDEX_NAME LIKE '%IX_' || V_UPPER_TBL_NM || '_N01%'
              ;
    END IF;
    /*--------------------------------------------------------------------------------------------*/
    /*  REBUILD ����                                                                              */
    /*--------------------------------------------------------------------------------------------*/
    V_PROCESS_NO := '3';                                                                            /* ���μ�����ȣ(3:��Ƽ��TRUNCATE���� ���μ���) */

    CASE WHEN V_IND_EXIST_YN = 'Y' THEN                                                             /* �ε����� ������ ��� */
              L_SQL :='ALTER INDEX ' || V_UPPER_TBL_OWR || '.UX_' || V_UPPER_TBL_NM || '_01 REBUILD';
              EXECUTE IMMEDIATE L_SQL;
         ELSE
              DBMS_OUTPUT.PUT_LINE('UX �ε��� ������');
    END CASE;
    CASE WHEN V_IX_IND_EXIST_YN = 'Y' THEN                                                             /* IX �ε����� ������ ��� */
              WHILE(V_IX_NUM <= V_IX_IND_EXIST_CNT)
              LOOP
                  L_SQL :='ALTER INDEX ' || V_UPPER_TBL_OWR || '.IX_' || V_UPPER_TBL_NM || '_N' || TO_CHAR(V_IX_NUM,'FM00') ||' REBUILD';
                  EXECUTE IMMEDIATE L_SQL;
                  V_IX_NUM := V_IX_NUM+1;
              END LOOP
              ;
         ELSE
              DBMS_OUTPUT.PUT_LINE('IX �ε��� ������');
    END CASE;
    IF SQLCODE = -54 THEN
        RAISE LOCK_CONFLICT;                                                                        /* ���̺� LOCK EXCEPTION */
    END IF;
    MSGCODE    := SQLCODE;                                                                          /* SQLCODE */
    V_RSLT_MSG := '���� ����';                                                                      /* ������(��������) */
EXCEPTION
    WHEN NO_INDEX_NAME THEN
         MSGCODE    := 'ERR';
         L_SQLERRM  := SQLERRM;
         V_RSLT_MSG := '(' || V_UPPER_TBL_NM || ')���̺��� INDEX ������';
    WHEN LOCK_CONFLICT THEN
         MSGCODE    := SQLCODE;
         L_SQLERRM  := SQLERRM;
         V_RSLT_MSG := '(' || V_UPPER_TBL_NM || ')���̺� LOCK';
         COMMIT;
    WHEN OTHERS THEN
         MSGCODE   := SQLCODE;
         L_SQLERRM := SQLERRM;
RAISE;
         IF V_PROCESS_NO = '1' THEN
             V_RSLT_MSG := '(' || V_UPPER_TBL_NM || ')EXC_���̺� ������';
         ELSIF V_PROCESS_NO = '2' THEN
             V_RSLT_MSG := '(' || V_UPPER_TBL_NM || ')INDEX ������';
         ELSIF V_PROCESS_NO = '3' THEN
             V_RSLT_MSG := '(' || V_UPPER_TBL_NM || ')REBUILD ���� �� ����';
         END IF;
END;
/

GRANT EXECUTE ON PMPOWN.SP_INDEX_REBUILD TO RL_PMP_IUD;
CREATE OR REPLACE PUBLIC SYNONYM SP_INDEX_REBUILD FOR PMPOWN.SP_INDEX_REBUILD;