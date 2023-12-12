BEGIN
              O_COUNT :\u003d 0;

              DELETE FROM ADMOWN.WATCO_TBTRO_M
                  WHERE OWNER_NM IN (\u0027STGOWN\u0027,\u0027SOROWN\u0027)
                         AND LENGTH(TBL_NM) \u003d 13
                 ;

              DBMS_OUTPUT.PUT_LINE(\u0027DELETE COUNT : \u0027 || SQL%ROWCOUNT);

             COMMIT;

             FOR TBL IN
             (
                SELECT
                                  OWNER
                                 ,TABLE_NAME
                     FROM ALL_TABLES
                 WHERE OWNER IN (\u0027STGOWN\u0027,\u0027SOROWN\u0027)
                       AND STATUS \u003d \u0027VALID\u0027
                       AND LENGTH(TABLE_NAME) \u003d 13
                 ORDER BY OWNER, TABLE_NAME
            )
            LOOP
                         INSERT INTO ADMOWN.WATCO_TBTRO_M VALUES (TBL.OWNER,TBL.TABLE_NAME,TO_CHAR(SYSTIMESTAMP, \u0027YYYYMMDDHH24MISSFF2\u0027));

                         O_COUNT :\u003d O_COUNT + SQL%ROWCOUNT;
               END LOOP;

               DBMS_OUTPUT_PUT_LINE(\u0027INSERT COUNT : \u0027 || O_COUNT);

               COMMIT;
END;