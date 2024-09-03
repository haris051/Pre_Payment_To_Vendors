drop procedure if Exists PROC_PRE_PAYMENT_TO_VENDOR;
DELIMITER $$
CREATE PROCEDURE `PROC_PRE_PAYMENT_TO_VENDOR`( P_VENDOR_ID TEXT,
											  P_ENTRY_DATE_FROM DATETIME,
											  P_ENTRY_DATE_TO DATETIME,
											  P_COMPANY_ID INT,
											  P_START INT,
											  P_LENGTH INT )
BEGIN
	SET @QRY = CONCAT(' SELECT CASE
									WHEN E.PAYMENT_SENT_ID IS NULL THEN NULL
									ELSE E.VENDOR_ID
							   END ''Vendor'',
							   CASE
									WHEN E.PAYMENT_SENT_ID IS NULL THEN NULL
									ELSE DATE_FORMAT(E.PS_ENTRY_DATE, ''%m-%d-%Y'')
							   END ''Date of Transaction'',
							   CASE
									WHEN E.PAYMENT_SENT_ID IS NULL THEN NULL
									ELSE DATE_FORMAT(E.DUE_DATE, ''%m-%d-%Y'')
							   END ''Due Date'',
							   E.PAYMENT_SENT_ID AS ''Form ID'',
							   CASE
									WHEN E.PAYMENT_SENT_ID IS NULL THEN NULL
									ELSE E.PAYPAL_TRANSACTION_ID
							   END ''Reference'',
							   Round(cast(SUM(E.AMOUNT) as Decimal(22,2)),2) AS ''Amount'',
							   Round(cast(SUM(E.FORM_AMOUNT) as Decimal(22,2)),2) AS ''Net Amount'',
							   Round(cast(SUM(E.FORM_AMOUNT) as Decimal(22,2)),2) AS ''Amount Due'',
							   CASE
									WHEN E.PAYMENT_SENT_ID IS NULL THEN NULL
									ELSE E.TERM_NAME
							   END ''Term'',
							   CASE
									WHEN E.PAYMENT_SENT_ID IS NULL THEN NULL
									ELSE E.DEFAULT_NUMBER_OF_DAYS
							   END ''No of Days'',
							   CASE
									WHEN E.PAYMENT_SENT_ID IS NULL THEN NULL
									ELSE E.AGE
							   END ''Age'',
							   Round(cast(SUM(E.RANGE_1) as Decimal(22,2)),2) AS ''0 - 30'',
							   Round(cast(SUM(E.RANGE_2) as Decimal(22,2)),2) AS ''31 - 60'',
							   Round(cast(SUM(E.RANGE_3) as Decimal(22,2)),2) AS ''61 - 90'',
							   Round(cast(SUM(E.RANGE_4) as Decimal(22,2)),2) AS ''Over 90 Days'',
							   Round(cast((SUM(IFNULL(E.RANGE_1, 0)) + SUM(IFNULL(E.RANGE_2, 0)) + SUM(IFNULL(E.RANGE_3, 0)) + SUM(IFNULL(E.RANGE_4, 0))) as Decimal(22,2)),2) AS TOTAL,
                               COUNT(*) OVER() AS TOTAL_ROWS
						  FROM (SELECT B.VENDOR_ID,
									   C.PS_ENTRY_DATE,
									   DATE_ADD(C.PS_ENTRY_DATE, INTERVAL IF(B.DEFAULT_NUMBER_OF_DAYS > 0, B.DEFAULT_NUMBER_OF_DAYS - 1, B.DEFAULT_NUMBER_OF_DAYS) DAY) AS DUE_DATE,
									   C.PAYMENT_SENT_ID,
									   C.PAYPAL_TRANSACTION_ID,
									   C.AMOUNT,
									   (A.FORM_AMOUNT * -1) AS FORM_AMOUNT,
									   D.TERM_NAME,
									   B.DEFAULT_NUMBER_OF_DAYS,
									   DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) AS AGE,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) >= 0 AND DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) <= 30 THEN (A.FORM_AMOUNT * -1)
											ELSE NULL
									   END AS RANGE_1,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) >= 31 AND DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) <= 60 THEN (A.FORM_AMOUNT * -1)
											ELSE NULL
									   END AS RANGE_2,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) >= 61 AND DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) <= 90 THEN (A.FORM_AMOUNT * -1)
											ELSE NULL
									   END AS RANGE_3,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) >= 91 THEN (A.FORM_AMOUNT * -1)
											ELSE NULL
									   END AS RANGE_4
								  FROM ((((SELECT VENDOR_ID,
												  FORM_ID,
                                                  FORM_FLAG,
                                                  FORM_AMOUNT
											 FROM PAYMENTS_DETAIL_NEW
											WHERE IS_CONFLICTED_FULL = ''N''
                                              AND FORM_FLAG = ''P''
                                              AND CASE
													   WHEN \'',P_VENDOR_ID,'\' <> "" THEN VENDOR_ID = \'',P_VENDOR_ID,'\'
													   ELSE TRUE
												  END) A 
									   JOIN VENDOR B ON (A.VENDOR_ID = B.ID 
														 AND B.COMPANY_ID = \'',P_COMPANY_ID,'\'
														 AND CASE
																  WHEN \'',P_VENDOR_ID,'\' <> "" THEN B.ID = \'',P_VENDOR_ID,'\'
																  ELSE TRUE
															 END))
									   JOIN PAYMENT_SENT C ON (A.FORM_ID = C.ID 
															   AND A.FORM_FLAG = ''P''
															   AND C.COMPANY_ID = \'',P_COMPANY_ID,'\'
															   AND CASE
																		WHEN \'',P_VENDOR_ID,'\' <> "" THEN C.VENDOR_ID = \'',P_VENDOR_ID,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN C.PS_ENTRY_DATE >= \'',P_ENTRY_DATE_FROM,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN C.PS_ENTRY_DATE <= \'',P_ENTRY_DATE_TO,'\'
																		ELSE TRUE
																   END))
									   JOIN PAYMENT_TERMS D ON (B.PAYMENT_TERM_ID = D.ID))) E
					  GROUP BY E.VENDOR_ID, E.PAYMENT_SENT_ID, E.PS_ENTRY_DATE, E.DUE_DATE, E.PAYPAL_TRANSACTION_ID, E.TERM_NAME, E.AGE, E.DEFAULT_NUMBER_OF_DAYS WITH ROLLUP
					    HAVING (E.DEFAULT_NUMBER_OF_DAYS IS NOT NULL) OR E.PAYMENT_SENT_ID IS NULL
						 LIMIT ',P_START,', ',P_LENGTH,';');
    PREPARE STMP FROM @QRY;
    EXECUTE STMP ;
    DEALLOCATE PREPARE STMP;
END $$
DELIMITER ;
