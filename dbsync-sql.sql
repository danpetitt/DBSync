SELECT 
        COUNT(*) AS cnt, 
        CONCAT(
		SUM(CONV(SUBSTRING(@CRC:=SHA1(CONCAT_WS('/##/',nid,strname)),1,8),16,10)),
		SUM(CONV(SUBSTRING(@CRC, 9,8),16,10)),
		SUM(CONV(SUBSTRING(@CRC,17,8),16,10)),
		SUM(CONV(SUBSTRING(@CRC,25,8),16,10))
        ) AS sig 
FROM 
	channel;


SELECT
    COUNT(*) AS cnt,
    CONCAT(SUM(CONV(
        SUBSTRING(@CRC:=MD5(
            CONCAT_WS('/##/',`nscheduleid`,
                COALESCE(`strdescription`,"#NULL#"),
                COALESCE(`dtstart`,"#NULL#"),
                COALESCE(`dtend`,"#NULL#"))),1,8),16,10 )),
    SUM(CONV(SUBSTRING(@CRC, 9,8),16,10)),
    SUM(CONV(SUBSTRING(@CRC,17,8),16,10)),
    SUM(CONV(SUBSTRING(@CRC,25,8),16,10))
    ) AS sig
FROM
SCHEDULE;


SELECT LEFT(CONCAT(IF(`nid`<0,'-','+'), LPAD(ABS(`nid`),9,'0')),4),
   CONCAT(
      SUM(CONV(SUBSTRING(MD5(CONCAT_WS(",",nid,strname)),1,8),16,10)),
      SUM(CONV(SUBSTRING(MD5(CONCAT_WS(",",nid,strname)),9,8),16,10)),
      SUM(CONV(SUBSTRING(MD5(CONCAT_WS(",",nid,strname)),17,8),16,10)),
      SUM(CONV(SUBSTRING(MD5(CONCAT_WS(",",nid,strname)),25,8),16,10))
   )AS hashkey,
   COUNT(*)AS yog_cnt, `nid`
FROM channel
GROUP BY 1 ORDER BY 2;


SELECT 0 AS chunk_num, COUNT(*) AS cnt,	   LOWER(CONCAT(LPAD(CONV(BIT_XOR(CAST(CONV(SUBSTRING(@crc, 1, 16), 16, 10) AS UNSIGNED)), 10, 16)	   , 16, '0'), LPAD(CONV(BIT_XOR(CAST(CONV(SUBSTRING(@crc := MD5(CONCAT_WS('#', `nCategoryID`,`nID`,`strDescription`,`strEmail`,`strImagePath`,`strName`,`strPostalAddress`,`strTelephone`,`strWebSiteURL`,`nOrderingNo`,`bIncludeInLibrarySearch`,`strShortName`,`nCountryID`,`strCountryName`,`strShortCountryName`,`strWatchLiveURL`,CONCAT(ISNULL(`nCategoryID`),ISNULL(`strDescription`),ISNULL(`strEmail`),ISNULL(`strImagePath`),ISNULL(`strPostalAddress`),ISNULL(`strTelephone`),ISNULL(`strWebSiteURL`),ISNULL(`nOrderingNo`),ISNULL(`nCountryID`),ISNULL(`strCountryName`),ISNULL(`strShortCountryName`),ISNULL(`strWatchLiveURL`)))), 17, 16), 16, 10) AS UNSIGNED)) 	   , 10, 16), 16, '0'))) 	   AS crc  FROM  `channel`  WHERE (1 = 1);



SELECT SHA1( CONCAT( CAST( nID AS CHAR ), '-', CAST( strName AS CHAR ) ) ) AS KeyHash, nID,strName,dtBirth,dtDeath,bMale,strDescription FROM `exportpdb_new`.`person` WHERE SHA1( CONCAT_WS( '-', CAST( nID AS CHAR ), CAST( strName AS CHAR ), CAST( COALESCE( dtBirth, '#~NULL~#' ) AS CHAR ), CAST( COALESCE( dtDeath, '#~NULL~#' ) AS CHAR ), CAST( COALESCE( bMale, '#~NULL~#' ) AS CHAR ), CAST( COALESCE( strDescription, '#~NULL~#' ) AS CHAR ) ) ) IN (SELECT SHA1( CONCAT_WS( '-', CAST( nID AS CHAR ), CAST( strName AS CHAR ), CAST( COALESCE( dtBirth, '#~NULL~#' ) AS CHAR ), CAST( COALESCE( dtDeath, '#~NULL~#' ) AS CHAR ), CAST( COALESCE( bMale, '#~NULL~#' ) AS CHAR ), CAST( COALESCE( strDescription, '#~NULL~#' ) AS CHAR ) ) ) FROM `exportpdb_live`.`person`);

SELECT SHA1( CONCAT_WS( '-', nID, strName ) ) AS KeyHash, nID,strName,dtBirth,dtDeath,bMale,strDescription FROM `exportpdb_new`.`person` WHERE ( CONCAT_WS( '-', nID, strName, dtBirth, dtDeath, bMale, strDescription ) ) NOT IN (SELECT ( CONCAT_WS( '-', nID, strName, dtBirth, dtDeath, bMale, strDescription ) ) FROM `exportpdb_live`.`person`);


SELECT SHA1( CONCAT_WS( '-', `exportpdb_new`.`person`.nID, `exportpdb_new`.`person`.strName, `exportpdb_new`.`person`.dtBirth, `exportpdb_new`.`person`.dtDeath, `exportpdb_new`.`person`.bMale, `exportpdb_new`.`person`.strDescription ) ) AS SourceKeyHash, SHA1( CONCAT_WS( '-', `exportpdb_live`.`person`.nID, `exportpdb_live`.`person`.strName, `exportpdb_live`.`person`.dtBirth, `exportpdb_live`.`person`.dtDeath, `exportpdb_live`.`person`.bMale, `exportpdb_live`.`person`.strDescription ) ) AS TargetKeyHash FROM `exportpdb_live`.`person`, `exportpdb_new`.`person` WHERE SHA1( CONCAT_WS( '-', `exportpdb_new`.`person`.nID ) ) IN (SELECT SHA1( CONCAT_WS( '-', nID ) ) FROM `exportpdb_live`.`person`);



SELECT SHA1( CONCAT( CAST( nID AS CHAR ) ) ) AS KeyHash, nID,strName,dtBirth,dtDeath,bMale,strDescription FROM `exportpdb_new`.`person` WHERE nID  IN (SELECT nID  FROM `exportpdb_live`.`person`);

SELECT *, CONCAT_WS( '-', CAST( nID AS CHAR ), CAST( COALESCE( dtDeath, '#~NULL~#' ) AS CHAR ) ) FROM person WHERE nid=160727;
SELECT *, CONCAT_WS( '-', nID, dtDeath, strName ) FROM person WHERE nid=160727;

