/*----------------------------------------------------------------------
Copyright (c) Dan Petitt, http://www.coderanger.com
All Rights Reserved.
Please see the file "licence.txt" for licencing details.

File:	TableCompareABC.h
Owner:	danp@coderanger.com

Purpose:	Abstract class for database syncronisation.
					Override OnProcessTable to perform different algorithms for
					generating differences SQL scripts
----------------------------------------------------------------------*/
#pragma once

class CTableCompareABC
{
public:
	CTableCompareABC(void);
	~CTableCompareABC(void);

	void SetSourceDatabase( LPCTSTR pcszHost, LPCTSTR pcszUser, LPCTSTR pcszPassword, LPCTSTR pcszDatabase, UINT uPort = 3306 );
	void SetTargetDatabase( LPCTSTR pcszHost, LPCTSTR pcszUser, LPCTSTR pcszPassword, LPCTSTR pcszDatabase, UINT uPort = 3306 );
	void Start( LPCTSTR pcszExportFilePath, bool bDisplayProgress );


	struct Field
	{
		Field() : bIsNull( false ), uType( 0 ), bIsPrimaryKey( false ), bIsUniqueKey( false ), bIsDifferent( true ) {};
		std::string strName;
		std::string strValue;
		bool bIsNull;
		DWORD uType;
		bool bIsPrimaryKey, bIsUniqueKey;

		bool bIsDifferent;
	};

	struct Record
	{
		std::string strHash;
		std::vector< Field > arrFields;
	};


protected:
	virtual void OnProcessTable( const XMySQL::CConnection::Table &tbl ) = 0;

	XMySQL::CConnection m_mySource;
	XMySQL::CConnection m_myTarget;

	FILE *m_pFile;
	bool m_bDisplayProgress;

	UINT m_uTotalIdentical;
	UINT m_uTotalDifferent;
	UINT m_uTotalInserted;
	UINT m_uTotalDeleted;

	UINT m_uRecordsIdentical;
	UINT m_uRecordsDifferent;
	UINT m_uRecordsInserted;
	UINT m_uRecordsDeleted;

	std::vector< std::string > m_arrInsert, m_arrDelete, m_arrUpdate;


private:
	void ProcessTable( const XMySQL::CConnection::Table &tbl );

	UINT GetIdenticalCount() const { return m_uRecordsIdentical; }
	UINT GetDifferentCount() const { return m_uRecordsDifferent; }
	UINT GetDeletedCount() const { return m_uRecordsDeleted; }
	UINT GetInsertedCount() const { return m_uRecordsInserted; }

	void WriteOutStatements( const std::vector< std::string > &arr );


private:
	time_t m_tStart;


private:
	CTableCompareABC( const CTableCompareABC & );
	CTableCompareABC & operator = ( const CTableCompareABC & );
};
