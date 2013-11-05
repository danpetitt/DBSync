/*----------------------------------------------------------------------
Copyright (c) Dan Petitt, http://www.coderanger.com
All Rights Reserved.
Please see the file "licence.txt" for licencing details.

File:	TableCompareStream.cpp
Owner:	danp@coderanger.com

Purpose:	Derived table syncronisation class which loads up all data
					into memory and analyses differences between the two databases

					Advantages: It works
					Disadvantages: It uses a lot of memory (hence only really use
					this on a 64 bit machine in 64 bit mode)
----------------------------------------------------------------------*/
#include "stdafx.h"
#include "TableCompareStream.h"


CTableCompareStream::CTableCompareStream(void)
{
}

CTableCompareStream::~CTableCompareStream(void)
{
}



void CTableCompareStream::OnProcessTable( const XMySQL::CConnection::Table &tbl )
{
	const size_t uNumFields = tbl.arrFields.size();
	std::string strKeyFields, strAllFields;
	for( size_t uField = 0; uField < uNumFields; uField++ )
	{
		const XMySQL::CConnection::FieldInfo &fieldInfo = tbl.arrFields.at( uField );
		if( fieldInfo.bIsPrimaryKey )
		{
			if( strKeyFields.length() )
			{
				strKeyFields += _T(", '-', ");
			}
			strKeyFields += _T("CAST( ") + fieldInfo.strName + _T(" AS CHAR )");
		}

		if( strAllFields.length() )
		{
			strAllFields += _T(",");
		}
		strAllFields += fieldInfo.strName;
	}

	std::stringstream dataRequestStream;
	dataRequestStream << _T("SELECT MD5( CONCAT( ") << strKeyFields.c_str() << _T(" ) ) AS KeyHash, ") << strAllFields;
	dataRequestStream << _T(" FROM `") << tbl.strName << _T("`");
	dataRequestStream << _T(" ORDER BY KeyHash");


	std::map< std::string, CTableCompareABC::Record > mapSourceRecords;

	XMySQL::CResultSet rsSource( m_mySource, XMySQL::CResultSet::Client );
	rsSource.Execute( dataRequestStream.str().c_str() );
	while( !rsSource.IsEOF() )
	{
		const char * pszSourceKey = rsSource.GetString( 0 );

		CTableCompareABC::Record rec;
		rec.strHash = std::string( pszSourceKey );

		// Store all our field data for each row for later comparison
		for( UINT u = 0; u < rsSource.GetFieldCount(); u++ )
		{
			CTableCompareABC::Field fld;
			fld.strName = std::string( rsSource.GetFieldName( u ) );
			fld.bIsNull = rsSource.IsNull( u );
			if( !fld.bIsNull )
			{
				fld.strValue = std::string( rsSource.GetFieldValue( u ) );
			}
			fld.uType = rsSource.GetFieldType( u );
			fld.bIsPrimaryKey = rsSource.IsFieldPrimaryKey( u );
			fld.bIsUniqueKey = rsSource.IsFieldUniqueKey( u );

			rec.arrFields.push_back( fld );
		}

		mapSourceRecords[ rec.strHash ] = rec;


		if( m_bDisplayProgress )
		{
			float nPercent = ( (float)rsSource.GetCurrentRowNum() / (float)rsSource.GetRowCount() ) * 100;
			_tprintf( _T("\r  Calculating hash: %3.0f%% (%d of %d)"), nPercent, static_cast<UINT>(rsSource.GetCurrentRowNum()), static_cast<UINT>(rsSource.GetRowCount()) );
		}

		rsSource.MoveNext();
	}



	std::map< std::string, bool > mapRecordsProcessed;

	//
	// Now go through our target database and compare cached rows
	XMySQL::CResultSet rsTarget( m_myTarget, XMySQL::CResultSet::Client );
	rsTarget.Execute( dataRequestStream.str().c_str() );
	while( !rsTarget.IsEOF() )
	{
		const std::string strTargetKey( rsTarget.GetString( 0 ) );
		mapRecordsProcessed[ strTargetKey ] = true;

		std::map< std::string, CTableCompareABC::Record >::iterator it = mapSourceRecords.find( strTargetKey );
		if( it == mapSourceRecords.end() )
		{
			// Target doesnt exist in source, delete it...
			std::stringstream deleteStatementStream;
			deleteStatementStream << _T("DELETE FROM `") << tbl.strName << _T("`");
			deleteStatementStream << BuildWhereClauseForKeys( rsTarget ) << _T(";\n"); 

			m_arrDelete.push_back( deleteStatementStream.str() );
			m_uRecordsDeleted++;
		}
		else
		{
			// Found record in target, compare values and then remove it from source
			CTableCompareABC::Record rec = it->second;

			bool bDifferent = false;
			for( UINT uTgt = 0; uTgt < rsTarget.GetFieldCount(); uTgt++ )	// skip first field which is the hash field
			{
				const char * pszTargetFieldName = rsTarget.GetFieldName( uTgt );

				Field &fld = rec.arrFields.at( uTgt );
				fld.bIsDifferent = false;
				if( !_stricmp( fld.strName.c_str(), pszTargetFieldName ) )
				{
					// Found field, compare values
					const char * pszTargetFieldValue = rsTarget.GetFieldValue( uTgt );
					if( fld.bIsNull && pszTargetFieldValue == NULL )
					{
						// Both same, no compare
					}
					else if( ( fld.bIsNull && pszTargetFieldValue != NULL ) || ( !fld.bIsNull && pszTargetFieldValue == NULL ) )
					{
						// One is a null, the other is not, so different
						fld.bIsDifferent = true;
						bDifferent = true;
					}
					else
					{
						// Both same and not null so compare values, case sensitive
						if( strcmp( fld.strValue.c_str(), pszTargetFieldValue ) )
						{
							fld.bIsDifferent = true;
							bDifferent = true;
						}
					}
				}
				else
				{
					_tprintf( _T("\nERROR: Fields are in a different order between source and target and cannot be compared") );
				}
			}


			if( bDifferent )
			{
				std::stringstream updateStatementStream;
				updateStatementStream << _T("UPDATE `") << tbl.strName << _T("`");
				updateStatementStream << _T(" SET ") << BuildDifferentFieldValues( rec ) << BuildWhereClauseForKeys( rec ) << _T(";\n");

				m_arrUpdate.push_back( updateStatementStream.str() );
				m_uRecordsDifferent++;
			}
			else
			{
				m_uRecordsIdentical++;
			}
		}


		if( m_bDisplayProgress )
		{
			float nPercent = ( (float)rsTarget.GetCurrentRowNum() / (float)rsTarget.GetRowCount() ) * 100;
			_tprintf( _T("\r  Analysing: %3.0f%% (%d of %d)                          "), nPercent, static_cast<UINT>(rsTarget.GetCurrentRowNum()), static_cast<UINT>(rsTarget.GetRowCount()) );
		}

		rsTarget.MoveNext();
	}


	//
	// Any records in source that werent in target need inserting into target
	std::map< std::string, CTableCompareABC::Record >::iterator itSource = mapSourceRecords.begin();
	for( itSource = mapSourceRecords.begin(); itSource != mapSourceRecords.end(); itSource++ )
	{
		const CTableCompareABC::Record rec = itSource->second;

		std::map< std::string, bool >::iterator itProcessed = mapRecordsProcessed.find( rec.strHash );
		if( itProcessed == mapRecordsProcessed.end() )
		{
			std::stringstream streamStatement;
			streamStatement << _T("INSERT INTO `") << tbl.strName << _T("`");
			streamStatement << _T(" SET ") << BuildDifferentFieldValues( rec ) << _T(";\n");

			m_arrInsert.push_back( streamStatement.str() );
			m_uRecordsInserted++;
		}
	}
}


std::string CTableCompareStream::BuildDifferentFieldValues( const CTableCompareABC::Record &rec )
{
	// Build a stream of all our name=value column value pairs
	std::stringstream streamFields;
	for( UINT u = 1; u < rec.arrFields.size(); u++ )	// first field is always KeyHash field so skip past it
	{
		const CTableCompareABC::Field &fld = rec.arrFields.at( u );
		if( !fld.bIsDifferent )
		{
			continue;
		}

		if( streamFields.str().size() )
		{
			streamFields << _T(",");
		}

		streamFields << fld.strName;
		streamFields << _T("=");
		if( fld.bIsNull )
		{
			streamFields << _T("NULL");
		}
		else
		{
			streamFields << _T("'");
			if( fld.uType == MYSQL_TYPE_STRING || fld.uType == MYSQL_TYPE_VAR_STRING || fld.uType == MYSQL_TYPE_VARCHAR || fld.uType == MYSQL_TYPE_BLOB )
			{
				std::string sValue( fld.strValue );
				m_mySource.EscapeString( sValue );
				streamFields << sValue;
			}
			else
			{
				streamFields << fld.strValue;
			}
			streamFields << _T("'");
		}
	}

	return streamFields.str();
}


std::string CTableCompareStream::BuildWhereClauseForKeys( const CTableCompareABC::Record &rec )
{
	// Build a stream of all our keyed name=value column value pairs
	std::stringstream streamFields;
	streamFields << _T(" WHERE ");
	UINT uFieldAddedCount = 0;
	for( UINT u = 0; u < rec.arrFields.size(); u++ )
	{
		const CTableCompareABC::Field &fld = rec.arrFields.at( u );
		if( fld.bIsPrimaryKey || fld.bIsUniqueKey )
		{
			if( uFieldAddedCount )
			{
				streamFields << _T(" AND ");
			}

			streamFields << fld.strName;
			streamFields << _T("=");
			if( fld.bIsNull )
			{
				streamFields << _T("NULL");
			}
			else
			{
				streamFields << _T("'");
				if( fld.uType == MYSQL_TYPE_STRING || fld.uType == MYSQL_TYPE_VAR_STRING || fld.uType == MYSQL_TYPE_VARCHAR || fld.uType == MYSQL_TYPE_BLOB )
				{
					std::string sValue( fld.strValue );
					m_mySource.EscapeString( sValue );
					streamFields << sValue;
				}
				else
				{
					streamFields << fld.strValue;
				}
				streamFields << _T("'");
			}

			uFieldAddedCount++;
		}
	}

	return streamFields.str();
}


std::string CTableCompareStream::BuildWhereClauseForKeys( const XMySQL::CResultSet &rs )
{
	// Build a stream of all our keyed name=value column value pairs
	std::stringstream streamFields;
	streamFields << _T(" WHERE ");

	UINT uFieldAddedCount = 0;
	const UINT uFieldCount = rs.GetFieldCount();
	for( UINT u = 0; u < uFieldCount; u++ )
	{
		const bool bIsPrimaryKey = rs.IsFieldPrimaryKey( u );
		const bool bIsUniqueKey = rs.IsFieldUniqueKey( u );

		if( bIsPrimaryKey || bIsUniqueKey )
		{
			const UINT uFieldType = rs.GetFieldType( u );

			if( uFieldAddedCount )
			{
				streamFields << _T(" AND ");
			}

			streamFields << rs.GetFieldName( u );
			streamFields << _T("=");
			if( rs.IsNull( u ) )
			{
				streamFields << _T("NULL");
			}
			else
			{
				const char * pcszFieldValue = rs.GetFieldValue( u );

				streamFields << _T("'");
				if( uFieldType == MYSQL_TYPE_STRING || uFieldType == MYSQL_TYPE_VAR_STRING || uFieldType == MYSQL_TYPE_VARCHAR || uFieldType == MYSQL_TYPE_BLOB )
				{
					std::string sValue( pcszFieldValue );
					m_mySource.EscapeString( sValue );
					streamFields << sValue;
				}
				else
				{
					streamFields << pcszFieldValue;
				}
				streamFields << _T("'");
			}

			uFieldAddedCount++;
		}
	}

	return streamFields.str();
}
