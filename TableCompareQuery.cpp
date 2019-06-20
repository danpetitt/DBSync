/*----------------------------------------------------------------------
Copyright (c) Dan Petitt, http://www.coderanger.com
All Rights Reserved.
Please see the file "licence.txt" for licencing details.

File:	TableCompareQuery.cpp
Owner:	danp@coderanger.com

Purpose:	Derived table syncronisation class which uses 3 different queries
					to find out rows to insert, delete, update

					Advantages: It works, its quick and uses low memory
					Disadvantages: Lot of work is done on server siding Querying for
					hashes
----------------------------------------------------------------------*/
#include "stdafx.h"
#include "TableCompareQuery.h"


CTableCompareQuery::CTableCompareQuery(void)
{
}

CTableCompareQuery::~CTableCompareQuery(void)
{
}



void CTableCompareQuery::OnProcessTable( const XMySQL::CConnection::Table &tbl )
{
	const std::string strSourceDB( m_mySource.CurrentDatabase() );
	const std::string strTargetDB( m_myTarget.CurrentDatabase() );

	const size_t uNumFields = tbl.arrFields.size();
	std::string strKeyFields, strAllFields, strKeyFieldsJoinWithDB, strAllFieldsWithSourceDB, strAllFieldsWithTargetDB;
	for( size_t uField = 0; uField < uNumFields; uField++ )
	{
		const XMySQL::CConnection::FieldInfo &fieldInfo = tbl.arrFields.at( uField );
		if( fieldInfo.bIsPrimaryKey )
		{
			if( strKeyFields.length() )
			{
				strKeyFields += _T(",");
			}
			strKeyFields += fieldInfo.strName;


			if( strKeyFieldsJoinWithDB.length() )
			{
				strKeyFieldsJoinWithDB += _T(" AND ");
			}
			strKeyFieldsJoinWithDB +=  _T("`") +  strSourceDB + _T("`.`") + tbl.strName + _T("`.") + fieldInfo.strName;
			strKeyFieldsJoinWithDB +=  _T("=`") +  strTargetDB + _T("`.`") + tbl.strName + _T("`.") + fieldInfo.strName;
		}


		if( strAllFields.length() )
		{
			strAllFields += _T(",");
		}
		strAllFields += fieldInfo.strName;


		if( strAllFieldsWithSourceDB.length() )
		{
			strAllFieldsWithSourceDB += _T(",");
		}
		strAllFieldsWithSourceDB += _T("`") +  strSourceDB + _T("`.`") + tbl.strName + _T("`.") + fieldInfo.strName;


		if( strAllFieldsWithTargetDB.length() )
		{
			strAllFieldsWithTargetDB += _T(",");
		}
		strAllFieldsWithTargetDB += _T("`") +  strTargetDB + _T("`.`") + tbl.strName + _T("`.") + fieldInfo.strName;
	}


	//
	// Get total records so we can work out count of identical records by subtracting insert, delete and update records from total
	UINT uTotalRecords = 0;

	XMySQL::CResultSet rsCount( m_mySource, XMySQL::CResultSet::Client );
	rsCount.Executef( _T("SELECT count(*) FROM %s"), tbl.strName.c_str() );
	if( !rsCount.IsEOF() )
	{
		uTotalRecords = atol( rsCount.GetFieldValue( 0 ) );
	}



	// Find our records to insert first...
	// SELECT * FROM `exportpdb_new`.person WHERE nid NOT IN (SELECT nid FROM `exportpdb_live`.person)
	std::stringstream dataInsertStream;
	dataInsertStream << _T("SELECT ") << strAllFields;
	dataInsertStream << _T(" FROM `") << strSourceDB << _T("`.`") << tbl.strName << _T("`");
	dataInsertStream << _T(" WHERE (") << strKeyFields << _T(") NOT IN (SELECT ") << strKeyFields << _T(" FROM `") << strTargetDB << _T("`.`") << tbl.strName << _T("`)");
	_tprintf( "  Querying for records to insert..." );


	XMySQL::CResultSet rsInserts( m_mySource, XMySQL::CResultSet::Client );
	rsInserts.Execute( dataInsertStream.str().c_str() );
	if( !rsInserts.IsEOF() )
	{
		const std::string bulkInsertStartStatement( _T("INSERT INTO `") + tbl.strName + _T("`(") + strAllFields + _T(") VALUES ") );
		static const std::string endStatement( _T(";\n") );


		m_arrInsert.push_back( bulkInsertStartStatement );


		std::string bulkInsertValuesStatement;

		while( !rsInserts.IsEOF() )
		{
			if( m_bUseBulkInserts )
			{
				// 	INSERT INTO programmekeyword(nProgrammeID, nKeywordID) VALUES
				// 		(740044, 37),
				// 		(740044, 42),
				// 		(740044, 560);
				{
					if( bulkInsertValuesStatement.size() )
						bulkInsertValuesStatement += _T(",");

					bulkInsertValuesStatement += _T("\n(") + BuildBulkInsertFieldValues( rsInserts ) + _T(")");
				}

				if( m_uRecordsInserted && m_uRecordsInserted % 200 == 0 )
				{
					m_arrInsert.push_back( bulkInsertValuesStatement );
					m_arrInsert.push_back( endStatement );

					m_arrInsert.push_back( bulkInsertStartStatement );

					bulkInsertValuesStatement.clear();
				}
			}
			else
			{
				std::string streamStatement;
				streamStatement += _T("INSERT INTO `") + tbl.strName + _T("`");
				streamStatement += _T(" SET ") + BuildInsertFieldValues( rsInserts ) + _T(";\n");

				m_arrInsert.push_back( streamStatement );
			}

			m_uRecordsInserted++;


			if( m_bDisplayProgress )
			{
				float nPercent = ( (float)rsInserts.GetCurrentRowNum() / (float)rsInserts.GetRowCount() ) * 100;
				_tprintf( _T("\r  Analysing inserts: %3.0f%% (%d of %d)     "), nPercent, static_cast<UINT>(rsInserts.GetCurrentRowNum()), static_cast<UINT>(rsInserts.GetRowCount()) );
			}

			rsInserts.MoveNext();
		}

		if( m_bUseBulkInserts && bulkInsertValuesStatement.size() )
		{
			// Some left over
			m_arrInsert.push_back( bulkInsertValuesStatement );
			m_arrInsert.push_back( endStatement );
		}
	}
	else
	{
		_tprintf( _T("\r  Analysed inserts, none found            ") );
	}



	// Next find our records to delete...
	// SELECT count(*),nid FROM `exportpdb_live`.person WHERE nid NOT IN (SELECT nid FROM `exportpdb_new`.person)
	std::stringstream dataDeleteStream;
	dataDeleteStream << _T("SELECT ") << strKeyFields;
	dataDeleteStream << _T(" FROM `") << strTargetDB << _T("`.`") << tbl.strName << _T("`");
	dataDeleteStream << _T(" WHERE (") << strKeyFields << _T(") NOT IN (SELECT ") << strKeyFields << _T(" FROM `") << strSourceDB << _T("`.`") << tbl.strName << _T("`)");
	_tprintf( "\n  Querying for records to delete..." );
//	_tprintf( "DELETE records: %s\n\n", dataDeleteStream.str().c_str() );

	XMySQL::CResultSet rsDeletes( m_mySource, XMySQL::CResultSet::Client );
	rsDeletes.Execute( dataDeleteStream.str().c_str() );
	if( !rsDeletes.IsEOF() )
	{
		while( !rsDeletes.IsEOF() )
		{
			// Target doesnt exist in source, delete it...
			std::stringstream deleteStatementStream;
			deleteStatementStream << _T("DELETE FROM `") << tbl.strName << _T("`");
			deleteStatementStream << BuildDeleteWhereClause( rsDeletes ) << _T(";\n"); 

			m_arrDelete.push_back( deleteStatementStream.str() );
			m_uRecordsDeleted++;


			if( m_bDisplayProgress )
			{
				float nPercent = ( (float)rsDeletes.GetCurrentRowNum() / (float)rsDeletes.GetRowCount() ) * 100;
				_tprintf( _T("\r  Analysing deletes: %3.0f%% (%d of %d)     "), nPercent, static_cast<UINT>(rsDeletes.GetCurrentRowNum()), static_cast<UINT>(rsDeletes.GetRowCount()) );
			}

			rsDeletes.MoveNext();
		}
	}
	else
	{
		_tprintf( _T("\r  Analysed deletes, none found            ") );
	}


	// Next find our records to be updated
	// 	SELECT
	// 		`exportpdb_new`.`schedulepersontypelink`.nscheduleid
	// 		, `exportpdb_new`.`schedulepersontypelink`.nPersonID
	// 		, `exportpdb_new`.`schedulepersontypelink`.nType
	// 		, `exportpdb_new`.`schedulepersontypelink`.nOrder
	// 		, `exportpdb_new`.`schedulepersontypelink`.strRole
	// 
	// 		, `exportpdb_live`.`schedulepersontypelink`.nscheduleid
	// 		, `exportpdb_live`.`schedulepersontypelink`.nPersonID
	// 		, `exportpdb_live`.`schedulepersontypelink`.nType
	// 		, `exportpdb_live`.`schedulepersontypelink`.nOrder
	// 		, `exportpdb_live`.`schedulepersontypelink`.strRole
	// 
	// 	FROM `exportpdb_new`.`schedulepersontypelink` 
	// 
	// 	INNER JOIN  `exportpdb_live`.`schedulepersontypelink`
	// 						ON (`exportpdb_live`.`schedulepersontypelink`.nscheduleid = `exportpdb_new`.`schedulepersontypelink`.nscheduleid 
	// 						AND `exportpdb_live`.`schedulepersontypelink`.nPersonID = `exportpdb_new`.`schedulepersontypelink`.nPersonID 
	// 						AND `exportpdb_live`.`schedulepersontypelink`.nType = `exportpdb_new`.`schedulepersontypelink`.nType )
	// 
	// 	WHERE
	// 	SHA1
	// 	( 
	// 		CONCAT_WS( '-', `exportpdb_new`.`schedulepersontypelink`.nscheduleid, `exportpdb_new`.`schedulepersontypelink`.nPersonID, `exportpdb_new`.`schedulepersontypelink`.nType, `exportpdb_new`.`schedulepersontypelink`.nOrder, `exportpdb_new`.`schedulepersontypelink`.strRole )
	// 	)
	// 	<>
	// 	SHA1
	// 	(
	// 		CONCAT_WS( '-', `exportpdb_live`.`schedulepersontypelink`.nscheduleid, `exportpdb_live`.`schedulepersontypelink`.nPersonID, `exportpdb_live`.`schedulepersontypelink`.nType, `exportpdb_live`.`schedulepersontypelink`.nOrder, `exportpdb_live`.`schedulepersontypelink`.strRole )
	// 	);

	if( !_tcsicmp( tbl.strName.c_str(), _T("programmepersontype") ) )
	{
		_tprintf( "\n  here..." );
	}

	std::stringstream dataUpdateStream;
	dataUpdateStream << _T("SELECT ") << strAllFieldsWithSourceDB << _T(",") << strAllFieldsWithTargetDB;
	dataUpdateStream << _T(" FROM `") << strSourceDB << _T("`.`") << tbl.strName << _T("`");
	dataUpdateStream << _T(" INNER JOIN `") << strTargetDB << _T("`.`") << tbl.strName << _T("`");
	dataUpdateStream << _T(" ON (") << strKeyFieldsJoinWithDB << _T(")");
	dataUpdateStream << _T(" WHERE SHA1( CONCAT_WS( '-', ") << strAllFieldsWithSourceDB << _T(" ) ) <> SHA1( CONCAT_WS( '-', ") << strAllFieldsWithTargetDB << _T(" ) )");
	_tprintf( "\n  Querying for records to update..." );
 	_tprintf( "UPDATE records: %s\n\n", dataUpdateStream.str().c_str() );


	//
	// Build up our update statements for fields that are different
	XMySQL::CResultSet rsSource( m_mySource, XMySQL::CResultSet::Client );
	rsSource.Execute( dataUpdateStream.str().c_str() );
	if( !rsSource.IsEOF() )
	{
		while( !rsSource.IsEOF() )
		{
			std::vector< CTableCompareABC::Field > arrKeyValues;
			std::vector< CTableCompareABC::Field > arrDifferentFields;

			for( UINT u = 0; u < uNumFields; u++ )
			{
				const char * pszSourceFieldName = rsSource.GetFieldName( u );
				const char * pszSourceFieldValue = rsSource.GetFieldValue( u );

				const char * pszTargetFieldName = rsSource.GetFieldName( u + (UINT)uNumFields );
				const char * pszTargetFieldValue = rsSource.GetFieldValue( u + (UINT)uNumFields );

				if( _tcsicmp( pszSourceFieldName, pszTargetFieldName ) )
				{
					// fields out of order, should never happen
					break;
				}


				bool bFieldDifferent = false;
				if( pszSourceFieldValue == NULL && pszTargetFieldValue == NULL )
				{
					// Both same, no compare
				}
				else if( ( pszSourceFieldValue == NULL && pszTargetFieldValue != NULL ) || ( pszSourceFieldValue != NULL && pszTargetFieldValue == NULL ) )
				{
					// One is a null, the other is not, so different
					bFieldDifferent = true;
				}
				else
				{
					// Both same and not null so compare values, case sensitive
					if( strcmp( pszSourceFieldValue, pszTargetFieldValue ) )
					{
						bFieldDifferent = true;
					}
				}

				if( bFieldDifferent )
				{
					CTableCompareABC::Field fld;
					fld.bIsDifferent = true;
					fld.bIsNull = ( pszSourceFieldValue == NULL ) ? true : false;
					fld.uType = rsSource.GetFieldType( u );
					fld.strName = std::string( pszSourceFieldName );
					if( !fld.bIsNull )
					{
						fld.strValue = std::string( pszSourceFieldValue );
					}
					arrDifferentFields.push_back( fld );
				}

				if( rsSource.IsFieldPrimaryKey( u ) || rsSource.IsFieldUniqueKey( u ) )
				{
					CTableCompareABC::Field fldKey;
					fldKey.bIsNull = ( pszTargetFieldValue == NULL ) ? true : false;
					fldKey.uType = rsSource.GetFieldType( u );
					fldKey.strName = std::string( pszTargetFieldName );
					if( !fldKey.bIsNull )
					{
						fldKey.strValue = std::string( pszTargetFieldValue );
					}
					arrKeyValues.push_back( fldKey );
				}

			}

			{
				std::stringstream updateStatementStream;
				updateStatementStream << _T("UPDATE `") << tbl.strName << _T("`");
				updateStatementStream << _T(" SET ") << BuildUpdateValues( arrDifferentFields ) << BuildUpdateWhereClause( arrKeyValues ) << _T(";\n");

				m_arrUpdate.push_back( updateStatementStream.str() );
				m_uRecordsDifferent++;
			}


			if( m_bDisplayProgress )
			{
				float nPercent = ( (float)rsSource.GetCurrentRowNum() / (float)rsSource.GetRowCount() ) * 100;
				_tprintf( _T("\r  Analysing updates: %3.0f%% (%d of %d)     "), nPercent, static_cast<UINT>(rsSource.GetCurrentRowNum()), static_cast<UINT>(rsSource.GetRowCount()) );
			}

			rsSource.MoveNext();
		}
	}
	else
	{
		_tprintf( _T("\r  Analysed updates, none found            ") );
	}


	m_uRecordsIdentical = uTotalRecords - m_uRecordsDifferent - m_uRecordsInserted;
}


std::string CTableCompareQuery::BuildUpdateValues( const std::vector< CTableCompareABC::Field > &arrFields )
{
	// Build a stream of all our name=value column value pairs
	std::stringstream streamFields;
	for( UINT u = 0; u < arrFields.size(); u++ )
	{
		const CTableCompareABC::Field &fld = arrFields.at( u );

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
			if( FieldIsQuoted( fld.uType ) )
			{
				streamFields << _T("'");

				std::string sValue( fld.strValue );
				m_mySource.EscapeString( sValue );
				streamFields << sValue;

				streamFields << _T("'");
			}
			else
			{
				streamFields << fld.strValue;
			}
		}
	}

	return streamFields.str();
}


std::string CTableCompareQuery::BuildUpdateWhereClause( const std::vector< CTableCompareABC::Field > &arrFields )
{
	// Build a stream of all our keyed name=value column value pairs
	std::stringstream streamFields;
	streamFields << _T(" WHERE ");
	UINT uFieldAddedCount = 0;
	for( UINT u = 0; u < arrFields.size(); u++ )
	{
		const CTableCompareABC::Field &fld = arrFields.at( u );

		if( uFieldAddedCount )
		{
			streamFields << _T(" AND ");
		}

		streamFields << fld.strName;
		streamFields << _T("=");

		if( FieldIsQuoted( fld.uType ) )
		{
			streamFields << _T("'");

			std::string sValue( fld.strValue );
			m_mySource.EscapeString( sValue );
			streamFields << sValue;

			streamFields << _T("'");
		}
		else
		{
			streamFields << fld.strValue;
		}

		uFieldAddedCount++;
	}

	return streamFields.str();
}


std::string CTableCompareQuery::BuildInsertFieldValues( const XMySQL::CResultSet &rs )
{
	// Build a stream of all our keyed name=value column value pairs
	std::stringstream streamFields;

	const UINT uFieldCount = rs.GetFieldCount();
	for( UINT u = 0; u < uFieldCount; u++ )
	{
		const UINT uFieldType = rs.GetFieldType( u );

		if( streamFields.str().size() )
		{
			streamFields << _T(",");
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

			if( FieldIsQuoted( uFieldType ) )
			{
				streamFields << _T("'");

				std::string sValue( pcszFieldValue );
				m_mySource.EscapeString( sValue );
				streamFields << sValue;

				streamFields << _T("'");
			}
			else
			{
				streamFields << pcszFieldValue;
			}
		}
	}

	return streamFields.str();
}


std::string CTableCompareQuery::BuildBulkInsertFieldValues( const XMySQL::CResultSet &rs )
{
	// Build a stream of all our keyed name=value column value pairs
	std::stringstream streamFields;

	const UINT uFieldCount = rs.GetFieldCount();
	for( UINT u = 0; u < uFieldCount; u++ )
	{
		const UINT uFieldType = rs.GetFieldType( u );

		if( streamFields.str().size() )
		{
			streamFields << _T(",");
		}

		if( rs.IsNull( u ) )
		{
			streamFields << _T("NULL");
		}
		else
		{
			const char * pcszFieldValue = rs.GetFieldValue( u );

			if( FieldIsQuoted( uFieldType ) )
			{
				streamFields << _T("'");

				std::string sValue( pcszFieldValue );
				m_mySource.EscapeString( sValue );
				streamFields << sValue;

				streamFields << _T("'");
			}
			else
			{
				streamFields << pcszFieldValue;
			}
		}
	}

	return streamFields.str();
}


std::string CTableCompareQuery::BuildDeleteWhereClause( const XMySQL::CResultSet &rs )
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

				if( FieldIsQuoted( uFieldType ) )
				{
					streamFields << _T("'");

					std::string sValue( pcszFieldValue );
					m_mySource.EscapeString( sValue );
					streamFields << sValue;

					streamFields << _T("'");
				}
				else
				{
					streamFields << pcszFieldValue;
				}
			}

			uFieldAddedCount++;
		}
	}

	return streamFields.str();
}
