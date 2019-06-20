/*----------------------------------------------------------------------
Copyright (c) Dan Petitt, http://www.coderanger.com
All Rights Reserved.
Please see the file "licence.txt" for licencing details.

File:	TableCompareQuery.h
Owner:	danp@coderanger.com

Purpose:	Derived table syncronisation class which uses 3 different queries
					to find out rows to insert, delete, update

					Advantages: It works, its quick and uses low memory
					Disadvantages: Lot of work is done on server siding calculating
					hashes
----------------------------------------------------------------------*/
#pragma once

#include "TableCompareABC.h"

class CTableCompareQuery: public CTableCompareABC
{
public:
	CTableCompareQuery(void);
	~CTableCompareQuery(void);


protected:
	virtual void OnProcessTable( const XMySQL::CConnection::Table &tbl );


private:
	std::string BuildUpdateValues( const std::vector< CTableCompareABC::Field > &arrFields );
	std::string BuildUpdateWhereClause( const std::vector< CTableCompareABC::Field > &arrFields );
	std::string BuildInsertFieldValues( const XMySQL::CResultSet &rs );
	std::string BuildBulkInsertFieldValues( const XMySQL::CResultSet &rs );
	std::string BuildDeleteWhereClause( const XMySQL::CResultSet &rs );


private:
	CTableCompareQuery( const CTableCompareQuery & );
	CTableCompareQuery & operator = ( const CTableCompareQuery & );
};
