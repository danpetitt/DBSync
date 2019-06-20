/*----------------------------------------------------------------------
Copyright (c) Dan Petitt, http://www.coderanger.com
All Rights Reserved.
Please see the file "licence.txt" for licencing details.

File:	TableCompareStream.h
Owner:	danp@coderanger.com

Purpose:	Derived table syncronisation class which loads up all data
					into memory and analyses differences between the two databases

					Advantages: It works
					Disadvantages: It uses a lot of memory (hence only really use
					this on a 64 bit machine in 64 bit mode)
----------------------------------------------------------------------*/
#pragma once

#include "TableCompareABC.h"

class CTableCompareStream : public CTableCompareABC
{
public:
	CTableCompareStream(void);
	~CTableCompareStream(void);


protected:
	virtual void OnProcessTable( const XMySQL::CConnection::Table &tbl );


private:
	std::string BuildDifferentFieldValues( const CTableCompareABC::Record &rec );
	std::string BuildBulkInsertFieldValues( const CTableCompareABC::Record &rec );
	std::string BuildWhereClauseForKeys( const CTableCompareABC::Record &rec );
	std::string BuildWhereClauseForKeys( const XMySQL::CResultSet &rs );


private:
	CTableCompareStream( const CTableCompareStream & );
	CTableCompareStream & operator = ( const CTableCompareStream & );
};
