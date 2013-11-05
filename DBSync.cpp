/*----------------------------------------------------------------------
Copyright (c) Dan Petitt, http://www.coderanger.com
All Rights Reserved.
Please see the file "licence.txt" for licencing details.

File:	DBSync.cpp
Owner:	danp@coderanger.com

Purpose:	Generates a SQL database of SQL Statements describing the
					differences between two mySQL databases
----------------------------------------------------------------------*/
#include "stdafx.h"
#include "TableCompareStream.h"


int _tmain(int argc, _TCHAR* argv[])
{
	_tprintf( _T("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n") );
	_tprintf( _T("MySQL DBSync (c) Copyright 2010 Coderanger.com. All rights reserved.\n\n") );
	_tprintf( _T("DBSync takes two mySQL databases and compares the data in each table so as to\n") );
	_tprintf( _T("create a SQL 'differences' file that can be run on the target db to bring it\n") );
	_tprintf( _T("into sync with the source database. This can use a lot of memory so use 64bit.\n\n") );
	_tprintf( _T("Usage:\n") );
	_tprintf( _T("dbsync64.exe (parameters as below all are required):\n") );
	_tprintf( _T("\t<sourcehost> <sourceport> <sourceuser> <sourcedbname> <sourcepass>\n") );
	_tprintf( _T("\t<targethost> <targetport> <targetuser> <targetpass> <targetdbname>\n") );
	_tprintf( _T("\t<showprogress: true|false>\n") );
	_tprintf( _T("\t<exportfilepath>\n") );
	_tprintf( _T("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n") );

	if( argc != 13 )
	{
		_tprintf( _T("ERROR: Number of arguments passed in were wrong, expecting 12\n") );
		return EXIT_FAILURE;
	}

	LPCTSTR pcszSourceHost = argv[ 1 ];
	UINT uSourcePort = _tstoi( argv[ 2 ] );
	LPCTSTR pcszSourceUser = argv[ 3 ];
	LPCTSTR pcszSourcePassword = argv[ 4 ];
	LPCTSTR pcszSourceDB = argv[ 5 ];

	LPCTSTR pcszTargetHost = argv[ 6 ];
	UINT uTargetPort = _tstoi( argv[ 7 ] );
	LPCTSTR pcszTargetUser = argv[ 8 ];
	LPCTSTR pcszTargetPassword = argv[ 9 ];
	LPCTSTR pcszTargetDB = argv[ 10 ];

	bool bShowProgress = true;
	if( !_tcsicmp( argv[ 11 ], _T("false") ) )
	{
		bShowProgress = false;
	}

	LPCTSTR pcszExportFilePath = argv[ 12 ];

	_tprintf( _T("Syncing between source db: %s and target db: %s\n\n"), pcszSourceDB, pcszTargetDB );


	try
	{
		CTableCompareStream tblStream;
		tblStream.SetSourceDatabase( pcszSourceHost, pcszSourceUser, pcszSourcePassword, pcszSourceDB, uSourcePort );
		tblStream.SetTargetDatabase( pcszTargetHost, pcszTargetUser, pcszTargetPassword, pcszTargetDB, uTargetPort );
		tblStream.Start( pcszExportFilePath, bShowProgress );
	}
	catch( XMySQL::CException *e )
	{
		_tprintf( _T("ERROR: (%d) %s\n"), e->GetErrorCode(), e->GetError() );
		delete e;
		return EXIT_FAILURE;
	}


	return EXIT_SUCCESS;
}
