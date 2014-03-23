DBSync
=======================

DBSync is a simple command line Windows tool to sync two mySQL databases and export a SQL 'differences' file which when executed back onto the target database will bring them both back into sync.

Requires the XMySQLLib library available at https://github.com/coderangerdan/XMySQLLib


## Usage:

    MySQL DBSync (c) Copyright CodeRanger.com. All rights reserved.
    DBSync takes two mySQL databases and compares the data in each table so as to
    create a SQL 'differences' file that can be run on the target db to bring it
    into sync with the source database. This can use a lot of memory so use 64bit.

    dbsync64.exe (parameters as below all are required):
        <sourcehost> <sourceport> <sourceuser> <sourcedbname> <sourcepass>
        <targethost> <targetport> <targetuser> <targetpass> <targetdbname>
        <showprogress: true|false>
        <exportfilepath>



## Licence:

    DBSync - a simple mysql database sync tool

    Copyright (c) Dan Petitt (CoderRanger.com)

    Redistribution and use in source and binary forms, with or without modification,
    are permitted provided that the following conditions are met:

        1)     Redistributions of source code must retain the above copyright
               notice, this list of conditions and the following disclaimer.

        2)     Redistributions in binary form must reproduce the above copyright
               notice, this list of conditions and the following disclaimer in
               the documentation and/or other materials provided with the
               distribution.
             
    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
    ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
    ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
