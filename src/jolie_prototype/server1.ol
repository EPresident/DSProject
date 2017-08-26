include "interfacce.iol"
include "console.iol"
include "string_utils.iol"
include "runtime.iol"
include "network_service.iol"
include "time.iol"
include "database.iol"

inputPort FlightBookingService 
{
  Location: "socket://localhost:8000"
  Protocol: sodep
  Interfaces: FlightBookingInterface, Coordinator
}

outputPort OtherServer 
{
  Protocol: sodep
  Interfaces: FlightBookingInterface
}

outputPort Coordinator 
{
  Protocol: sodep
  Interfaces: Coordinator
}

execution{concurrent}

constants
{
	serverName="Alaitalia",
	dbname = "db1",
	location = "socket://localhost:8000"
}

init
{
	println@Console("Server "+serverName+" initialized.")();
	global.id = 0;
	
	with ( connectionInfo ) {
            .username = "sa";
            .password = "";
            .host = "";
           .database = "file:"+dbname;
           .driver = "sqlite"
        };
        connect@Database( connectionInfo )( void );
		
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        //!! TEST resetto il DB										!!!
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        scope ( resets ) {
        install ( SQLException => println@Console("seat già vuota")() ); 
			updateRequest ="DROP TABLE seat";
			update@Database( updateRequest )( ret )
        };   
		scope ( resett ) {
        install ( SQLException => println@Console("trans già vuota")() ); 
			updateRequest ="DROP TABLE trans";
			update@Database( updateRequest )( ret )
        };  
		scope ( resetc ) {
        install ( SQLException => println@Console("coordTrans già vuota")() ); 
			updateRequest ="DROP TABLE coordTrans";
			update@Database( updateRequest )( ret )
        };  
        

        scope ( createTables ) {
            install ( SQLException => println@Console("Seat table already there")() );
            updateRequest =
                " CREATE TABLE \"seat\" ( "+
                    " `flight`	TEXT NOT NULL, "+
                    " `seat`	INTEGER NOT NULL, "+
                    " `state`	INTEGER NOT NULL DEFAULT 0, "+
                    " PRIMARY KEY(flight,seat))";
            update@Database( updateRequest )( ret )
        };
        scope ( createTablet ) {
            install ( SQLException => println@Console("Transact table already there")() );
            updateRequest =
                " CREATE TABLE \"trans\" ( "+
                " `tid`	TEXT NOT NULL, "+
                " `seat`	INTEGER NOT NULL, "+
                " `flight`	TEXT NOT NULL, "+
                " `newst`	INTEGER NOT NULL, "+
				" `committed` INTEGER NOT NULL DEFAULT 0, "+ // 0 = TENTATIVE, 1 = COMMITTED 2= COMMITTED (final)
                " PRIMARY KEY(tid,seat,flight))";
            update@Database( updateRequest )( ret )
        };
        scope ( createTablec ) {
            install ( SQLException => println@Console("Coord table already there")() );
            updateRequest =
                " CREATE TABLE \"coordtrans\" ( "+
                " `tid`	TEXT, "+
                " `partec`	TEXT, "+
				" `state`	INTEGER NOT NULL DEFAULT 0, " + // 0=REQUESTED, 1=CAN COMMIT, 2=COMMITTED, 3=ABORT
                " PRIMARY KEY(tid,partec))";
            update@Database( updateRequest )( ret )
        };

        //per ora creo i voli se non presenti
        
        scope ( v1 ) {
        install ( SQLException => println@Console("volo presente")() );
        updateRequest =
            "INSERT INTO seat(flight, seat, state) " +
            "VALUES (:flight, :seat, :state)";
        updateRequest.flight = "AZ0123";
        updateRequest.seat = 69;
        updateRequest.state = 0;
        update@Database( updateRequest )( ret )
        };

        scope ( v2 ) {
        install ( SQLException => println@Console("volo presente")() );
        updateRequest =
            "INSERT INTO seat(flight, seat, state) " +
            "VALUES (:flight, :seat, :state)";
        updateRequest.flight = "AZ0123";
        updateRequest.seat = 70;
        updateRequest.state = 0;
        update@Database( updateRequest )( ret )
        };
        
        scope ( v3 ) {
        install ( SQLException => println@Console("volo presente")() );        
        updateRequest =
            "INSERT INTO seat(flight, seat, state) " +
            "VALUES (:flight, :seat, :state)";
        updateRequest.flight = "AZ4556";
        updateRequest.seat = 42;
        updateRequest.state = 0;
        update@Database( updateRequest )( ret )
        };
  
        scope ( v4 ) {
        install ( SQLException => println@Console("volo presente")() ); 
        updateRequest =
            "INSERT INTO seat(flight, seat, state) " +
            "VALUES (:flight, :seat, :state)";
        updateRequest.flight = "AZ4556";
        updateRequest.seat = 44;
        updateRequest.state = 0;
        update@Database( updateRequest )( ret )
        };
         
        //se ero coordinatore cercare nel database se transazioni che non hanno ricevuto una risposta al commit
        //CODE
        
		scope ( recoveryTest ) {
			install ( SQLException => println@Console("vfadasda")() ); 
			updateRequest =
				"INSERT INTO coordTrans(tid, partec, state) " +
				"VALUES (:tid, :partec, :state)";
			updateRequest.tid = "Lefthansa4B0R7";
			updateRequest.partec = "socket://localhost:8001";
			updateRequest.state = 0;
			update@Database( updateRequest )( ret );
			updateRequest =
				"INSERT INTO coordTrans(tid, partec, state) " +
				"VALUES (:tid, :partec, :state)";
			updateRequest.tid = "Lefthansa4B0R7";
			updateRequest.partec = "socket://localhost:8000";
			updateRequest.state = 0;
			update@Database( updateRequest )( ret )
        };

		
		coordinatorRecovery

	
}

/*
==================================================================================================
||																								||
||											FUNCTIONS											||
||																								||
==================================================================================================
*/

define abortAll
{

	// Register abort in progress
	for(i=0, i<#participants, i++)
	{
				
		// Register that the transaction has aborted
		tr.statement[i] ="UPDATE coordtrans SET state = 3 "+ // ABORTED
		"WHERE tid = :tid AND partec = :partec";
		tr.statement[i].tid = transName;
		tr.statement[i].partec = participants[i]
	};

	executeTransaction@Database(tr)(ret);
	undef(tr);
	
	// Ask all participants to abort the transaction
	for(i=0, i<#participants, i++) //rendere parallelo 
	{
		OtherServer.location = participants[i];
		println@Console("Mando abort a "+OtherServer.location)();
		abort@OtherServer(tid)();
		
		// Remove participant from transaction
		updateRequest ="DELETE FROM coordtrans WHERE tid= :tid AND partec =:partec ";
		updateRequest.tid = transName;
		updateRequest.partec = participants[i];
		update@Database( updateRequest )( ret )

	};
	println@Console("Transaction "+transName+" aborted!")()
}

define finalizeCommit
{
	// All participants can commit; proceed finalizing the commit phase by sending doCommit
	println@Console("\n --------------------------------------------------------\n"+
	"Tutti i "+#participants+ "partecipanti possono fare il commit.")();
	serverfail=0;	
	
	for(i=0, i<#participants, i++)  //rendere parallelo 
	{
		OtherServer.location = participants[i];
		println@Console("Mando doCommit a "+OtherServer.location)();
		scope ( docom ){
			install (
				IOException => println@Console( "Server "+participant+" non disponibile 4")();
					sleep@Time(2000)();  //continua
					serverfail++
			);
			
			doCommit@OtherServer(tid)(answ);
			
			// Register that participant has committed
			tr.statement[i] ="UPDATE coordtrans SET state = 2 " // COMMITTED
			+" WHERE tid = :tid AND partec = :partec";
			tr.statement[i].tid = transName;
			tr.statement[i].partec = participants[i]		
		}
	};
	
	scope (doCommitTrans)
	{
		install(SQLException => println@Console("Errore nel doCommit")());
		executeTransaction@Database(tr)(ret)
	};
	undef(tr);

	println@Console("----> Transaction "+transName+" was successful! Errors: "+serverfail+"<----")()
}

define showDBS
{
	println@Console("\n\t\t---SEAT---")();
	qr = "SELECT * FROM seat";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	println@Console(str)();
	
	println@Console("\t\t---TRANS---")();
	qr = "SELECT * FROM trans";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	println@Console(str)();
	
	println@Console("\t\t---COORDTRANS---")();
	qr = "SELECT * FROM coordTrans";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	println@Console(str+"\n")()
}

define abort 
{
	// Variable transName must be defined

	// esegui transazione di commit per tid sul db
	println@Console("Abortisco la transazione "+transName+"...")();
	
	// Get list of changes to undo
	qr = "SELECT flight, seat, committed FROM trans WHERE tid= :tid";
	qr.tid = transName;
	query@Database(qr)(qres);
	
	// Undo the changes
	i = 0;
	for(row in qres)
	{
		if(row.row.committed != 0)
		{
			tr.statement[i] = "UPDATE seat SET state = 0 "+
			"WHERE tid = :tid AND flight = :flight AND seat = :seat";
			tr.statement[i].flight = row.row.flight;
			tr.statement[i].seat = row.row.seat;
			tr.statement[i].tid = transName;
			i++
		}		
	};
	tr.statement[i] = "DELETE FROM trans WHERE tid = :tid ";
	tr.statement[i].tid = transName;
	
	scope(abortTr)
	{
		install(SQLException => println@Console("Errore nell'abort")());
		executeTransaction@Database(tr)(ret)
	};
	
	undef(qr);
	undef(tr);
	
	println@Console("Abortita la transazione "+transName+"!")()
}


define coordinatorRecovery
{
	// Look for leftover transactions
	// prefix cr_ is to avoid variable clashes with abort procedure
	println@Console("\t\t---COORDINATOR RECOVERY---")();
	cr_qr = "SELECT tid, partec FROM coordTrans " +
	" WHERE state = 0 OR state = 1 OR state = 3";
	query@Database(cr_qr)(cr_qres);
	
	for( cr_row in cr_qres.row )
	{	
		cr_deleteEntry = true;
		if( cr_row.partec == location )
		{
			println@Console("Mando abort a me stesso.")();
			transName = cr_row.tid;
			abort
		} else
		{
			OtherServer.location = cr_row.partec;
			tid.location = location;
			println@Console("tid.location = "+tid.location)();
			tid.id = 69;
			tid.issuer = "PornHub";
			println@Console("Mando abort a "+OtherServer.location)();
			scope (cr_abortReq)
			{
				install(default => println@Console("Errore nell'abort, tengo la entry.")();
					cr_deleteEntry = false);
				abort@OtherServer(tid)()
			}
		};
		
		if( cr_deleteEntry )
		{
			cr_ur = "DELETE FROM coordTrans WHERE partec = :partec AND tid = :tid";
			cr_ur.tid = cr_row.tid;
			cr_ur.partec = cr_row.partec;
			update@Database(cr_ur)(ret)
		}
	};
	undef(OtherServer.location);
	println@Console("--- Coordinator recovery done.---")();
	showDBS
}

// TODO
/*define transactionRecovery
{

}*/

/*
==================================================================================================
||																								||
||											MAIN												||
||																								||
==================================================================================================
*/


main 
{
	[book(seatRequest)(tid)  //Coordinator
	{
		tid.issuer = serverName;
		tid.id = ++global.id;
		tid.location = ""+FlightBookingService.Location;
		println@Console("tid.location ="+tid.location)()
	}]
	{
		transName = tid.issuer+tid.id;		
		
		global.openTrans.(transName) << tid;
		println@Console("\nAperta transazione "+transName)();
		println@Console("Posti richiesti: "+#seatRequest.seat+"\n")();

		participants -> global.openTrans.(transName).participant;
		
		// Request lock-ins
		// TODO parallelizzare
		for(i=0, i<#seatRequest.seat, i++)
		{			
			// send lock-in request to participant
			OtherServer.location = seatRequest.seat[i].server;
			lockRequest.seat[0].number = seatRequest.seat[i].number;
			lockRequest.seat[0].flightID = seatRequest.seat[i].flightID;
			lockRequest.tid << tid;
			println@Console("Richiedo il posto "+lockRequest.seat[0].number+" del volo "+lockRequest.seat[0].flightID
				+" al server "+OtherServer.location)();
 			requestLockIn@OtherServer(lockRequest);
			
			println@Console("Ho contattato "+OtherServer.location)();
			
			// Register participants locally
			participants[#participants] = seatRequest.seat[i].server	
		};
		
		// Register participants on the DB for recovery
		scope (join) 
		{
			install (SQLException => println@Console("Partecipante duplicato")());
			// Save all participants in the database through a transaction
			for(i=0, i<#participants, i++)
			{ 
				tr.statement[i] ="INSERT INTO coordtrans(tid, partec, state) VALUES (:tid, :partec, :state)";
				tr.statement[i].tid = transName;
				tr.statement[i].partec = participants[i];
				tr.statement[i].state=0  //REQUESTED
			};
			// salvo tutti i partecipanti da avvisare
			executeTransaction@Database( tr )( ret );
			undef(tr)
		};	
		
		// Give the participants time to process
		sleep@Time(500)();

		// Done requesting locks, start 2 phase commit
		allCanCommit=true;
		println@Console("Partecipanti: "+#participants)();
		
		for(i=0, i<#participants, i++)
		{
			// Ask if can commit
			OtherServer.location = participants[i];
			println@Console("Chiedo canCommit a "+OtherServer.location)();
			canCommit@OtherServer(tid)(answ);
			println@Console(OtherServer.location+" risponde "+answ)();
			if(answ==false)
			{
				allCanCommit=false;
				// Register that participant can't commit
				tr.statement[i] ="UPDATE coordtrans SET state = 1 WHERE tid = :tid AND partec = :partec";
				tr.statement[i].tid = transName;
				tr.statement[i].partec = participants[i];
				tr.statement[i].state=3 //ABORT
			} else
			{
				// Register that participant can commit
				tr.statement[i] ="UPDATE coordtrans SET state = 1 WHERE tid = :tid AND partec = :partec";
				tr.statement[i].tid = transName;
				tr.statement[i].partec = participants[i];
				tr.statement[i].state=1 //CAN COMMIT
			}
		};
		
		scope (canCommitTR)
		{
			install (SQLException => println@Console("Errore nello scrivere i risultati del canCommit")());
			executeTransaction@Database( tr )( ret );
			undef(tr)
		};
		
		
		// if all can commit, proceed; else, abort.
		if(allCanCommit==true)
		{
			finalizeCommit
		}
		else
		{
			abortAll
		}
	}
	
//==================================================================================================
	
	[requestLockIn(lockRequest)] //Partecipant
	{
		// Write a tentative version of the request
		transName = lockRequest.tid.issuer+lockRequest.tid.id;
		scope (writeReq)
		{	
			install (SQLException => println@Console("Errore nella scrittura della richiesta!")() );
			ur = "INSERT INTO trans(tid, seat, flight, newst, committed) SELECT :tid, :seat, :flight, :newst, :committed "
			/*+"WHERE 0 = (SELECT state FROM seat WHERE flight=:flight AND seat=:seat)"*/ ;
			ur.flight = lockRequest.seat.flightID;
			ur.seat = lockRequest.seat.number;
			ur.tid = transName;
			ur.newst = 1; // occupied
			ur.committed = 0; // tentative
			
			update@Database( ur )( ret )
		}
	}
	
//==================================================================================================
	
	[canCommit(tid)(answer)  //Partecipant
	{
		answer = true;
		transName = tid.issuer+tid.id;

		// Get list of changes to commit
		qr = "SELECT flight, seat FROM trans WHERE tid= :tid";
		qr.tid = transName;
		query@Database(qr)(qres);
		
		valueToPrettyString@StringUtils(qres)(str);
		println@Console("qres: "+str)();
				
		
		scope(resourceCheck)
		{
			install(ResourceUnavailable => answer=false;
			println@Console("Risorse non disponibili per la transazione "+transName+"!")());
			// Commit the changes
			i = 0;
			for(row in qres)
			{
				undef(qr);
				qr = "SELECT seat FROM seat WHERE flight = :flight AND seat = :seat AND state = 0";
				qr.flight = row.row.flight;
				qr.seat = row.row.seat;
				query@Database(qr)(resChk);
				
				/*valueToPrettyString@StringUtils(resChk)(str);
				println@Console("ResChk: "+str)();*/
				
				if(#resChk.row == 0) 
				{
					// Resource unavailable
					throw (ResourceUnavailable)
				};
				
				tr.statement[i] = "UPDATE seat SET state = 1 WHERE flight = :flight AND seat = :seat AND state = 0";
				tr.statement[i].flight = row.row.flight;
				tr.statement[i].seat = row.row.seat;
				i++;
				tr.statement[i] = "UPDATE trans SET committed = 1 "+ // committed but not finalized
					"WHERE tid = :tid AND flight = :flight AND seat = :seat";
				tr.statement[i].flight = row.row.flight;
				tr.statement[i].seat = row.row.seat;
				tr.statement[i].tid = transName
			};
			
			scope(canCommitTr)
			{
				install(SQLException => println@Console("Errore nel canCommit")();
					answer = false);
				executeTransaction@Database(tr)(ret)
			};
			
			undef(qr);
			undef(tr)
		}
	}]

//==================================================================================================
	
	[doCommit(tid)(answer) //Partecipant
	{
		// esegui transazione di commit per tid sul db
		transName = tid.issuer+tid.id;
		answer = true;
		
		// Get list of changes to commit
		qr = "SELECT flight, seat FROM trans WHERE tid= :tid";
		qr.tid = transName;
		query@Database(qr)(qres);
		
		/*valueToPrettyString@StringUtils(qres)(str);
		println@Console(str)();*/
		
		// Commit the changes
		i = 0;
		for(row in qres)
		{
			tr.statement[i] = "UPDATE trans SET committed = 2 "+ // finalized commit
				"WHERE tid = :tid AND flight = :flight AND seat = :seat";
			tr.statement[i].flight = row.row.flight;
			tr.statement[i].seat = row.row.seat;
			tr.statement[i].tid = transName;
			i++
		};
		
		scope(canCommitTr)
		{
			install(SQLException => println@Console("Errore nel doCommit")();
				answer = false);
			executeTransaction@Database(tr)(ret)
		};
		
		undef(qr);
		undef(tr);

		println@Console("----> Commit sulla transazione "+tid.issuer+tid.id+"! <----")()

	}]

//==================================================================================================

	[abort(tid)()] //Partecipant
	{
		transName = tid.issuer+tid.id;
		abort
	}
}