include "interfacce.iol"
include "console.iol"
include "string_utils.iol"
include "runtime.iol"
include "network_service.iol"
include "time.iol"
include "database.iol"

constants
{
	serverName="Alaitalia",
	dbname = "db1",
	myLocation = "socket://localhost:8000",
	locatSubServ= "socket://localhost:8003"
}

inputPort FlightBookingService 
{
  Location: myLocation
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

type tidcount: void{
	.tid: string
	.count: int
}

interface Spawn {
  RequestResponse:
    spawnCanCommit( tidcount )( bool ),
    spawnDoCommit( tidcount )( void ),
    spawnAbort( tidcount )( void )
    //spawnReqLock( tidcount )( void )
}

outputPort Self {
  Location: locatSubServ
  Protocol: sodep
  Interfaces: Spawn
}

inputPort Spawn {
  Location: locatSubServ
  Protocol: sodep
  Interfaces: Spawn
}

execution{concurrent}

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
                    " `customer`TEXT, "+
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
                " `newst`	INTEGER, "+
                " `newcust`	TEXT, "+
                " `committed` INTEGER NOT NULL DEFAULT 0, "+ // 0 = TENTATIVE, 1 = COMMITTED
                " PRIMARY KEY(seat,flight))";
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
			
			/*updateRequest =
				"INSERT INTO trans(tid, seat, flight, newst, committed) " +
				"VALUES (:tid, :seat, :flight, 1, 0)";
			updateRequest.tid = "Lefthansa4B0R7";
			updateRequest.seat = 666;
			updateRequest.flight = "SA0666";
			update@Database( updateRequest )( ret );
			
			updateRequest =
				"INSERT INTO trans(tid, seat, flight, newst, committed) " +
				"VALUES (:tid, :seat, :flight, 1, 1)";
			updateRequest.tid = "Lefthansa4B0R7";
			updateRequest.seat = 999;
			updateRequest.flight = "SA0666";
			update@Database( updateRequest )( ret );*/
			
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
	// Register abort in progress  ?? potrei aver registrato pure prima delle risposte del cancommit come abort ??
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
	println@Console("Begin concurrent abort "+transName)();
	req.tid = transName;
	req.count=#participants-1;
        spawnAbort@Self(req)();
                
        queryRequest ="SELECT count(*) AS count FROM coordtrans WHERE tid= :tid " ;
        queryRequest.tid = transName;
        query@Database( queryRequest )( queryResult );
		
        serverfail=queryResult.row.count;
        
	println@Console("----> Transaction "+transName+" aborted! Errors: "+serverfail+"<----")()
}

define finalizeCommit
{
	// All participants can commit; proceed finalizing the commit phase by sending doCommit
	println@Console("\n --------------------------------------------------------\n"+
	"Tutti i "+#participants+ "partecipanti possono fare il commit.")();
	
        // Register commit in progress  
	for(i=0, i<#participants, i++)
	{
		// Register that the transaction has aborted
		tr.statement[i] ="UPDATE coordtrans SET state = 2 "+ // COMMITTED
		"WHERE tid = :tid AND partec = :partec";
		tr.statement[i].tid = transName;
		tr.statement[i].partec = participants[i]
	};
	
	scope (doCommitTrans)
	{
		install(SQLException => println@Console("Errore nel doCommit")());
		executeTransaction@Database(tr)(ret)
	};
	undef(tr);

	// Ask all participants to commit the transaction
	println@Console("Begin concurrent commit "+transName)();
	req.tid = transName;
	req.count=#participants-1;
        spawnDoCommit@Self(req)();
        
        queryRequest ="SELECT count(*) AS count FROM coordtrans WHERE tid= :tid " ;
		queryRequest.tid = transName;
		query@Database( queryRequest )( queryResult );
		
        serverfail=queryResult.row.count;
        
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

define showInternalState
{
	println@Console("\n\t\t---Open transactions---")();
	valueToPrettyString@StringUtils(global.openTrans)(str);
	println@Console(str)()
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
	for(row in qres.row)
	{
		if(row.committed != 0)
		{
			println@Console("Annullo i cambiamenti...")();
			tr.statement[i] = "UPDATE seat SET state = 0 "+
			"WHERE tid = :tid AND flight = :flight AND seat = :seat";
			tr.statement[i].flight = row.flight;
			tr.statement[i].seat = row.seat;
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
	//showDBS;
	// Look for leftover transactions
	// prefix cr_ is to avoid variable clashes with abort procedure
	println@Console("\t\t---COORDINATOR RECOVERY---")();
	cr_qr = "SELECT tid, partec FROM coordTrans " +
	" WHERE state = 0 OR state = 1 OR state = 3";
	query@Database(cr_qr)(cr_qres);
	
	for( cr_row in cr_qres.row )
	{	
		cr_deleteEntry = true;
		if( cr_row.partec == myLocation )
		{
			println@Console("Mando abort a me stesso.")();
			transName = cr_row.tid;
			abort
		} else
		{
			OtherServer.location = cr_row.partec;
			tid = cr_row.tid;
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
	println@Console("--- Coordinator recovery done.---")()//;
	//showDBS
	// sleep(120000);
        // coordinatorRecovery@Self()() //necessario anche se gli altri server non rispondono subito
	
}

// TODO
/*define transactionRecovery
{

// se non ho ancora risposto si a cancommit dovrei poter abortire liberando subito le risorse bloccate commit 0
// ma se ho risposto si il coordinatore deve avvisarmi anche se è crashato mentre il si era sul canale 
// e quindi non ha salvato l'esito della transazione

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
	[book(seatRequest)(receipt)  //Coordinator
	{
		getRandomUUID@StringUtils()(receipt)
	}]
	{
		transName = serverName+(++id);

		transInfo.tid = transName;
		transInfo.coordLocation = myLocation;
		
		global.openTrans.(transName) << tid;
		println@Console("\nAperta transazione "+transName)();
		println@Console("Posti richiesti: "+#seatRequest.seat+"\n")();

		participants -> global.openTrans.(transName).participant;
		
		// Request lock-ins
		// TODO parallelizzare
		// cecherei di raggruppare a questo livello i lock multipli ad uno stesso server
		for(i=0, i<#seatRequest.seat, i++)
		{			
			// send lock-in request to participant
			OtherServer.location = seatRequest.seat[i].server;
			lockRequest.seat[0].number = seatRequest.seat[i].number;
			lockRequest.seat[0].flightID = seatRequest.seat[i].flightID;
			lockRequest.transInfo << transInfo;
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
		
                println@Console("Begin concurrent cancommit "+transName)();
                req.tid = transName;
                req.count=#participants-1;
                spawnCanCommit@Self(req)(allCanCommit);	
		
		// if all can commit, proceed; else, abort.
		if(allCanCommit==true)
		{
			finalizeCommit
		}
		else
		{
			abortAll
		};
		undef(global.openTrans.(transName))
	}
	
//==================================================================================================
	
	[requestLockIn(lockRequest)] //Partecipant
	{
		// Write a tentative version of the request
		transName = lockRequest.transInfo.tid;
                
                scope(lock){

 		valueToPrettyString@StringUtils(lockRequest)(str);
                 println@Console(str+ #lockRequest.seat)();
                
                install (SQLException => println@Console(" Esiste già un lock su seat,flight quindi fallisco")() );
                for(i=0, i<#lockRequest.seat, i++)
		{
                    tr.statement[i] = "INSERT INTO trans(tid, seat,flight, newst, newcust,committed) SELECT :tid, :seat, :flight, :newst, :newcust , 0 "
                    +"WHERE 0 = (SELECT state FROM seat WHERE flight=:flight AND seat=:seat)" ;
                    tr.statement[i].flight = lockRequest.seat[i].flightID;
                    tr.statement[i].seat = lockRequest.seat[i].number;
                    tr.statement[i].tid = transName;
                    tr.statement[i].newst = 2;
                    tr.statement[i].newcust = transName
                };
                // se non sono riuscito a bloccare tutti i posti li libero tutti
                tr.statement[#lockRequest.seat] = "DELETE FROM trans WHERE tid= :tid AND :count <> (SELECT count(*) FROM trans WHERE tid= :tid) " ;
                tr.statement[#lockRequest.seat].tid=transName;
                tr.statement[#lockRequest.seat].count=#lockRequest.seat;

                tr.statement[#lockRequest.seat+1] ="UPDATE seat SET state = 1 "+
			" WHERE EXISTS ( SELECT * FROM trans  "+
			" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) ";
                tr.statement[#lockRequest.seat+1].tid = transName;  

                executeTransaction@Database( tr )( ret )
                }
	}
	
//==================================================================================================
	
	[canCommit(tid)(answer)  //Partecipant
	{
                transName=tid;
		// If the transaction ID is present in the database, then the seats are reserved correctly
                
                // mi impegno a non cancellare in caso di fault
                tr.statement[0] ="UPDATE trans SET committed = 1 WHERE tid= :tid ";
                executeTransaction@Database( tr )( ret );
                
		// cerca sul db se è presente tid nell'elenco
		queryRequest =
			"SELECT count(*) AS count FROM trans WHERE tid= :tid " ;
		queryRequest.tid = transName;
		query@Database( queryRequest )( queryResult );
		answer = queryResult.row.count!=0
	}]

//==================================================================================================
	
	[doCommit(tid)(answer) //Partecipant
	{
		// esegui transazione di commit per tid sul db
                transName = tid;
		//da questo punto mi impegno a non liberare il lock in nessun caso fino a doCommit o abort
                tr.statement[0] ="UPDATE seat SET state = (SELECT trans.newst FROM trans "+
                        " WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid), "+
                        " customer = (SELECT trans.newcust FROM trans  "+
                        " WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) "+
                        " WHERE EXISTS ( SELECT * FROM trans  "+
                        " WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) ";
                tr.statement[0].tid = transName;  
                
                tr.statement[1] =    "DELETE FROM trans WHERE tid= :tid";
                tr.statement[1].tid = transName;
                
                executeTransaction@Database( tr )( ret );
                
                answer = true; //rimuovere RR
                
		println@Console("----> Commit sulla transazione "+tid+"! <----")()

	}]

//==================================================================================================

	[abort(tid)()] //Partecipant
	{
		transName = tid;
		//esegui transazione di abort per tid sul db
		tr.statement[0] ="UPDATE seat SET state = 0, "+
			" customer = (SELECT trans.newcust FROM trans  "+
			" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) "+
			" WHERE EXISTS ( SELECT * FROM trans  "+
			" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) ";
		tr.statement[0].tid = transName;  
		
		tr.statement[1] ="DELETE FROM trans WHERE tid= :tid";
		tr.statement[1].tid = transName;
		
		executeTransaction@Database( tr )( ret );
		
		println@Console("Abortita la transazione "+tid+"!")()
	}
	
        [spawnCanCommit(tc)(resp)  //Coordinator
	{
            if ( tc.count >= 0 )
            {  
                transName = tc.tid;
                participants -> global.openTrans.(transName).participant;
                {
                    {OtherServer.location = participants[tc.count];
                    println@Console("Chiedo canCommit a "+OtherServer.location  )();
                    canCommit@OtherServer(tc.tid)(resp1);
                    println@Console(OtherServer.location+" risponde "+resp1)()}
                                                        |   {  d.tid <<tc.tid;
                                                            d.count =tc.count-1;
                                                            spawnCanCommit@Self(d)(respm)}};
                resp=respm && resp1
            } else  {
                resp= true
            }
        }]  
        
        [spawnDoCommit(tc)()  //Coordinator
	{
            
            if ( tc.count >= 0 )
            {  
                transName = tc.tid;
                participants -> global.openTrans.(transName).participant;
                {
                    {	OtherServer.location = participants[tc.count];	
                        println@Console("Mando doCommit a "+OtherServer.location)();
                        scope ( docom ){
			install (
				IOException => println@Console( "Server "+participant+" non disponibile 4")();
					sleep@Time(500)() //continua
			);
			
			doCommit@OtherServer(tc.tid)(answ);
			
			// Register that participant has committed ??
                        // Remove participant from transaction
                        updateRequest ="DELETE FROM coordtrans WHERE tid= :tid AND partec =:partec ";
                        updateRequest.tid = transName;
                        updateRequest.partec = participants[tc.count];
                        update@Database( updateRequest )( ret )}}
                                                        |   {  d.tid << tc.tid;
                                                            d.count =tc.count-1;
                                                            spawnDoCommit@Self(d)(resp)}}
            }
        }]      
        
        [spawnAbort(tc)()  //Coordinator
	{
            
            if ( tc.count >= 0 )
            {  
                transName = tc.tid;
                participants -> global.openTrans.(transName).participant;
                {
                    {OtherServer.location = participants[tc.count];
                    println@Console("Mando abort a "+OtherServer.location)();
                    //gestire errore connessione
                    abort@OtherServer(tc.tid)();
		
                    // Remove participant from transaction
                    updateRequest ="DELETE FROM coordtrans WHERE tid= :tid AND partec =:partec ";
                    updateRequest.tid = transName;
                    updateRequest.partec = participants[tc.count];
                    update@Database( updateRequest )( ret )}
                                                        |   {  d.tid <<tc.tid;
                                                            d.count =tc.count-1;
                                                            spawnAbort@Self(d)(resp)}}

            } 
        }]
	
}