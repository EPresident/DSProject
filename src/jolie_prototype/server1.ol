include "interfacce.iol"
include "console.iol"
include "string_utils.iol"
include "runtime.iol"
include "network_service.iol"
include "time.iol"
include "database.iol"
include "message_digest.iol"
include "math.iol"

constants
{
	serverName="Alaitalia",
	dbname = "db1",
	myLocation = "socket://localhost:8000",
	locatSubServ= "socket://localhost:8003",
	participantTimeout = 60000,
	coordinatorTimeout = 60000,
	maxGDAttempts = 5
}

interface TimeoutInterface {
  OneWay:
    timeout ( undefined )
}

inputPort LocalInput {
  Location : "local"
  Interfaces: TimeoutInterface
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

outputPort Client 
{
  Protocol: sodep
  Interfaces: ClientInterface
}

type tidcount: void{
	.tid: string
	.count: int
}

interface Spawn {
  RequestResponse:
    spawnCanCommit( tidcount )( bool ) throws InterruptedException,
    spawnDoCommit( tidcount )( void ),
    spawnAbort( tidcount )( void ),
    spawnReqLock( tidcount )( void ) throws InterruptedException
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
	
	with ( connectionInfo ) 
	{
		.username = "sa";
		.password = "";
		.host = "";
		.database = "file:"+dbname;
		.driver = "sqlite"
	};
	
	connect@Database( connectionInfo )( void );
	
	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	//!!			TEST resetto il DB							  !!
	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	scope ( resets ) 
	{
		install ( SQLException => println@Console("seat già vuota")() ); 
			updateRequest ="DROP TABLE seat";
			update@Database( updateRequest )( ret )
	};   
	scope ( resett ) 
	{
		install ( SQLException => println@Console("trans già vuota")() ); 
			updateRequest ="DROP TABLE trans";
			update@Database( updateRequest )( ret )
	};  
	scope ( resetc ) 
	{
		install ( SQLException => println@Console("coordTrans già vuota")() ); 
			updateRequest ="DROP TABLE coordTrans";
			update@Database( updateRequest )( ret )
	};  
    scope ( resettr ) 
	{
		install ( SQLException => println@Console("transReg già vuota")() ); 
			updateRequest ="DROP TABLE transreg";
			update@Database( updateRequest )( ret )
	};   

    
	scope ( createTables ) 
	{
		install ( SQLException => println@Console("Seat table already there")() );
		updateRequest =
			" CREATE TABLE \"seat\" ( "+
				" `flight`	TEXT NOT NULL, "+
				" `seat`	INTEGER NOT NULL, "+
				" `state`	INTEGER NOT NULL DEFAULT 0, "+
				" `hash` TEXT, "+
				" PRIMARY KEY(flight,seat))";
		update@Database( updateRequest )( ret )
	};
	scope ( createTablet ) 
	{
		install ( SQLException => println@Console("Transact table already there")() );
		updateRequest =
			" CREATE TABLE \"trans\" ( "+
			" `tid`	TEXT NOT NULL, "+
			" `seat`	INTEGER NOT NULL, "+
			" `flight`	TEXT NOT NULL, "+
			" `oldstate`	INTEGER, "+
			" `newstate`	INTEGER, "+
			" `newhash`	TEXT, "+
			" `committed` INTEGER NOT NULL DEFAULT 0, "+ // 0 = TENTATIVE, 1 = COMMITTED
			" PRIMARY KEY(seat,flight))";
		update@Database( updateRequest )( ret )
	};
	scope ( createTablec ) 
	{
		install ( SQLException => println@Console("Coord table already there")() );
		updateRequest =
			" CREATE TABLE \"coordtrans\" ( "+
				" `tid`	TEXT, "+
				" `partec`	TEXT, "+
				" `cid`	TEXT, "+
				" `state`	INTEGER NOT NULL DEFAULT 0, " + // 0=REQUESTED, 1=CAN COMMIT, 2=COMMITTED, 3=ABORT
				" PRIMARY KEY(tid,partec))";
		update@Database( updateRequest )( ret )
	};
	scope ( createTransReg ) 
	{
		install ( SQLException => println@Console("Transaction registry already there")() );
		updateRequest =
			" CREATE TABLE \"transreg\" ( "+
				" `tid`	TEXT, "+
				" `coord`	TEXT, "+
				" `cid`	TEXT, "+
				" PRIMARY KEY(tid))";
		update@Database( updateRequest )( ret )
	};
	
	scope ( createCounter ) 
	{
		install ( SQLException => println@Console("Counter already there")() );
		updateRequest =
			" CREATE TABLE counter ( "+
				" counter	INTEGER, "+
				" PRIMARY KEY(counter))";
		update@Database( updateRequest )( ret )
	};
	
	//per ora creo i voli se non presenti
	
	scope ( v1 ) 
	{
		install ( SQLException => println@Console("volo presente")() );
		updateRequest =
			"INSERT INTO seat(flight, seat, state) " +
			"VALUES (:flight, :seat, :state)";
		updateRequest.flight = "AZ0123";
		updateRequest.seat = 69;
		updateRequest.state = 0;
		update@Database( updateRequest )( ret )
	};
	
	scope ( v2 ) 
	{
		install ( SQLException => println@Console("volo presente")() );
		updateRequest =
			"INSERT INTO seat(flight, seat, state) " +
			"VALUES (:flight, :seat, :state)";
		updateRequest.flight = "AZ0123";
		updateRequest.seat = 70;
		updateRequest.state = 0;
		update@Database( updateRequest )( ret )
	};
	
	scope ( v3 ) 
	{
		install ( SQLException => println@Console("volo presente")() );        
		updateRequest =
			"INSERT INTO seat(flight, seat, state) " +
			"VALUES (:flight, :seat, :state)";
		updateRequest.flight = "AZ4556";
		updateRequest.seat = 42;
		updateRequest.state = 0;
		update@Database( updateRequest )( ret )
	};
	
	scope ( v4 ) 
	{
		install ( SQLException => println@Console("volo presente")() ); 
		updateRequest =
			"INSERT INTO seat(flight, seat, state) " +
			"VALUES (:flight, :seat, :state)";
		updateRequest.flight = "AZ4556";
		updateRequest.seat = 44;
		updateRequest.state = 0;
		update@Database( updateRequest )( ret )
	};
		
	coordinatorRecovery;
	transactionRecovery
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
	participants -> global.openTrans.(transName).participant;
	if(#participants > 0)
	{
		for(i=0, i<#participants, i++)
		{
			// Register that the transaction has aborted
			tr.statement[i] ="UPDATE coordtrans SET state = 3 "+ // ABORTED
			"WHERE tid = :tid AND partec = :partec";
			tr.statement[i].tid = transName;
			tr.statement[i].partec = participants[i]
		};
	
		scope (abortAllTrans)
		{
			install
			(
				SQLException => 
					println@Console(transName+": FATAL ERROR - SQL Exception during abortAll ")();
					halt@Runtime(1)(),
				IOException => 
					println@Console(transName+": FATAL ERROR - DB unreachable")();
					halt@Runtime(1)()
			);
			executeTransaction@Database(tr)(ret)
		};
		undef(tr);
	
		// Ask all participants to abort the transaction
		println@Console("\t"+transName+": begin concurrent abort")();
		req.tid = transName;
		req.count=#participants-1;
		spawnAbort@Self(req)();
		
		queryRequest ="SELECT count(*) AS count FROM coordtrans WHERE tid= :tid " ;
		queryRequest.tid = transName;
		query@Database( queryRequest )( queryResult );
			
		serverfail=queryResult.row.count
	};
        
	println@Console(transName+": ABORTED! Errors: "+serverfail)()
}

define finalizeCommit
{
	// All participants can commit; proceed finalizing the commit phase by sending doCommit
	println@Console("\t"+transName+": all "+#participants+ " participants can commit.")();
	
	// Register commit in progress  
	for(i=0, i<#participants, i++)
	{
		tr.statement[i] ="UPDATE coordtrans SET state = 2 "+ // DO COMMIT
		"WHERE tid = :tid AND partec = :partec";
		tr.statement[i].tid = transName;
		tr.statement[i].partec = participants[i]
	};
	
	scope (doCommitTrans)
	{
		install
		(
			SQLException => 
				println@Console(transName+": FATAL ERROR - SQL Exception during commit finalization")();
				halt@Runtime(1)(),
			IOException => 
				println@Console(transName+": FATAL ERROR - DB unreachable")();
				halt@Runtime(1)()
		);
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
	
	println@Console(transName+": TRANSACTION SUCCESSFUL! Unresponsive participants: "+serverfail)()
}

define showDBS
{
	dump = "\t\t=== Database Dump ===";
	dump += "\n\t\t---SEAT---";
	qr = "SELECT * FROM seat";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	dump += "\n"+str;
	
	dump += "\n\t\t---TRANS---";
	qr = "SELECT * FROM trans";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	dump += "\n"+str;
	
	dump += "\n"+"\t\t---COORDTRANS---";
	qr = "SELECT * FROM coordTrans";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	dump += "\n"+str;
	
	dump += "\n"+"\t\t---TRANSREG---";
	qr = "SELECT * FROM transreg";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	dump += "\n"+str;
	
	dump += "\n"+"\t\t---COUNTER---";
	qr = "SELECT * FROM counter";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	dump += "\n"+str+"\n";
	
	println@Console(dump)()
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

	//esegui transazione di abort per tid sul db
	tr.statement[0] ="UPDATE seat SET state = 0, "+
		" hash = (SELECT trans.newhash FROM trans  "+
		" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) "+
		" WHERE EXISTS ( SELECT * FROM trans  "+
		" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) ";
	tr.statement[0].tid = transName;  
	
	tr.statement[1] ="DELETE FROM trans WHERE tid= :tid";
	tr.statement[1].tid = transName;
	
	tr.statement[2] ="DELETE FROM transreg WHERE tid= :tid";
	tr.statement[2].tid = transName;
	
	install 
	(
		IOException => println@Console( "Database non disponibile quindi non posso rimuovere e devo propagare l'eccezione al coordinatore in modo che mi ricontatti quando sarà possibile")(),
		//throw(fault) al coordinatore
		SQLException => println@Console( "Impossibile sql abort partec")()
	);
	executeTransaction@Database( tr )( ret );
	
	println@Console("Abortita la transazione "+tid+"!")()
}

define doCommit
{
	// Variable transName must be defined
	
	tr.statement[0] ="UPDATE seat SET state = (SELECT trans.newstate FROM trans "+
		" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid), "+
		" hash = (SELECT trans.newhash FROM trans  "+
		" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) "+
		" WHERE EXISTS ( SELECT * FROM trans  "+
		" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) ";
	tr.statement[0].tid = transName;  
	
	tr.statement[1] =    "DELETE FROM trans WHERE tid= :tid";
	tr.statement[1].tid = transName;
	
	tr.statement[2] =    "DELETE FROM transreg WHERE tid= :tid";
	tr.statement[2].tid = transName;
	
	install // scope main
	(
		IOException => 
			println@Console( transName+": FATAL ERROR - DB unreachable")();
			answer = false,
		SQLException => 
			println@Console( transName+": FATAL ERROR - SQL error in doCommit")();
			answer = false
	);
	executeTransaction@Database( tr )( ret );
	
	answer = true; //rimuovere RR
	
	println@Console(transName+": COMMIT!")()
}

define coordinatorRecovery
{
	// Look for leftover transactions
	// prefix cr_ is to avoid variable clashes with abort procedure
	println@Console("\t\t---COORDINATOR RECOVERY---")();
	cr_qr = "SELECT tid, partec, cid FROM coordTrans " +
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
				tReq.tid = tid;
				tReq.cid = cr_row.cid;
				abort@OtherServer(tReq)()
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
	
	// Recover transaction counter
	cr_qr = "SELECT * FROM counter";
	query@Database(cr_qr)(cr_qres);
	
	if(#cr_qres.row > 0)
	{
		synchronized(id)
		{
			global.id = cr_qres.row[0].counter
		}
	} else
	{
		// Counter table is empty
		synchronized(id)
		{
			global.id = 0
		};
		cr_ur = "INSERT INTO counter(counter) VALUES (0)";
		update@Database(cr_ur)(cr_ures)
	};
	
	println@Console("\t\t--- Coordinator recovery done.---")()//;	
}

define tryGetDecision
{
	// Must be called inside transactionRecovery
	scope(getDecision)
	{
		install
		(
			default =>
				powReq.exponent = gDAttempts;
				powReq.base = 2;
				pow@Math(powReq)(multiplier);
				gDAttempts++;
				println@Console("\tgetDecision error for"+transName)();
				if(gDAttempts < maxGDAttempts)
				{
					sleep@Time(15000*multiplier)(); // Exponential backoff
					tryGetDecision
				} else
				{
					println@Console("\tERROR! can't getDecision for "+transName)()
				}
		);
		println@Console("\tGet decision for "+transName+"...")();
		Coordinator.location = row.coord;
		getDecision@Coordinator(row.tid)(doCommit);
		
		if ( doCommit )
		{
			// do commit
			println@Console("\t...committing "+transName)();
			doCommit
		} else
		{
			// abort
			println@Console("\t...aborting "+transName)();
			abort
		}
	}
}

define transactionRecovery
{
	println@Console("\t\t---PARTICIPANT RECOVERY---")();
	// Find open transactions
	qr = "SELECT * FROM transreg";
	query@Database(qr)(qres);
	
	for(row in qres.row)
	{
		// If modifications were ready to commit, ask the coordinator for the decision 
		// Else, wait for a possible canCommit, then abort
		transName = row.tid;
		
		qr = "SELECT committed FROM trans WHERE tid = :tid";
		qr.tid = row.tid;
		query@Database(qr)(qres2);
		
		if(qres2.row[0].state == 1)
		{
			gDAttempts = 0; //# of getDecision attemps done so far
			tryGetDecision
		} else
		{
			// wait for canCommit; if it doesn't arrive, abort
			println@Console("Waiting for canCommit...")();
			sleep@Time(1000)();
			
			// recycle previous query
			query@Database(qr)(qres2);
			
			if(qres2.row[0].state != 1) // canCommit hasn't arrived
			{
				println@Console("\t...aborting "+transName)();
				abort
			}
		}
	};
	println@Console("\t\t---Participant recovery done.---")()
}

define checkCID
{
	// cid = CoordinatorID		tid = TransactionID 
	// Checks if the cid passed with a request for tid matches the registered cid for tid
	// In case of mismatch InvalidCID is thrown
	// If no cid is assigned to tid MissingCID is thrown
	// The cid to be checked must be in the receivedCID variable!
	// The tid to be checked must be in the transName variable!
	qr = "SELECT cid FROM transreg WHERE tid = :tid";
	qr.tid = transName;
	query@Database(qr)(qres);
	
	if( #qres.row == 0 )
	{
		throw(MissingCID)
	};
	if ( receivedCID != qres.row[0].cid )
	{
		throw(InvalidCID)
	}
}

define checkInterrupt
{
	// transName must be defined
	if (global.openTrans.(transName).interrupt)
	{
		throw (InterruptedException)
	}
}

define interrupt
{
	synchronized(interrupted)
	{
		global.openTrans.(transName).interrupt = true
	}
}

/*
==================================================================================================
||																								||
||											MAIN												||
||																								||
==================================================================================================
*/


main 
{	
	[timeout(msg)]
	{
		transName = msg.tid;
		if(msg.coordinator && is_defined(global.openTrans.(transName)) )
		{
			println@Console("Timeout for "+transName+"!")();
			interrupt
		};
		if(!msg.coordinator)
		{
			// Participant timeout
			// TODO
			transName = msg.tid;
			println@Console(transName+": TIMEOUT!")();
			//transactionRecovery
			
			qr = "SELECT committed FROM trans WHERE tid = :tid";
			qr.tid = transName;
			query@Database(qr)(qres);
				
			if(qres2.row[0].state == 1)
			{
				// If I said I can commit, I can't go back until told otherwise
				gDAttempts = 0; //# of getDecision attemps done so far
				tryGetDecision
			} else
			{
				abort
			}
		}
	}

	[book(seatRequest)(response)  //Coordinator
	{	
		install
		( 
			InterruptedException =>
				println@Console(transName+" interrupted by timeout!")();
				abortAll;
				throw (InterruptedException) // @ client
		);

		synchronized (id)
		{
			transName = serverName+(++global.id)
		};
		transInfo.tid = transName;
		transInfo.coordLocation = myLocation;
		
		scope(saveCounter)
		{
			install(SQLException => println@Console("Can't save counter!")());
			// Save transaction counter
			ur = "UPDATE counter SET counter = :ctr WHERE 0=0";
			ur.ctr = global.id;
			update@Database(ur)(ures)
		};
		
		timeoutReq = coordinatorTimeout; // timeout after a minute
		timeoutReq.message.tid = transName;
		timeoutReq.message.coordinator = true;
		setNextTimeout@Time(timeoutReq);
		
		global.openTrans.(transName) << transInfo;
		global.openTrans.(transName).seatRequest << seatRequest;
		
		getRandomUUID@StringUtils()(response.receipt); // Generate receipt
		synchronized (transName) // Calculate hash
		{
			md5@MessageDigest(response.receipt)(global.openTrans.(transName).receiptHash)
		};	
		println@Console("=====> Opened transaction "+transName)();
		participants -> global.openTrans.(transName).participant; 
		
		// Request lock-ins
		req.tid = transName;
		req.count=#seatRequest.lserv-1;
		spawnReqLock@Self(req)();
                
		// Give the participants time to process
		sleep@Time(500)();
		
		// Done requesting locks, start 2 phase commit
		println@Console("\t"+transName+": "+#participants+" participants.")();
		println@Console("\t"+transName+": begin concurrent canCommit.")();
		
		req.tid = transName;
		req.count=#participants-1;
		spawnCanCommit@Self(req)(allCanCommit);
                
		if(allCanCommit)
		{
			// Check if the client is still alive and well
			Client.location = seatRequest.client;
			scope(clientCheck)
			{
				install
				( 
					default => println@Console(transName+": client unresponsive! Aborting.")();
					allCanCommit=false
				);
				canCommit@Client(response.receipt)(answer);
				allCanCommit = answer
			}
		};
		
		// if all can commit, proceed; else, abort.
		if(allCanCommit==true)
		{
			finalizeCommit;
			response.success = true
		}
		else
		{
			abortAll;
			response.success = false
		}
	}]
	{	
		undef(global.openTrans.(transName));
		showDBS
	}
	
	
	[spawnReqLock(tc)()  //Coordinator
	{
		transName = tc.tid;
		checkInterrupt;
		if ( tc.count >= 0 )
		{
			{
				participants -> global.openTrans.(transName).participant;
				// send lock-in request to participant
				transInfo.tid = transName;
				transInfo.coordLocation = myLocation;
				OtherServer.location = global.openTrans.(transName).seatRequest.lserv[tc.count].server;
				
				lockRequest.seat << global.openTrans.(transName).seatRequest.lserv[tc.count].seat;
				lockRequest.transInfo << transInfo;
				lockRequest.receiptHash = global.openTrans.(transName).receiptHash;
				// each participant is given a unique ID for this coordinator
				getRandomUUID@StringUtils()(lockRequest.cid); 
				
				// Register participants
				i = #participants;
				participants[i] = global.openTrans.(transName).seatRequest.lserv[tc.count].server;
				participants[i].cid = lockRequest.cid;   
					
				scope (join) 
				{
					install 
					(
						SQLException => println@Console(transName+": SQL error - possible duplicate participant. Aborting")();
						interrupt;
						throw (InterruptedException),
						IOException => println@Console(transName+": FATAL ERROR - DB unreachable")();
						halt@Runtime(1)()
					);
					
					// Save participant in the database through a transaction
					tr.statement[0] ="INSERT INTO coordtrans(tid, partec, cid, state) " +
						"VALUES (:tid, :partec, :cid, :state)";
					tr.statement[0].tid = transName;
					tr.statement[0].partec = OtherServer.location;
					tr.statement[0].cid = lockRequest.cid;
					tr.statement[0].state=0;  //REQUESTED
			
					executeTransaction@Database( tr )( ret );
					undef(tr);
					scope(req)
					{
						install 
						(
							IOException => println@Console(transName+": IO Error - server "+participant[tc.count]+" unresponsive. Aborting")();
							interrupt;
							throw (InterruptedException)
						);
						requestLockIn@OtherServer(lockRequest)
						//println@Console("Ho contattato "+OtherServer.location)()
					}
				}
			}
			
			|   
			
			{   
				d.tid <<tc.tid;
				d.count =tc.count-1;
				spawnReqLock@Self(d)(resp)
			}
		}
	}]
	
	[requestLockIn(lockRequest)] //Partecipant
	{
		// Write a tentative version of the request
		transName = lockRequest.transInfo.tid;
		
		// set timeout
		timeoutReq = participantTimeout; // timeout after a minute
		timeoutReq.message.tid = transName;
		timeoutReq.message.coordinator = false;
		setNextTimeout@Time(timeoutReq);
		
		scope(lock)
		{		
			install 
			(
				SQLException => println@Console(transName+": esiste già un lock su seat,flight quindi fallisco")(), //TODO
				IOException => 
					println@Console(transName+": FATAL ERROR - DB unresponsive")()
			);
			
			//verificare la semantica in caso di errori negli update della stessa transazione
			for(i=0, i<#lockRequest.seat, i++)
			{
				// Record the changes to be done
				tr.statement[i] = "INSERT INTO trans(tid, seat,flight, oldstate, newstate, newhash, committed) SELECT :tid, :seat, :flight, :oldstate, :newstate, :newhash , 0 "
				+"WHERE :oldstate = (SELECT state FROM seat WHERE flight=:flight AND seat=:seat)" ;
				tr.statement[i].flight = lockRequest.seat[i].flightID;
				tr.statement[i].seat = lockRequest.seat[i].number;
				tr.statement[i].tid = transName;
				if  ( is_defined( lockRequest.seat[i].receiptForUndo ) ){ //cancellazione
					println@Console("\t"+transName+": richiesto annullamento")();
					tr.statement[i] = tr.statement[i]+" AND :hash = (SELECT hash FROM seat WHERE flight=:flight AND seat=:seat)";
					tr.statement[i].newstate = 0;
					tr.statement[i].oldstate = 1;
					tr.statement[i].newhash = "";
					md5@MessageDigest(lockRequest.seat[i].receiptForUndo)(tr.statement[i].hash)
				}else{  //prenotazione
					println@Console("\t"+transName+": richiesta prenotazione")();
					tr.statement[i].newstate = 1;
					tr.statement[i].oldstate = 0;
					tr.statement[i].newhash = lockRequest.receiptHash
				}
			};
			// se non sono riuscito a bloccare tutti i posti li libero tutti
			tr.statement[#lockRequest.seat] = "DELETE FROM trans WHERE tid= :tid AND :count <> (SELECT count(*) FROM trans WHERE tid= :tid) " ;
			tr.statement[#lockRequest.seat].tid=transName;
			tr.statement[#lockRequest.seat].count=#lockRequest.seat;

			tr.statement[#lockRequest.seat+1] ="UPDATE seat SET state = 1 "+
			" WHERE EXISTS ( SELECT * FROM trans  "+
			" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) ";
			tr.statement[#lockRequest.seat+1].tid = transName;  

			tr.statement[#lockRequest.seat+2] = "INSERT INTO transreg(tid, coord, cid) VALUES (:tid, :coord, :cid) ";
			tr.statement[#lockRequest.seat+2].tid = lockRequest.transInfo.tid;
			tr.statement[#lockRequest.seat+2].coord = lockRequest.transInfo.coordLocation;
			tr.statement[#lockRequest.seat+2].cid = lockRequest.cid;
			
			executeTransaction@Database( tr )( ret )
		}
	}
	
//==================================================================================================
	
	[canCommit(tReq)(answer)  //Partecipant
	{
		transName=tReq.tid;
		
		// Check if cid matches
		scope(cidCheck)
		{
			install
			(
				InvalidCID => answer = false;
				println@Console(transName+": received request with invalid cid!")()
			);
			install
			(
				MissingCID => answer = false;
				println@Console(transName+": received request for a transaction without cid!")()
			);
			receivedCID = tReq.cid;
			checkCID;
	
			// If the transaction ID is present in the database, then the seats are reserved correctly
			install 
			(
				IOException => 
				{
					println@Console(transName+": FATAL ERROR - DB unresponsive")();
					answer=false
				},
				SQLException => 
					println@Console(transName+": FATAL ERROR - SQL Error in canCommit")();
					answer = false
			);
			
			// mi impegno a non cancellare in caso di fault
			tr.statement[0] ="UPDATE trans SET committed = 1 WHERE tid= :tid ";
			executeTransaction@Database( tr )( ret );
			
			// cerca sul db se è presente tid nell'elenco
			queryRequest =
				"SELECT count(*) AS count FROM trans WHERE tid= :tid " ;
			queryRequest.tid = transName;
			query@Database( queryRequest )( queryResult );
			answer = queryResult.row.count!=0
		}
	}]
	
	[spawnCanCommit(tc)(resp)  //Coordinator  // IMPROVE gestire + velocemente l'abort
	{
		transName = tc.tid;
		checkInterrupt;
		if ( tc.count >= 0 )
		{
			{
				{
				participants -> global.openTrans.(transName).participant;
				OtherServer.location = participants[tc.count];
				//println@Console("Chiedo canCommit a "+OtherServer.location  )();
					
					scope(sendMsg)
					{
						install 
						(
							IOException => 
							{
								println@Console(transName+": "+OtherServer.location+" non disponibile per canCommit" )(); 
								resp1=false
							}
						);
						tReq.tid = tc.tid;
						tReq.cid = participants[tc.count].cid;
						canCommit@OtherServer(tReq)(resp1)
						//println@Console(OtherServer.location+" risponde "+resp1)()
					}
                }                                       
				
				|
				
				{   
					d.tid <<tc.tid;
					d.count =tc.count-1;
					spawnCanCommit@Self(d)(respm)
				}
			};
			resp=respm && resp1
		} else  {
			resp= true
		}
	}]

//==================================================================================================
	
	[doCommit(tReq)(answer) //Partecipant
	{
		// esegui transazione di commit per tid sul db
		transName=tReq.tid;
		
		// Check if cid matches
		scope(cidCheck)
		{
			install
			(
				InvalidCID => answer = false;
				println@Console(transName+": received request with invalid cid!")()
			);
			install
			(
				MissingCID => answer = false;
				println@Console(transName+": received request for a transaction without cid!")()
			);
			receivedCID = tReq.cid;
			checkCID;
	
			doCommit
		}
	}]
	
	[spawnDoCommit(tc)()  //Coordinator
	{
		if ( tc.count >= 0 )
		{
			{
				transName = tc.tid;
				participants -> global.openTrans.(transName).participant;
				OtherServer.location = participants[tc.count];
				//println@Console("Mando doCommit a "+OtherServer.location)();
				scope ( docom )
				{
					install 
					(
						IOException => println@Console(transName+": server "+participant[tc.count]+" non disponibile quindi non rimuovo dal db e riprovo più tardi")() // TODO
					);
					tReq.tid = tc.tid;
					tReq.cid = participants[tc.count].cid;
					doCommit@OtherServer(tReq)(answ);

					// Register that participant has committed ??
					// Remove participant from transaction		
					updateRequest ="DELETE FROM coordtrans WHERE tid= :tid AND partec =:partec ";
					updateRequest.tid = transName;
					updateRequest.partec = participants[tc.count];
					scope ( saveresp )
					{
						install 
						(
							IOException => println@Console(transName+": FATAL ERROR - DB unreachable")(),
							SQLException => println@Console(transName+": FATAL ERROR - SQL Error in spawnDoCommit")()
						);
							update@Database( updateRequest )( ret )
					}
				}
			}   
			
			|   
			
			{   
				d.tid <<tc.tid;
				d.count =tc.count-1;
				spawnDoCommit@Self(d)(resp)
			}
		}
	}]

//==================================================================================================

	[abort(tReq)()] //Partecipant
	{
		transName=tReq.tid;
		
		// Check if cid matches
		scope(cidCheck)
		{
			install
			(
				InvalidCID => println@Console("Received request with invalid cid!")()
			);
			install
			(
				MissingCID => println@Console("Received request for a transaction without cid!")()
			);
			receivedCID = tReq.cid;
			checkCID;
			
			abort
		}
	}
	
	[spawnAbort(tc)()  //Coordinator
	{
		if ( tc.count >= 0 )
		{
			{
				transName = tc.tid;
				participants -> global.openTrans.(transName).participant;
				OtherServer.location = participants[tc.count];
				println@Console("Mando abort a "+OtherServer.location)();
				scope ( abort )
				{
					install 
					(
						IOException => println@Console( "Server "+participant[tc.count]+" non disponibile quindi non rimuovo dal db e riprovo più tardi")()	
					);
					tReq.tid = tc.tid;
					tReq.cid = participants[tc.count].cid;
					abort@OtherServer(tReq)();
					
					// Remove participant from transaction
					updateRequest ="DELETE FROM coordtrans WHERE tid= :tid AND partec =:partec ";
					updateRequest.tid = transName;
					updateRequest.partec = participants[tc.count];
					scope ( saveresp )
					{
						install (
							IOException => println@Console( "Database non disponibile quindi non posso rimuovere dal db e dovrò riprovare più tardi")(),
							SQLException => println@Console( "Impossibile sql abort coord")()
						);
						update@Database( updateRequest )( ret )
					}
				}
			}                                       
			
			|   
			
			{   
				d.tid <<tc.tid;
				d.count =tc.count-1;
				spawnAbort@Self(d)(resp)
			}
		}
	}]	        
	//==================================================================================================
	
	// Get the list of free seats
	[getAvailableSeats()(seatList)
	{
		queryRequest =  "SELECT seat, flight FROM seat WHERE state = 0" ;
		query@Database( queryRequest )( queryResult );
		for(row in queryResult.row)
		{
			i = #seatList.seat;
			seatList.seat[i] = row.seat;
			seatList.seat[i].flight = row.flight
		}
	}]
	
	// Get the list of seats reserved to a specific receiptHash
	[getReservedSeats(hash)(seatList)
	{
		queryRequest =  "SELECT seat, flight FROM seat WHERE hash = :hash" ;
		queryRequest.hash = hash;
		query@Database( queryRequest )( queryResult );
		for(row in queryResult.row)
		{
			i = #seatList.seat;
			seatList.seat[i] = row.seat;
			seatList.seat[i].flight = row.flight
		}
	}]
	
	[getDecision(tid)(answer)
	{
		qr = "SELECT state FROM coordtrans WHERE tid = :tid";
		qr.tid = tid;
		query@Database(qr)(qres);
		
		if(#qres.row == 1 && qres.row[0].state == 2)
		{
			// all participants can commit
			answer = true
		} else
		{
			answer = false
		}
		
	}]
		
}