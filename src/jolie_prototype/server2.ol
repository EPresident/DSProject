include "interfacce.iol"
include "console.iol"
include "string_utils.iol"
include "runtime.iol"
include "network_service.iol"
include "time.iol"
include "database.iol"

constants
{
	serverName="Lefthansa",
	dbname = "db2",
	myLocation = "socket://localhost:8001",
	locatSubServ= "socket://localhost:8004"
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
    spawnAbort( tidcount )( void ),
    spawnReqLock( tidcount )( void )
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
       
    
	scope ( createTables ) 
	{
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
	scope ( createTablet ) 
	{
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
	scope ( createTablec ) 
	{
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
		
	//se ero coordinatore cercare nel database se transazioni che non hanno ricevuto una risposta al commit
	//CODE
	
	/*scope ( recoveryTest ) 
	{
		install ( SQLException => println@Console("vfadasda")() ); 
		updateRequest =
			"INSERT INTO coordTrans(tid, partec, state) " +
			"VALUES (:tid, :partec, :state)";
		updateRequest.tid = "Lefthansa4B0R7";
		updateRequest.partec = "socket://localhost:8001";
		updateRequest.state = 0;
		update@Database( updateRequest )( ret );
		
		updateRequest =
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
		update@Database( updateRequest )( ret );
		
		updateRequest =
			"INSERT INTO coordTrans(tid, partec, state) " +
			"VALUES (:tid, :partec, :state)";
		updateRequest.tid = "Lefthansa4B0R7";
		updateRequest.partec = "socket://localhost:8000";
		updateRequest.state = 0;
		update@Database( updateRequest )( ret )
	};*/
	
	
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

	scope (abortAllTrans)
	{
		install
		(
			SQLException => println@Console("Impossibile sql abortall coord ")(),
			IOException => println@Console("DB non raggiungibile quindi non posso decidere per "+transName)()
		);
		executeTransaction@Database(tr)(ret)
	};
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
		tr.statement[i] ="UPDATE coordtrans SET state = 2 "+ // COMMITTED
		"WHERE tid = :tid AND partec = :partec";
		tr.statement[i].tid = transName;
		tr.statement[i].partec = participants[i]
	};
	
	scope (doCommitTrans)
	{
		install
		(
			SQLException => println@Console("Impossibile sql fincomm coord ")(),
			IOException => println@Console("DB non raggiungibile quindi non posso decidere per "+transName)()
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
	
	println@Console("----> Transaction "+transName+" was successful! Errors: "+serverfail+"<----")()
	
// 	if (serverfail!=0)
// 	{
//             sleep(60000);
//             recoveryCoord
// 	}
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
	println@Console("\t\t--- Coordinator recovery done.---")()//;
	//showDBS
	// sleep(120000);
        // coordinatorRecovery@Self()() //necessario anche se gli altri server non rispondono subito
	
}

// TODO
/*define transactionRecovery
{
    //se memorizzo il nome del coordinatore posso chiamare haveDecided@coord(tid); per ogni transazione con committed 1 e il coordinatore 
    //può registrare i parteciapanti anche sono dopo la fase di cancommit altrimenti il coordinatore deve registrare i partecipanti prima del cancommit e 
    //deve avvisare tutti e attendo il suo recovery 
    
    //eseguo solo al primo avvio o in caso di recuperata connessione al db
    
    //queryRequest =  "SELECT tid,coord FROM trans WHERE committed= 1 " ;
    //query@Database( queryRequest )( queryResult );
    //foreach(){
    //haveDecided@coord(tid)
    //}
    
    //cancello i lock se non mi ero preparato a rispondere si a cancommit dopo 1 secondo per attendere un possibile cancommit che sta arrivando
    sleep(1000);
    tr.statement[0] ="DELETE FROM trans WHERE committed = 0 ";
    install (
            IOException => println@Console( "Database non disponibile quindi non posso eseguire il recovery")(),
            SQLException => println@Console( "Impossibile recover partec")()
    );
    executeTransaction@Database( tr )( ret )
    

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
		transName = serverName+(++id);  // leggere il numero di transazione dal db
		transInfo.tid = transName;
		transInfo.coordLocation = myLocation;
		
		global.openTrans.(transName) << transInfo;
		global.openTrans.(transName).seatRequest << seatRequest;
		
		participants -> global.openTrans.(transName).participant;
                
		println@Console("\nAperta transazione "+transName)();
		
		// Request lock-ins
		req.tid = transName;
		req.count=#seatRequest.lserv-1;
		spawnReqLock@Self(req)();
                
		// Give the participants time to process
		sleep@Time(500)();
		
		// Done requesting locks, start 2 phase commit
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
			
		scope(lock)
		{
		//valueToPrettyString@StringUtils(lockRequest)(str);
		//println@Console(str+ #lockRequest.seat)();
			
			install 
			(
				SQLException => println@Console(" Esiste già un lock su seat,flight quindi fallisco")(),
				IOException => println@Console( "Database non disponibile quindi non eseguo neinte e al cancommit risponderò no")()
			);
			
			//verificare la semantica in caso di errori negli update della stessa transazione
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
		install 
		(
			IOException => {println@Console( "Database non disponibile quindi non sapendo rispondo no")();answer=false},
			//throw al coordinatore
			SQLException => println@Console( "Impossibile sql cancommit partec")()
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
	}]

//==================================================================================================
	
	[doCommit(tid)(answer) //Partecipant
	{
		// esegui transazione di commit per tid sul db
		transName = tid;
	
		tr.statement[0] ="UPDATE seat SET state = (SELECT trans.newst FROM trans "+
				" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid), "+
				" customer = (SELECT trans.newcust FROM trans  "+
				" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) "+
				" WHERE EXISTS ( SELECT * FROM trans  "+
				" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid) ";
		tr.statement[0].tid = transName;  
		
		tr.statement[1] =    "DELETE FROM trans WHERE tid= :tid";
		tr.statement[1].tid = transName;
		
		install 
		(
			IOException => println@Console( "Database non disponibile quindi non posso finalizzare il commit locale e devo propagare l'eccezione al coordinatore in modo che mi ricontatti quando sarà possibile")(),
			//throw al coordinatore
			SQLException => println@Console( "Impossibile sql commit partec")()
		);
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
		
		install 
		(
			IOException => println@Console( "Database non disponibile quindi non posso rimuovere e devo propagare l'eccezione al coordinatore in modo che mi ricontatti quando sarà possibile")(),
			//throw(fault) al coordinatore
			SQLException => println@Console( "Impossibile sql abort partec")()
		);
		executeTransaction@Database( tr )( ret );
		
		println@Console("Abortita la transazione "+tid+"!")()
	}
	
	[spawnCanCommit(tc)(resp)  //Coordinator  // IMPROVE gestire + velocemente l'abort
	{
		if ( tc.count >= 0 )
		{
			{
				{
                    transName = tc.tid;
                    participants -> global.openTrans.(transName).participant;
                    OtherServer.location = participants[tc.count];
                    println@Console("Chiedo canCommit a "+OtherServer.location  )();
                    install (IOException => {println@Console( OtherServer.location+" non disponibile per canCommit" )(); resp1=false});
                    canCommit@OtherServer(tc.tid)(resp1);
                    println@Console(OtherServer.location+" risponde "+resp1)()
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
        
	[spawnDoCommit(tc)()  //Coordinator
	{
		if ( tc.count >= 0 )
		{
			{
				transName = tc.tid;
				participants -> global.openTrans.(transName).participant;
				OtherServer.location = participants[tc.count];
				println@Console("Mando doCommit a "+OtherServer.location)();
				scope ( docom )
				{
					install 
					(
						IOException => println@Console( "Server "+participant[tc.count]+" non disponibile quindi non rimuovo dal db e riprovo più tardi")()
					);
					doCommit@OtherServer(tc.tid)(answ);

					// Register that participant has committed ??
					// Remove participant from transaction		
					updateRequest ="DELETE FROM coordtrans WHERE tid= :tid AND partec =:partec ";
					updateRequest.tid = transName;
					updateRequest.partec = participants[tc.count];
					scope ( saveresp )
					{
						install 
						(
							IOException => println@Console( "Database non disponibile quindi non posso rimuovere dal db e dovrò riprovare più tardi")(),
							SQLException => println@Console( "Impossibile sql docom coord")()
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
					abort@OtherServer(tc.tid)();
					
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
	        
	[spawnReqLock(tc)()  //Coordinator
	{
		if ( tc.count >= 0 )
		{
			{
				transName = tc.tid;
				participants -> global.openTrans.(transName).participant;
				// send lock-in request to participant
				transInfo.tid = transName;
				transInfo.coordLocation = myLocation;
				OtherServer.location = global.openTrans.(transName).seatRequest.lserv[tc.count].server;
				
				lockRequest.seat << global.openTrans.(transName).seatRequest.lserv[tc.count].seat;
				lockRequest.transInfo << transInfo;
				
				// Register participants
				participants[#participants] = global.openTrans.(transName).seatRequest.lserv[tc.count].server;
					
				scope (join) 
				{
					install 
					(
						SQLException => println@Console("Partecipante duplicato quindi input non valido e abortisco la transazione")(), //aggiungere
						IOException => println@Console( "Database non disponibile 	quindi abortisco la transazione")() //aggiungere
					);
					// Save participant in the database through a transaction

					tr.statement[0] ="INSERT INTO coordtrans(tid, partec, state) VALUES (:tid, :partec, :state)";
					tr.statement[0].tid = transName;
					tr.statement[0].partec = OtherServer.location;
					tr.statement[0].state=0;  //REQUESTED
			
					executeTransaction@Database( tr )( ret );
					undef(tr);
					scope(req)
					{
						install 
						(
							IOException => println@Console( "Server "+participant[tc.count]+" non disponibile quindi abortisco al transazione")() //aggiungere
						);
						requestLockIn@OtherServer(lockRequest);
						println@Console("Ho contattato "+OtherServer.location)()
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
}