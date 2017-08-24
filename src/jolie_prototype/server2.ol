include "interfacce.iol"
include "console.iol"
include "string_utils.iol"
include "runtime.iol"
include "network_service.iol"
include "time.iol"
include "database.iol"

inputPort FlightBookingService 
{
  Location: "socket://localhost:8001"
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
	serverName="Lefthansa"
}

init
{
	println@Console("Server "+serverName+" initialized.")();
	global.id = 0;
	with ( connectionInfo ) {
            .username = "sa";
            .password = "";
            .host = "";
           .database = "file:db2";
           .driver = "sqlite"
        };
        connect@Database( connectionInfo )( void );

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
                " `newst`	TEXT, "+
                " PRIMARY KEY(tid,seat,flight))";
            update@Database( updateRequest )( ret )
        };
        scope ( createTablec ) {
            install ( SQLException => println@Console("Coord table already there")() );
            updateRequest =
                " CREATE TABLE \"coordtrans\" ( "+
                " `tid`	TEXT, "+
                " `partec`	TEXT, "+
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
          
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        //!! per ora rendo tutti voli già presenti in db disponibili !!!
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
        scope ( reset ) {
        install ( SQLException => println@Console("non so")() ); 
        updateRequest ="UPDATE seat SET state = 0 ";
        update@Database( updateRequest )( ret )
        }        
        
        //se ero coordinatore cercare nel database se transazioni che non hanno ricevuto una risposta al commit
        //CODE
        


	
}

define abortAll
{
	// Ask all participants to abort the transaction
	for(i=0, i<#participants, i++)
	{
		OtherServer.location = participants[i];
		println@Console("Mando abort a "+OtherServer.location)();
		abort@OtherServer(tid)()
	};
	println@Console("Transaction "+transName+" aborted!")()
}

define finalizeCommit
{
	// All participants can commit; proceed finalizing the commit phase by sending doCommit
	println@Console("Tutti i "+#participants+ "partecipanti possono fare il commit.")();
	serverfail=0;
	
	// Save all participants in the database through a transaction
	for(i=0, i<#participants, i++)
	{ 
		tr.statement[i] ="INSERT INTO coordtrans(tid, partec) VALUES (:tid, :partec)";
		tr.statement[i].tid = transName;
		tr.statement[i].partec = participants[i]
	};
	// salvo tutti i partecipanti da avvisare
	executeTransaction@Database( tr )( ret );
	
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
			// rimuovo quelli che hanno risposto
			println@Console(OtherServer.location+" risponde "+answ)();
			updateRequest ="DELETE FROM coordtrans WHERE tid= :tid AND partec =:partec ";
			updateRequest.tid = transName;
			updateRequest.partec = participants[i];
			update@Database( updateRequest )( ret );
			
			undef(participants[i]) // rimuovo anche anche in locale
		}
	};
	println@Console("Transaction "+transName+" was successful! Errors: "+serverfail)()
}

main 
{
	[book(seatRequest)(tid)  //Coordinator
	{
		tid.issuer = serverName;
		tid.id = ++global.id;
		tid.location = ""+FlightBookingService.Location
	}]
	{
		transName = tid.issuer+tid.id;		
		
		global.openTrans.(transName) << tid;
		println@Console("Aperta transazione "+transName)();

		participants -> global.openTrans.(transName).participant;
		
		// Request lock-ins
		for(i=0, i<#seatRequest.seat, i++)
		{
			// also register participants
			participants[#participants] = seatRequest.seat[i].server;
			
			// Register participants to the transaction on the DB for recovery
			scope (join) 
			{
				install (SQLException => println@Console("Errore nella registrazione dei partecipanti!")() );
				updateRequest ="INSERT INTO coordtrans(tid, partec) VALUES (:tid, :partec)";
				updateRequest.tid = transName;
				updateRequest.partec = seatRequest.seat[i].server;
				update@Database( updateRequest )( ret )
			};
			
			// send lock-in request to participant
			OtherServer.location = seatRequest.seat[i].server;
			lockRequest.seat[0].number = seatRequest.seat[i].number;
			lockRequest.seat[0].flightID = seatRequest.seat[i].flightID;
			lockRequest.tid << tid;
			println@Console("Richiedo il posto "+lockRequest.seat[0].number+" del volo "+lockRequest.seat[0].flightID
				+" al server "+OtherServer.location)();
 			requestLockIn@OtherServer(lockRequest);
			
			println@Console("Ho contattato "+OtherServer.location)()
		};
		
		// Give the participants time to process
		sleep@Time(2000)();

		// Done requesting locks, start 2 phase commit
		allCanCommit=true;
		println@Console("Partecipanti: "+#participants)();
		valueToPrettyString@StringUtils(participants)(str);
		println@Console(str)();

		for(i=0, i<#participants, i++)
		{
			// Ask if can commit
			OtherServer.location = participants[i];
			println@Console("Chiedo canCommit a "+OtherServer.location)();
			canCommit@OtherServer(tid)(answ);
			println@Console(OtherServer.location+" risponde "+answ)();
			if(answ==false)
			{
				allCanCommit=false
			}
			
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
	
	[requestLockIn(lockRequest)] //Partecipant
	{
		transName = lockRequest.tid.issuer+lockRequest.tid.id;

                tr.statement[0] = "INSERT INTO trans(tid, seat,flight, newst, newcust) SELECT :tid, :seat, :flight, :newst, :newcust "
                +"WHERE 0 = (SELECT state FROM seat WHERE flight=:flight AND seat=:seat)" ;
                tr.statement[0].flight = lockRequest.seat.flightID;
                tr.statement[0].seat = lockRequest.seat.number;
                tr.statement[0].tid = transName;
                tr.statement[0].newst = 2;
                tr.statement[0].newcust = transName;
                
                tr.statement[1] ="UPDATE seat SET state = 1 WHERE  "+
                        "seat = :seat AND flight = :flight AND state=0";
                tr.statement[1].tid = transName;  
                tr.statement[1].flight = lockRequest.seat.flightID;
                tr.statement[1].seat = lockRequest.seat.number;

                executeTransaction@Database( tr )( ret )
	}
	
		
	[canCommit(tid)(answer)  //Partecipant
	{
		// If the transaction ID is present in the database, then the seats are reserved correctly
		transName = tid.issuer+tid.id;
		// cerca sul db se è presente tid nell'elenco
		queryRequest =
			"SELECT count(*) AS count FROM trans WHERE tid= :tid " ;
		queryRequest.tid = transName;
		query@Database( queryRequest )( queryResult );
		valueToPrettyString@StringUtils(queryResult)(str);
		println@Console(str)();
		answer = queryResult.row.count!=0
	}]
	
	
	[doCommit(tid)(answer) //Partecipant
	{
                // esegui transazione di commit per tid sul db
                transName = tid.issuer+tid.id;

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
                
                answer = true;
                println@Console("Commit sulla transazione "+tid.issuer+tid.id+"!")()

	}]

	[abort(tid)()] //Partecipant
	{
		transName = tid.issuer+tid.id;
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
		
		println@Console("Abortita la transazione "+tid.issuer+tid.id+"!")()
	}
}