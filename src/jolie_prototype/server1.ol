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
	serverName="Alaitalia"
}

init
{
	println@Console("Server "+serverName+" initialized.")();
	global.id = 0;
	//global.flight["AZ0123"].seat[69].free = true;
	//global.flight["AZ0123"].seat[70].free = true;
	with ( connectionInfo ) {
            .username = "sa";
            .password = "";
            .host = "";
            .database = "file:db1";
            .driver = "sqlite"
        };
        connect@Database( connectionInfo )( void );
        //se ero coordinatore cercare nel database se transazioni che non hanno ricevuto una risposta al commit
        //CODE
        
        // crea tabella se non esiste
		scope(createSeatTable){
			install ( SQLException => println@Console("Tabella posti già presente")() );
			updateRequest =
				"CREATE TABLE seat(flight CHAR(6) NOT NULL, " +
				"seat UNSIGNED SMALLINT NOT NULL, state UNSIGNED TINYINT(1) NOT NULL, " +
				"PRIMARY KEY(flight, seat))";
			update@Database( updateRequest )( ret )
		};
		
		scope ( createCoordTransTable ) {
			install ( SQLException => println@Console("Tabella coordinatore transazioni già presente")() );
			updateRequest =
				"CREATE TABLE coordtrans(tid VARCHAR(50) NOT NULL, " +
				"partec VARCHAR(30) NOT NULL, " +
				"PRIMARY KEY(tid, partec))";
			update@Database( updateRequest )( ret )
		};
		
		scope ( createTransTable ) {
			install ( SQLException => println@Console("Tabella transazioni già presente")() );
			updateRequest =
				"CREATE TABLE trans(tid VARCHAR(50) NOT NULL, " +
				"seat UNSIGNED SMALLINT NOT NULL, " +
				"flight CHAR(6) NOT NULL, " +
				"newst VARCHAR(30) NOT NULL, " +
				"PRIMARY KEY(tid))";
			update@Database( updateRequest )( ret )
		};
		
		scope ( addFlights ) {
			install ( SQLException => println@Console("Errore nell'aggiunta dei voli")() );
			updateRequest =
				"INSERT INTO seat(flight, seat, state) " +
				"VALUES (:flight, :seat, :state)";
			updateRequest.flight = "AZ4556";
			updateRequest.seat = 44;
			updateRequest.state = 0;
			update@Database( updateRequest )( ret )
		}
	
}

define abortAll
{
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
        println@Console("Tutti i "+#participants+ "partecipanti possono fare il commit.")();
        serverfail=0;
        for(i=0, i<#participants, i++)
        {
            // salvo tutti i partecipanti da avvisare   FIXME salvare tutti in modo atomico
			scope(updatePartic) {
				install ( SQLException => println@Console("Errore nell'aggiunta dei partecipanti")() );
				updateRequest ="INSERT INTO coordtrans(tid, partec) VALUES (:tid, :partec)";
				updateRequest.tid = transName;
				updateRequest.partec = participants[i];
				update@Database( updateRequest )( ret )
			};
            OtherServer.location = participants[i];
            println@Console("Mando doCommit a "+OtherServer.location)();
            scope ( docom ){
                install (
                    IOException => println@Console( "Server "+participant+" non disponibile 4")();
                        sleep@Time(2000)();  //continua
                        serverfail++
                );
				install ( SQLException => println@Console("Errore nella rimozione dei partecipanti")() );
                
                doCommit@OtherServer(tid)(answ);
                // rimuovo quelli che hanno risposto
                println@Console(OtherServer.location+" risponde "+answ)();
                updateRequest ="DELETE FROM coordtrans WHERE tid= :tid AND partec =:partec ";
                updateRequest.tid = transName;
                updateRequest.partec = participants[i];
                update@Database( updateRequest )( ret )
            }
        };
        println@Console("Transaction "+transName+" was successful! err "+serverfail)()
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
		//valueToPrettyString@StringUtils(global.openTrans)(str);
		//println@Console(str)();
		
		participants -> global.openTrans.(transName).participant;
		
		// Request lock-ins
        for(i=0, i<#seatRequest.seat, i++)
		{
			// also register participants
			participants[#participants] = seatRequest.seat[i].server;
			
			OtherServer.location = seatRequest.seat[i].server;
			lockRequest.seat[0].number = seatRequest.seat[i].number;
			lockRequest.seat[0].flightID = seatRequest.seat[i].flightID;
			lockRequest.tid << tid;
			println@Console("Richiedo il posto "+lockRequest.seat[0].number+" del volo "+lockRequest.seat[0].flightID
				+" al server "+OtherServer.location)();
 			requestLockIn@OtherServer(lockRequest);
			println@Console("Ho contattato "+OtherServer.location)()
		};
		
		println@Console("asdasdadad")();
		sleep@Time(2000)();
                
		// Done requesting locks, start 2 phase commit
		allCanCommit=true;
		println@Console("Partecipanti: "+#participants)();
		valueToPrettyString@StringUtils(participants)(str);
		println@Console(str)();

                for(i=0, i<#participants, i++)
		{
                        OtherServer.location = participants[i];
			println@Console("Chiedo canCommit a "+OtherServer.location)();
			canCommit@OtherServer(tid)(answ);
			println@Console(OtherServer.location+" risponde "+answ)();
			if(answ==false)
			{
				allCanCommit=false
			}
			
		};
                
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
		// Open transaction
		
		//"CASE x WHEN 2 THEN ROLLBACK WHEN 3 THEN ROLLBACK ELSE (UPDATE ) END"
		
		transName = lockRequest.tid.issuer+lockRequest.tid.id;
        scope(lockInReq) {      
			install ( SQLException => println@Console("Errore nel lock in")() );
            updateRequest = "INSERT INTO trans(tid, seat,flight, newst) SELECT :tid, :seat, :flight, :newst "
            +"WHERE 0 = (SELECT state FROM seat WHERE flight=:flight AND seat=:seat)" ;
            updateRequest.flight = lockRequest.seat[0].flightID;
            updateRequest.seat = lockRequest.seat[0].number;
            updateRequest.tid = transName;
            updateRequest.newst = transName;
            update@Database( updateRequest )( res );
            
            updateRequest ="UPDATE seat SET state = 1 WHERE  "+
                    "seat = (SELECT seat FROM trans WHERE tid= :tid) AND "+
                    "flight = (SELECT flight FROM trans WHERE tid= :tid )";
            updateRequest.tid = transName;  
            update@Database( updateRequest )( ret )
        }      
	}
	
		
	[canCommit(tid)(answer)  //Partecipant
	{
                transName = tid.issuer+tid.id;
		// cerca sul db se è presente tid nell'elenco
                println@Console(transName)();
		scope(canCommitQuery) {
				install ( SQLException => println@Console("Errore nella query di canCommit")() );
                queryRequest =
                    "SELECT tid, seat, flight, newst FROM trans WHERE tid= :tid " ;
                queryRequest.tid = transName;
                query@Database( queryRequest )( queryResult );
                valueToPrettyString@StringUtils(queryResult)(str);
                println@Console(str)();
                println@Console(queryResponse.row[0].SEAT)();
                answer = queryResult!=0
		}
	}]
	
	[doCommit(tid)(answer) //Partecipant
	{
                // esegui transazione di commit per tid sul db
                transName = tid.issuer+tid.id;
                //INFORMAZIONI: [server3.ol] Output message TypeMismatch (executeTransaction@Database): Invalid native type for node #Message: expected VOID, found java.lang.String
//                 updateRequest ="BEGIN transact; UPDATE seat SET state = 2 WHERE  "+
//                        "seat = (SELECT seat FROM trans WHERE tid= :tid) AND "+
//                        "flight = (SELECT flight FROM trans WHERE tid= :tid ) ;" +
//                                 "DELETE FROM trans WHERE tid= :tid; COMMIT transact";
//                 updateRequest.tid = transName;
//                 executeTransaction@Database( updateRequest )( ret );
                
            //transazione unica
			scope(doCommitTrans) {
				install ( SQLException => println@Console("Errore nella transazione di doCommit")() );
                updateRequest ="UPDATE seat SET state = 2 WHERE  "+
                        "seat = (SELECT seat FROM trans WHERE tid= :tid) AND "+
                        "flight = (SELECT flight FROM trans WHERE tid= :tid )";
                updateRequest.tid = transName;  
                update@Database( updateRequest )( ret );
                        
                updateRequest =    "DELETE FROM trans WHERE tid= :tid";
                updateRequest.tid = transName;
                update@Database( updateRequest )( ret );
				
				answer = true;
                println@Console("Commit sulla transazione "+tid.issuer+tid.id+"!")()
            }   
                
			
	}]

	[abort(tid)()] //Partecipant
	{
		//esegui transazione di abort per tid sul db
		scope(abortTrans) {
				install ( SQLException => println@Console("Errore nella transazione di abort")() );
                updateRequest ="UPDATE seat SET state = 0 WHERE  "+
                        "seat = (SELECT seat FROM trans WHERE tid= :tid) AND "+
                        "flight = (SELECT flight FROM trans WHERE tid= :tid )";
                updateRequest.tid = transName;  
                update@Database( updateRequest )( ret );
                transName = tid.issuer+tid.id;
                updateRequest ="DELETE FROM trans WHERE tid= :tid";
                updateRequest.tid = transName;
                update@Database( updateRequest )( ret );
		println@Console("Abortita la transazione "+tid.issuer+tid.id+"!")()
		}
	}
}