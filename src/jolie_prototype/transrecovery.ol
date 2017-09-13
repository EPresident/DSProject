include "interfacce.iol"
include "interfaceservertransrecovery.iol"
include "console.iol"
include "string_utils.iol"
include "time.iol"
include "runtime.iol"
include "database.iol"
include "math.iol"

outputPort Self{
	Interfaces: TransRecovery
}

inputPort Self {
	Location: "local"
	Interfaces: TransRecovery
}

outputPort Coordinator 
{
  Protocol: sodep
  Interfaces: Coordinator
}

outputPort OtherServer 
{
  Protocol: sodep
  Interfaces: FlightBookingInterface
}

execution{concurrent}

init
{
	getLocalLocation@Runtime()( Self.location )
}


define abort 
{
	// Variable transName must be defined

	//esegui transazione di abort per tid sul db
	tr.statement[0] ="UPDATE seat SET state = (SELECT trans.oldstate FROM trans "+
		" WHERE trans.flight = seat.flight AND trans.seat = seat.seat AND trans.tid= :tid), "+
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

main{
	[tryGetDecision(tac)]
        {
            with ( connectionInfo ) 
            {
		.username = "sa2";
		.password = "";
		.host = "";
		.database = "file:"+tac.db;
		.driver = "sqlite"
            };
            connect@Database( connectionInfo )( void );
            
            scope(getDecision)
            {
                install
                (
                    default =>
                        powReq.exponent = tac.att;
                        powReq.base = 2;
                        pow@Math(powReq)(multiplier);
                        println@Console("\tgetDecision error for"+transName)();
                        sleep@Time(15000*multiplier)(); // Exponential backoff;
                        ntac.att=tac.att+1;
                        ntac.tid=tac.tid;
                        ntac.coordloc=tac.coordloc;
                        ntac.db=tac.db;
                        tryGetDecision@Self(ntac)
                );
                println@Console("\tGet decision for "+transName+"...")();
                transName=tac.tid;
                Coordinator.location = tac.coordloc;
                getDecision@Coordinator(tac.tid)(rdoCommit);
    
                if ( rdoCommit )
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
	[recovery(db)]
        {
        
        with ( connectionInfo ) 
	{
		.username = "sa2";
		.password = "";
		.host = "";
		.database = "file:"+db;
		.driver = "sqlite"
	};
	
	connect@Database( connectionInfo )( void );
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
		
		if(qres2.row[0].committed == 1)
		{
                    tac.att=0;//# of getDecision attemps done so far
                    tac.tid=transName;
                    tac.coordloc=row.coord;
                    tac.db=db;
                    tryGetDecision@Self(tac)
		} else
		{
			// wait for canCommit; if it doesn't arrive, abort
			println@Console("Waiting for canCommit...")();
			sleep@Time(1000)();
			
			// recycle previous query
			query@Database(qr)(qres2);
			
			if(qres2.row[0].committed != 1) // canCommit hasn't arrived
			{
				println@Console("\t...aborting "+transName)();
				abort
			}
		}
	};
	println@Console("\t\t---Participant recovery done.---")()
        }
        
        [coordinatorRecovery(db)]
        {
        with ( connectionInfo ) 
	{
		.username = "sa2";
		.password = "";
		.host = "";
		.database = "file:"+db;
		.driver = "sqlite"
	};
	
	connect@Database( connectionInfo )( void );
        
	// Look for leftover transactions
	// prefix cr_ is to avoid variable clashes with abort procedure
	println@Console("\t\t---COORDINATOR RECOVERY---")();
	cr_qr = "SELECT tid, partec, cid, state FROM coordTrans ";
	query@Database(cr_qr)(cr_qres);
	
	for( cr_row in cr_qres.row )
	{	
		cr_deleteEntry = true;
		if( cr_row.partec == myLocation )
		{
			transName = cr_row.tid;
			if (cr_row.state==2){
                                println@Console("Mando doCommit a me stesso.")();
				doCommit
			} else {
                                println@Console("Mando abort a me stesso.")();
				abort
			}
		} else
		{
			OtherServer.location = cr_row.partec;
			tid = cr_row.tid;
			scope (cr_abortReq)
			{
				install(default => println@Console("Errore nel recovery, tengo la entry.")();
					cr_deleteEntry = false);
				tReq.tid = tid;
				tReq.cid = cr_row.cid;
				if (cr_row.state==2){
                                        println@Console("Mando doCommit a "+OtherServer.location)();
					doCommit@OtherServer(tReq)()
				} else {
                                        println@Console("Mando abort a "+OtherServer.location)();
					abort@OtherServer(tReq)()
				}
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
	println@Console("\t\t--- Coordinator recovery done.---")()
        }
} 
