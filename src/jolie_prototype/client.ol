include "interfacce.iol"
include "console.iol"
include "time.iol"
include "string_utils.iol"
include "message_digest.iol"
include "database.iol"
include "math.iol"

constants
{
	dbname = "dbClient",
	myLocation = "socket://localhost:7999"
}

outputPort FlightBookingService 
{
  Protocol: sodep
  Interfaces: FlightBookingInterface, Coordinator
}

inputPort ClientService
{
	Location: myLocation
	Protocol: sodep
	Interfaces: ClientInterface, InternalClientInterface
}

//devo conoscere tutte le compagnie e poter chiedere tutti i voli e i posti disponibili per ogni volo


execution{concurrent}

init
{
	with ( connectionInfo ) 
	{
		.username = "sa";
		.password = "";
		.host = "";
		.database = "file:"+dbname;
		.driver = "sqlite"
	};
	connect@Database( connectionInfo )( void );
	
	// DEBUG
	scope ( reset ) 
	{
		install ( SQLException => println@Console("receipts gia vuota")() ); 
			updateRequest ="DROP TABLE receipt";
			update@Database( updateRequest )( ret )
	};   
	
	scope ( createReceipt ) 
	{
		install ( SQLException => println@Console("Receipt table already there")() );
		updateRequest =
			" CREATE TABLE receipt ( "+
				" receipt	TEXT NOT NULL, "+
				" PRIMARY KEY(receipt))";
		update@Database( updateRequest )( ret )
	};
	
	scope ( test ) 
	{
		install ( SQLException => println@Console("errore inserimento")() );
		updateRequest =
			"INSERT INTO receipt(receipt) " +
			"VALUES (:rc)";
		updateRequest.rc = "AJEJEBRAZORF666";
		update@Database( updateRequest )( ret )
	};
	
	println@Console("Client service ready.")();
	showDB
}

define showDB
{
	println@Console("\n\t\t---RECEIPT---")();
	qr = "SELECT * FROM receipt";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	println@Console(str)()
}

define tryBooking
{
	scope(try)
	{
		install (
			IOException => 
				println@Console( "Server unavailable; attempts left: "+(--attemptsLeft) )();
				if (attemptsLeft>0)
				{
					powReq.exponent = maxAttempts-attemptsLeft;
					powReq.base = 2;
					pow@Math(powReq)(multiplier);	
					println@Console("Waiting "+(1500*multiplier)+"ms")();
					sleep@Time(1500*multiplier)();
					tryBooking
				} else
				{
					println@Console( "Server "+seatRequest.lserv[0].server+" unavailable.")()
				},
			InterruptedException => println@Console("Transaction timed out.")()
		);
			
		// Take first server as coordinator...
		FlightBookingService.location = seatRequest.lserv[0].server;
		
		getAvailableSeats@FlightBookingService()(seatList);
		valueToPrettyString@StringUtils(seatList)(str);
		println@Console("Il coordinatore ha i posti "+str)();
		
		seatRequest.clientLocation = myLocation;
		book@FlightBookingService(seatRequest)(response);
		
		if(response.success)
		{
			println@Console("Successo! Ricevuta: "+response.receipt)();
			sleep@Time(2000)();
			
			md5@MessageDigest(response.receipt)(hash);
			getReservedSeats@FlightBookingService(hash)(seatList);
			valueToPrettyString@StringUtils(seatList)(str);
			println@Console("Ho riservato dal coordinatore i posti "+str)();
			sleep@Time(1000)();
			
			println@Console("Annullo la transazione")();
			seatRequest.lserv[0].seat[0].receiptForUndo=response.receipt;
			seatRequest.lserv[0].seat[1].receiptForUndo=response.receipt;
			seatRequest.lserv[1].seat[0].receiptForUndo=response.receipt;
			book@FlightBookingService(seatRequest)(response);
			if(response.success)
			{
				println@Console("Annullato con successo. Ricevuta: "+response.receipt)()
			}else
			{
				println@Console("Annullamento fallito!")()
			}
		} else
		{
			println@Console("Transazione fallita.")()
		}

	}
}

main 
{
	[book(seatRequest)]
	{
		maxAttempts = 4;
		attemptsLeft = maxAttempts;
		tryBooking;
		showDB
	}
	
	[canCommit(receipt)(answer)
	{
		// save receipt
		install 
		( 
			SQLException => println@Console("errore inserimento")();
			answer = false
		);
		updateRequest =
			"INSERT INTO receipt(receipt) " +
			"VALUES (:rc)";
		updateRequest.rc = receipt;
		update@Database( updateRequest )( ret );
		println@Console("Can commit.")();
		answer = true
	}]
}