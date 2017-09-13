include "interfacce.iol"
include "console.iol"
include "time.iol"
include "string_utils.iol"
include "database.iol"

constants
{
	dbname = "dbClient",
	serviceLocation = "socket://localhost:7999"
}

outputPort ClientService
{
	Location: serviceLocation
	Protocol: sodep
	Interfaces: InternalClientInterface
}

execution { single }

define showDB
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
	println@Console("\n\t\t---RECEIPT---")();
	qr = "SELECT * FROM receipt";
	query@Database(qr)(qres);
	valueToPrettyString@StringUtils(qres)(str);
	println@Console(str)()
}

main 
{
	println@Console("Sending request(1)...")();
	request.lserv[0].server="socket://localhost:8001";
	request.lserv[0].seat[0].flightID="AB0123";
	request.lserv[0].seat[0].number=69;
	request.lserv[0].seat[1].flightID="AB0123";
	request.lserv[0].seat[1].number=70;
	request.lserv[1].server="socket://localhost:8000";
	request.lserv[1].seat[0].flightID="AZ4556";
	request.lserv[1].seat[0].number=44;
	request.clientLocation = ClientService.location;
	
	book@ClientService(request);
	println@Console("Done(1).")()
	|
        println@Console("Sending request(2)...")();
	request2.lserv[0].server="socket://localhost:8002";
	request2.lserv[0].seat[0].flightID="AQ0123";
	request2.lserv[0].seat[0].number=63;
	request2.lserv[0].seat[1].flightID="AQ0123";
	request2.lserv[0].seat[1].number=64;
	request2.lserv[1].server="socket://localhost:8003";
	request2.lserv[1].seat[0].flightID="AC4556";
	request2.lserv[1].seat[0].number=42;
	request2.lserv[2].server="socket://localhost:8004";
	request2.lserv[2].seat[0].flightID="AD4556";
	request2.lserv[2].seat[0].number=42;
	request2.clientLocation = ClientService.location;
	
	book@ClientService(request2);
	println@Console("Done(2).")()
}